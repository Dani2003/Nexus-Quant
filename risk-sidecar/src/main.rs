use std::cmp::Ordering;
use std::f64::consts::PI;
use std::sync::Arc;
use std::time::Instant;
use rand::Rng;
use rayon::prelude::*;
use tonic::{transport::Server, Request, Response, Status};
use risk_engine::risk_service_server::{RiskService, RiskServiceServer};
use risk_engine::{RiskRequest, RiskResponse};

pub mod risk_engine {
    tonic::include_proto!("risk");
}

/// Simulation configuration parameters
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    pub num_paths: usize,
    pub confidence_level: f64,
    pub annualized_volatility: f64,
    pub trading_days_per_year: f64,
    pub risk_free_rate: f64,
    pub default_max_loss_limit: f64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            num_paths: 10_000,
            confidence_level: 0.95,
            annualized_volatility: 0.25, // 25% annualized volatility
            trading_days_per_year: 252.0,
            risk_free_rate: 0.04,        // 4% risk-free rate
            default_max_loss_limit: 50_000.0,
        }
    }
}

/// High-performance Monte Carlo VaR Engine leveraging Rayon CPU parallelism
/// and Tokio spawn_blocking to prevent async event loop starvation.
#[derive(Clone)]
pub struct MonteCarloRiskEngine {
    config: Arc<SimulationConfig>,
}

impl MonteCarloRiskEngine {
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Pure CPU-bound Monte Carlo simulation running across all CPU cores via Rayon.
    /// Handles both LONG (trade_size > 0) and SHORT (trade_size < 0) positions explicitly.
    pub fn compute_var_sync(&self, current_price: f64, trade_size: f64) -> (f64, bool, u128) {
        let start_time = Instant::now();
        let total_paths = self.config.num_paths;

        let dt = 1.0 / self.config.trading_days_per_year;
        let dt_sqrt = dt.sqrt();
        let r = self.config.risk_free_rate;
        let sigma = self.config.annualized_volatility;
        let drift = (r - 0.5 * sigma * sigma) * dt;
        let diffusion = sigma * dt_sqrt;

        // Number of CPU threads available in Rayon pool
        let num_threads = rayon::current_num_threads().max(1);
        let chunk_size = (total_paths + num_threads - 1) / num_threads;

        // Partition 10,000 paths across CPU cores using Rayon parallel chunks
        let mut all_losses: Vec<f64> = (0..num_threads)
            .into_par_iter()
            .flat_map_iter(|thread_idx| {
                let count = if thread_idx == num_threads - 1 {
                    total_paths - chunk_size * (num_threads - 1)
                } else {
                    chunk_size
                };

                let mut rng = rand::thread_rng();
                let mut chunk_losses = Vec::with_capacity(count);

                // Box-Muller transform generates normal variates in pairs
                let pairs = count / 2;
                for _ in 0..pairs {
                    let u1: f64 = rng.gen_range(1e-7..1.0);
                    let u2: f64 = rng.gen_range(0.0..1.0);
                    let radius = (-2.0 * u1.ln()).sqrt();
                    let theta = 2.0 * PI * u2;

                    let z0 = radius * theta.cos();
                    let z1 = radius * theta.sin();

                    let s_t0 = current_price * (drift + diffusion * z0).exp();
                    let s_t1 = current_price * (drift + diffusion * z1).exp();

                    // Representation of position loss:
                    // For Long (trade_size > 0): price drop (s_t < current) gives positive loss.
                    // For Short (trade_size < 0): price rise (s_t > current) gives positive loss.
                    chunk_losses.push(trade_size * (current_price - s_t0));
                    chunk_losses.push(trade_size * (current_price - s_t1));
                }

                if count % 2 != 0 {
                    let u1: f64 = rng.gen_range(1e-7..1.0);
                    let u2: f64 = rng.gen_range(0.0..1.0);
                    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos();
                    let s_t = current_price * (drift + diffusion * z).exp();
                    chunk_losses.push(trade_size * (current_price - s_t));
                }

                chunk_losses
            })
            .collect();

        // O(N) pdqselect for 95% quantile Value-at-Risk
        let target_index = ((all_losses.len() as f64) * self.config.confidence_level).floor() as usize;
        let index = target_index.min(all_losses.len().saturating_sub(1));

        all_losses.select_nth_unstable_by(index, |a, b| {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        });

        let var_metric = all_losses[index];

        // Dynamic risk limit: allow trade if 95% VaR does not exceed 15% of notional exposure or default limit
        let notional_value = current_price * trade_size.abs();
        let max_position_risk = (notional_value * 0.15).max(self.config.default_max_loss_limit);
        let is_approved = var_metric < max_position_risk;

        let elapsed_us = start_time.elapsed().as_micros();
        (var_metric, is_approved, elapsed_us)
    }

    /// Asynchronously runs the CPU-bound simulation offloaded to a blocking threadpool,
    /// ensuring the Tokio async I/O runtime remains completely unblocked.
    pub async fn simulate_var(&self, current_price: f64, trade_size: f64) -> (f64, bool, u128) {
        let engine = self.clone();
        tokio::task::spawn_blocking(move || engine.compute_var_sync(current_price, trade_size))
            .await
            .unwrap_or((0.0, false, 0))
    }
}

pub struct MyRiskService {
    engine: MonteCarloRiskEngine,
}

impl MyRiskService {
    pub fn new() -> Self {
        Self {
            engine: MonteCarloRiskEngine::new(SimulationConfig::default()),
        }
    }
}

#[tonic::async_trait]
impl RiskService for MyRiskService {
    async fn evaluate_risk(
        &self,
        request: Request<RiskRequest>,
    ) -> Result<Response<RiskResponse>, Status> {
        let req = request.into_inner();

        // Strict input validation
        if req.portfolio_id.trim().is_empty() {
            return Err(Status::invalid_argument("portfolio_id must not be empty"));
        }
        if req.asset_symbol.trim().is_empty() {
            return Err(Status::invalid_argument("asset_symbol must not be empty"));
        }
        if !req.current_price.is_finite() || req.current_price <= 0.0 {
            return Err(Status::invalid_argument(
                "current_price must be a finite, positive number",
            ));
        }
        if !req.proposed_trade_size.is_finite() || req.proposed_trade_size == 0.0 {
            return Err(Status::invalid_argument(
                "proposed_trade_size must be a finite, non-zero number (positive for LONG, negative for SHORT)",
            ));
        }

        let (var_metric, is_approved, elapsed_us) = self
            .engine
            .simulate_var(req.current_price, req.proposed_trade_size)
            .await;

        let computation_time_ms = ((elapsed_us as f64) / 1000.0).ceil() as i32;

        let position_type = if req.proposed_trade_size > 0.0 { "LONG" } else { "SHORT" };

        println!(
            "[RiskEngine] [{}] Portfolio: {}, Symbol: {}, Size: {}, Price: {:.2} -> VaR(95%): {:.2}, Approved: {}, Latency: {}μs ({}ms)",
            position_type,
            req.portfolio_id,
            req.asset_symbol,
            req.proposed_trade_size,
            req.current_price,
            var_metric,
            is_approved,
            elapsed_us,
            computation_time_ms
        );

        let response = RiskResponse {
            is_approved,
            value_at_risk: var_metric,
            computation_time_ms,
        };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("RISK_SIDECAR_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("RISK_SIDECAR_PORT").unwrap_or_else(|_| "50051".to_string());
    let addr = format!("{}:{}", host, port).parse()?;
    let service = MyRiskService::new();

    println!("============================================================");
    println!(" [Risk Sidecar] High-Performance Multi-Core Monte Carlo VaR");
    println!(" Listening on {}", addr);
    println!(" Rayon CPU worker threads: {}", rayon::current_num_threads());
    println!(" Simulation paths: 10,000 | Target Latency: < 5ms");
    println!("============================================================");

    Server::builder()
        .add_service(RiskServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_long_position_var() {
        let engine = MonteCarloRiskEngine::new(SimulationConfig::default());
        let (var_metric, is_approved, elapsed_us) = engine.simulate_var(150.0, 100.0).await;

        println!("LONG test: VaR={:.4}, approved={}, latency={}μs", var_metric, is_approved, elapsed_us);
        assert!(var_metric > 0.0, "Long position VaR should be positive");
    }

    #[tokio::test]
    async fn test_short_position_var() {
        let engine = MonteCarloRiskEngine::new(SimulationConfig::default());
        // Negative trade_size represents a short position
        let (var_metric, is_approved, elapsed_us) = engine.simulate_var(150.0, -100.0).await;

        println!("SHORT test: VaR={:.4}, approved={}, latency={}μs", var_metric, is_approved, elapsed_us);
        assert!(var_metric > 0.0, "Short position VaR should be positive (drawdown from price increase)");
    }

    #[tokio::test]
    async fn test_concurrent_load_performance() {
        let engine = MonteCarloRiskEngine::new(SimulationConfig::default());
        // Warm up
        let _ = engine.simulate_var(150.0, 100.0).await;

        // Dispatch 10 concurrent simulation requests simultaneously
        let mut handles = Vec::new();
        let total_start = Instant::now();

        for i in 0..10 {
            let e = engine.clone();
            let size = if i % 2 == 0 { 100.0 } else { -100.0 };
            handles.push(tokio::spawn(async move {
                e.simulate_var(150.0 + (i as f64), size).await
            }));
        }

        for h in handles {
            let (var, approved, latency) = h.await.unwrap();
            assert!(var > 0.0);
            println!("Concurrent task finished: VaR={:.2}, approved={}, latency={}μs", var, approved, latency);
        }

        let total_time_ms = total_start.elapsed().as_millis();
        println!("10 Concurrent Monte Carlo requests completed in {}ms", total_time_ms);
    }
}
