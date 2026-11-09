use std::cmp::Ordering;
use std::f64::consts::PI;
use std::sync::Arc;
use std::time::Instant;
use rand::Rng;
use tonic::{transport::Server, Request, Response, Status};
use risk_engine::risk_service_server::{RiskService, RiskServiceServer};
use risk_engine::{RiskRequest, RiskResponse};

pub mod risk_engine {
    tonic::include_proto!("risk");
}

/// Configuration parameters for Monte Carlo Value-at-Risk (VaR) simulation
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    pub num_paths: usize,
    pub num_workers: usize,
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
            num_workers: 8,
            confidence_level: 0.95,
            annualized_volatility: 0.25, // 25% annualized volatility
            trading_days_per_year: 252.0,
            risk_free_rate: 0.04,        // 4% risk-free rate
            default_max_loss_limit: 50_000.0, // Risk limit in base currency
        }
    }
}

/// High-performance Monte Carlo VaR Engine leveraging Tokio multi-threading
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

    /// Executes 10,000 simulated paths concurrently across worker tasks using tokio::spawn
    pub async fn simulate_var(&self, current_price: f64, trade_size: f64) -> (f64, bool, u128) {
        let start_time = Instant::now();
        let total_paths = self.config.num_paths;
        let num_workers = self.config.num_workers.max(1);
        let paths_per_worker = total_paths / num_workers;
        let remainder = total_paths % num_workers;

        let dt = 1.0 / self.config.trading_days_per_year;
        let dt_sqrt = dt.sqrt();
        let r = self.config.risk_free_rate;
        let sigma = self.config.annualized_volatility;
        let drift = (r - 0.5 * sigma * sigma) * dt;
        let diffusion = sigma * dt_sqrt;

        let mut join_handles = Vec::with_capacity(num_workers);

        for i in 0..num_workers {
            let count = if i == num_workers - 1 {
                paths_per_worker + remainder
            } else {
                paths_per_worker
            };

            let handle = tokio::spawn(async move {
                let mut rng = rand::thread_rng();
                let mut chunk_losses = Vec::with_capacity(count);

                // Efficient Box-Muller transform to generate standard normal variates in pairs
                let pairs = count / 2;
                for _ in 0..pairs {
                    let u1: f64 = rng.gen_range(1e-7..1.0);
                    let u2: f64 = rng.gen_range(0.0..1.0);
                    let radius = (-2.0 * u1.ln()).sqrt();
                    let theta = 2.0 * PI * u2;

                    let z0 = radius * theta.cos();
                    let z1 = radius * theta.sin();

                    // Simulating Geometric Brownian Motion (GBM) step
                    let s_t0 = current_price * (drift + diffusion * z0).exp();
                    let s_t1 = current_price * (drift + diffusion * z1).exp();

                    // Loss metric: positive loss represents portfolio drawdown
                    chunk_losses.push(trade_size * (current_price - s_t0));
                    chunk_losses.push(trade_size * (current_price - s_t1));
                }

                // If odd number of paths, generate single tail path
                if count % 2 != 0 {
                    let u1: f64 = rng.gen_range(1e-7..1.0);
                    let u2: f64 = rng.gen_range(0.0..1.0);
                    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos();
                    let s_t = current_price * (drift + diffusion * z).exp();
                    chunk_losses.push(trade_size * (current_price - s_t));
                }

                chunk_losses
            });

            join_handles.push(handle);
        }

        // Aggregate results from concurrent worker tasks
        let mut all_losses = Vec::with_capacity(total_paths);
        for handle in join_handles {
            if let Ok(mut worker_losses) = handle.await {
                all_losses.append(&mut worker_losses);
            }
        }

        // Calculate Value-at-Risk using quickselect O(N) for maximum performance
        let target_index = ((all_losses.len() as f64) * self.config.confidence_level).floor() as usize;
        let index = target_index.min(all_losses.len().saturating_sub(1));

        all_losses.select_nth_unstable_by(index, |a, b| {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        });

        let var_metric = all_losses[index];
        // Allow trade if 95% VaR does not exceed the allowed loss threshold or 15% of position value
        let max_position_risk = (current_price * trade_size.abs() * 0.15).max(self.config.default_max_loss_limit);
        let is_approved = var_metric < max_position_risk;

        let elapsed_us = start_time.elapsed().as_micros();
        (var_metric, is_approved, elapsed_us)
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
        let (var_metric, is_approved, elapsed_us) = self
            .engine
            .simulate_var(req.current_price, req.proposed_trade_size)
            .await;

        let computation_time_ms = ((elapsed_us as f64) / 1000.0).ceil() as i32;

        println!(
            "[RiskEngine] Portfolio: {}, Symbol: {}, Size: {}, Price: {:.2} -> VaR(95%): {:.2}, Approved: {}, Latency: {}μs ({}ms)",
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
    println!(" [Risk Sidecar] High-Performance Monte Carlo VaR Service");
    println!(" Listening on {}", addr);
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
    async fn test_monte_carlo_var_simulation() {
        let engine = MonteCarloRiskEngine::new(SimulationConfig::default());
        // Warm up tokio threadpool
        let _ = engine.simulate_var(150.0, 100.0).await;

        let (var_metric, is_approved, elapsed_us) = engine.simulate_var(150.0, 100.0).await;

        println!(
            "Test Simulation: VaR(95%)={:.4}, is_approved={}, latency={}μs",
            var_metric, is_approved, elapsed_us
        );

        assert!(var_metric > 0.0, "VaR metric should be positive for price drop risk");
        #[cfg(not(debug_assertions))]
        assert!(elapsed_us < 5_000, "Simulation in release mode should complete in under 5ms (5000μs), took {}μs", elapsed_us);
    }
}

