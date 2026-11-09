package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.PositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PositionRepository extends JpaRepository<PositionEntity, String> {
    List<PositionEntity> findByPortfolioId(String portfolioId);
    Optional<PositionEntity> findByPortfolioIdAndSymbol(String portfolioId, String symbol);
}

