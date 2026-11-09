package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.RiskDecisionEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface RiskDecisionRepository extends JpaRepository<RiskDecisionEntity, String> {
    List<RiskDecisionEntity> findAllByOrderByTimestampDesc(Pageable pageable);
}
