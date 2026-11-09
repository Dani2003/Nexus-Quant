package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.ExecutionRecordEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ExecutionRecordRepository extends JpaRepository<ExecutionRecordEntity, String> {
    List<ExecutionRecordEntity> findAllByOrderByTimestampDesc(Pageable pageable);
}

