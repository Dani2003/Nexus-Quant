package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.TradeOrderEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TradeOrderRepository extends JpaRepository<TradeOrderEntity, String> {
    List<TradeOrderEntity> findAllByOrderByTimestampDesc(Pageable pageable);
}
