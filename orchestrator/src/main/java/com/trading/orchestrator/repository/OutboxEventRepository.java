package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Durable repository for outbox events supporting safe multi-instance coordination and status queries.
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {
    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(String status, Pageable pageable);

    @Modifying
    @Query("UPDATE OutboxEventEntity e SET e.status = 'IN_FLIGHT' WHERE e.eventId = :eventId AND e.status = 'PENDING'")
    int claimOutboxEvent(@Param("eventId") String eventId);
}
