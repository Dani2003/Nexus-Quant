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
 * Durable repository for outbox events supporting safe multi-instance coordination,
 * stale lease reclamation, and status queries.
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(String status, Pageable pageable);

    @Query("SELECT e FROM OutboxEventEntity e WHERE e.status = 'PENDING' OR (e.status = 'IN_FLIGHT' AND e.updatedAt < :staleThreshold) ORDER BY e.createdAt ASC")
    List<OutboxEventEntity> findPendingOrStaleInFlight(@Param("staleThreshold") long staleThreshold, Pageable pageable);

    @Modifying
    @Query("UPDATE OutboxEventEntity e SET e.status = 'IN_FLIGHT', e.updatedAt = :now, e.retryCount = e.retryCount + 1 " +
           "WHERE e.eventId = :eventId AND (e.status = 'PENDING' OR (e.status = 'IN_FLIGHT' AND e.updatedAt < :staleThreshold))")
    int claimOutboxEventWithLease(
            @Param("eventId") String eventId,
            @Param("now") long now,
            @Param("staleThreshold") long staleThreshold
    );
}
