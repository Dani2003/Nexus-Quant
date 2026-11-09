package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.IdempotencyClaimEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyClaimRepository extends JpaRepository<IdempotencyClaimEntity, String> {
    Optional<IdempotencyClaimEntity> findByFingerprint(String fingerprint);

    @Modifying
    @Query("UPDATE IdempotencyClaimEntity c SET c.status = 'IN_FLIGHT', c.updatedAt = :now " +
           "WHERE c.fingerprint = :fingerprint AND (c.status = 'FAILED' OR (c.status = 'IN_FLIGHT' AND c.updatedAt < :expirationThreshold))")
    int reclaimStaleOrFailed(
            @Param("fingerprint") String fingerprint,
            @Param("now") long now,
            @Param("expirationThreshold") long expirationThreshold
    );
}
