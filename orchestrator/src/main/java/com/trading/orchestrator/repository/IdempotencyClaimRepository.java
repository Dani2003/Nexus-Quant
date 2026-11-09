package com.trading.orchestrator.repository;

import com.trading.orchestrator.entity.IdempotencyClaimEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyClaimRepository extends JpaRepository<IdempotencyClaimEntity, String> {
    Optional<IdempotencyClaimEntity> findByFingerprint(String fingerprint);
}
