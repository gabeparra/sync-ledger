"""Multi-signal entity resolution connecting systems without a shared primary key."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from sync_ledger.db.models import CanonicalEntity, EntityLink
from sync_ledger.reconciliation.normalizers import NormalizedSignals

MatchStrategy = Literal[
    "exact_link",
    "natural_key",
    "composite_score",
    "new_entity",
    "ambiguous_match",
]


@dataclass(frozen=True)
class CandidateScore:
    """Scored candidate CanonicalEntity evaluated during entity resolution."""

    entity: CanonicalEntity
    score: float
    signals: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MatchDecision:
    """Final outcome of resolving an inbound record against canonical entities."""

    strategy: MatchStrategy
    canonical_entity: CanonicalEntity | None
    confidence: float
    matched_signals: list[str]
    duplicate_external_ids: list[str] = field(default_factory=list)
    ambiguous_candidate_ids: list[str] = field(default_factory=list)


def score_candidate(
    entity_type: str,
    incoming: NormalizedSignals,
    candidate: CanonicalEntity,
) -> CandidateScore:
    """Compute a deterministic weighted confidence score between inbound signals and a candidate."""
    score = 0.0
    matched: list[str] = []

    if entity_type == "customer":
        # Disqualify conflicting legal tax IDs even if email/name overlap
        if (
            incoming.tax_id
            and candidate.normalized_tax_id
            and incoming.tax_id != candidate.normalized_tax_id
        ):
            return CandidateScore(entity=candidate, score=0.0, signals=["tax_id_conflict"])

        if incoming.tax_id and candidate.normalized_tax_id == incoming.tax_id:
            score += 0.80
            matched.append("tax_id")

        if incoming.email and candidate.normalized_email == incoming.email:
            score += 0.72
            matched.append("email")

        if incoming.phone and candidate.normalized_phone == incoming.phone:
            score += 0.40
            matched.append("phone")

        name_matched = bool(incoming.name and candidate.normalized_name == incoming.name)
        if name_matched:
            score += 0.35
            matched.append("name")

        if (
            name_matched
            and incoming.postal_code
            and candidate.normalized_postal_code == incoming.postal_code
        ):
            score += 0.35
            matched.append("postal_code")

    else:
        # Invoice / Order resolution
        if (
            incoming.invoice_number
            and candidate.normalized_invoice_number
            and incoming.invoice_number != candidate.normalized_invoice_number
        ):
            return CandidateScore(entity=candidate, score=0.0, signals=["invoice_number_conflict"])

        if (
            incoming.invoice_number
            and candidate.normalized_invoice_number == incoming.invoice_number
        ):
            score += 0.75
            matched.append("invoice_number")

        if incoming.tax_id and candidate.normalized_tax_id == incoming.tax_id:
            score += 0.20
            matched.append("customer_tax_id")
        elif incoming.email and candidate.normalized_email == incoming.email:
            score += 0.15
            matched.append("customer_email")

        cand_currency = candidate.canonical_data.get("currency")
        cand_amount = candidate.canonical_data.get("amount_cents")
        if (
            incoming.invoice_number
            and candidate.normalized_invoice_number == incoming.invoice_number
            and incoming.currency
            and cand_currency == incoming.currency
            and incoming.amount_cents is not None
            and cand_amount == incoming.amount_cents
        ):
            score += 0.10
            matched.append("amount_currency")

    bounded_score = round(min(1.0, max(0.0, score)), 4)
    return CandidateScore(entity=candidate, score=bounded_score, signals=matched)


class CrossSystemMatcher:
    """Resolves external records into CanonicalEntities without a shared primary key."""

    def __init__(
        self,
        *,
        auto_link_threshold: float = 0.70,
        ambiguous_margin: float = 0.10,
    ) -> None:
        self.auto_link_threshold = auto_link_threshold
        self.ambiguous_margin = ambiguous_margin

    def resolve(
        self,
        session: Session,
        *,
        source_system: str,
        entity_type: str,
        external_id: str,
        signals: NormalizedSignals,
    ) -> MatchDecision:
        """Resolve an inbound source record to an existing or new CanonicalEntity."""
        # 1. Exact prior link check
        existing_link = session.scalar(
            select(EntityLink).where(
                EntityLink.source_system == source_system,
                EntityLink.entity_type == entity_type,
                EntityLink.external_id == external_id,
            )
        )
        if existing_link is not None:
            canonical = session.get(CanonicalEntity, existing_link.canonical_entity_id)
            if canonical is not None:
                return MatchDecision(
                    strategy="exact_link",
                    canonical_entity=canonical,
                    confidence=1.0,
                    matched_signals=["external_link"],
                )

        # 2. Candidate blocking using indexed normalized columns
        candidates = self._fetch_candidates(session, entity_type, signals)
        if not candidates:
            return MatchDecision(
                strategy="new_entity",
                canonical_entity=None,
                confidence=1.0,
                matched_signals=[],
            )

        # 3. Deterministic weighted scoring
        scored = [score_candidate(entity_type, signals, candidate) for candidate in candidates]
        qualifying = sorted(
            (c for c in scored if c.score >= self.auto_link_threshold),
            key=lambda c: c.score,
            reverse=True,
        )

        if not qualifying:
            return MatchDecision(
                strategy="new_entity",
                canonical_entity=None,
                confidence=1.0,
                matched_signals=[],
            )

        # 4. Ambiguity detection when multiple canonical entities compete closely
        if len(qualifying) >= 2 and (
            qualifying[0].score - qualifying[1].score <= self.ambiguous_margin
        ):
            return MatchDecision(
                strategy="ambiguous_match",
                canonical_entity=None,
                confidence=qualifying[0].score,
                matched_signals=qualifying[0].signals,
                ambiguous_candidate_ids=[c.entity.id for c in qualifying[:5]],
            )

        winner = qualifying[0]

        # 5. Intra-source duplicate detection: does the same source system already
        #    have a DIFFERENT external_id mapped to this canonical entity?
        same_source_links = list(
            session.scalars(
                select(EntityLink).where(
                    EntityLink.canonical_entity_id == winner.entity.id,
                    EntityLink.source_system == source_system,
                    EntityLink.entity_type == entity_type,
                    EntityLink.external_id != external_id,
                )
            )
        )
        duplicate_ids = [link.external_id for link in same_source_links]

        strategy: MatchStrategy = (
            "natural_key"
            if ("tax_id" in winner.signals or "invoice_number" in winner.signals)
            else "composite_score"
        )
        return MatchDecision(
            strategy=strategy,
            canonical_entity=winner.entity,
            confidence=winner.score,
            matched_signals=winner.signals,
            duplicate_external_ids=duplicate_ids,
        )

    @staticmethod
    def _fetch_candidates(
        session: Session, entity_type: str, signals: NormalizedSignals
    ) -> list[CanonicalEntity]:
        filters = []
        if signals.tax_id:
            filters.append(CanonicalEntity.normalized_tax_id == signals.tax_id)
        if signals.email:
            filters.append(CanonicalEntity.normalized_email == signals.email)
        if signals.phone:
            filters.append(CanonicalEntity.normalized_phone == signals.phone)
        if signals.invoice_number:
            filters.append(CanonicalEntity.normalized_invoice_number == signals.invoice_number)
        if signals.name:
            filters.append(CanonicalEntity.normalized_name == signals.name)

        if not filters:
            return []

        stmt = select(CanonicalEntity).where(
            CanonicalEntity.entity_type == entity_type,
            or_(*filters),
        )
        return list(session.scalars(stmt))
