import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import relationship

from ingestion.db.engine import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Layer 1 — Raw documents
# ---------------------------------------------------------------------------

class RawDocument(Base):
    __tablename__ = "raw_documents"

    id = Column(String, primary_key=True, default=_uuid)
    filename = Column(String, nullable=False)
    original_path = Column(String, nullable=False)
    stored_path = Column(String, nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)
    mime_type = Column(String, nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    extracted_text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)

    canonical = relationship("CanonicalDocument", back_populates="raw_doc", uselist=False)
    pipeline_runs = relationship("PipelineRun", back_populates="raw_doc")


# ---------------------------------------------------------------------------
# Layer 2 — Canonical documents
# ---------------------------------------------------------------------------

class CanonicalDocument(Base):
    __tablename__ = "canonical_documents"

    id = Column(String, primary_key=True, default=_uuid)
    raw_doc_id = Column(String, ForeignKey("raw_documents.id"), nullable=False, unique=True)
    title = Column(String, nullable=True)
    content_type = Column(String, nullable=True)      # prd, competitor, insight, decision, metric, integration, general
    authority_level = Column(String, nullable=False, default="observed")   # authoritative, observed, derived, proposed
    version = Column(String, nullable=True)
    source_url = Column(String, nullable=True)
    status = Column(String, nullable=False, default="draft")   # draft, review, published, rejected
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=True)

    raw_doc = relationship("RawDocument", back_populates="canonical")
    entities = relationship("Entity", back_populates="canonical_doc")
    chunks = relationship("Chunk", back_populates="canonical_doc")
    review_items = relationship("ReviewQueueItem", back_populates="canonical_doc")


# ---------------------------------------------------------------------------
# Layer 3 — Entities
# ---------------------------------------------------------------------------

class Entity(Base):
    __tablename__ = "entities"

    id = Column(String, primary_key=True, default=_uuid)
    canonical_doc_id = Column(String, ForeignKey("canonical_documents.id"), nullable=False)
    name = Column(String, nullable=False)
    entity_type = Column(String, nullable=False)   # Product, Feature, Competitor, CompetitorFeature, Decision, Insight, Metric, Integration, SourceDocument
    normalized_name = Column(String, nullable=False, index=True)
    aliases = Column(Text, nullable=True)          # JSON list of strings
    confidence_score = Column(Float, nullable=False, default=1.0)
    authority_level = Column(String, nullable=False, default="observed")
    source_references = Column(Text, nullable=True)  # JSON list of raw_doc_ids
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)

    canonical_doc = relationship("CanonicalDocument", back_populates="entities")
    outgoing_relationships = relationship("Relationship", foreign_keys="Relationship.source_entity_id", back_populates="source_entity")
    incoming_relationships = relationship("Relationship", foreign_keys="Relationship.target_entity_id", back_populates="target_entity")


# ---------------------------------------------------------------------------
# Layer 4 — Relationships
# ---------------------------------------------------------------------------

class Relationship(Base):
    __tablename__ = "relationships"

    id = Column(String, primary_key=True, default=_uuid)
    source_entity_id = Column(String, ForeignKey("entities.id"), nullable=False)
    target_entity_id = Column(String, ForeignKey("entities.id"), nullable=False)
    relationship_type = Column(String, nullable=False)  # belongs_to, has_version, competes_with, depends_on, mentions
    confidence_score = Column(Float, nullable=False, default=1.0)
    source_doc_id = Column(String, ForeignKey("canonical_documents.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)

    source_entity = relationship("Entity", foreign_keys=[source_entity_id], back_populates="outgoing_relationships")
    target_entity = relationship("Entity", foreign_keys=[target_entity_id], back_populates="incoming_relationships")


# ---------------------------------------------------------------------------
# Layer 5 — Chunks
# ---------------------------------------------------------------------------

class Chunk(Base):
    __tablename__ = "chunks"

    id = Column(String, primary_key=True, default=_uuid)
    canonical_doc_id = Column(String, ForeignKey("canonical_documents.id"), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    char_start = Column(Integer, nullable=True)
    char_end = Column(Integer, nullable=True)
    heading_context = Column(Text, nullable=True)
    confidence_score = Column(Float, nullable=False, default=1.0)
    authority_level = Column(String, nullable=False, default="observed")
    passed_review = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)

    canonical_doc = relationship("CanonicalDocument", back_populates="chunks")
    index_entry = relationship("IndexEntry", back_populates="chunk", uselist=False)
    review_items = relationship("ReviewQueueItem", back_populates="chunk")


# ---------------------------------------------------------------------------
# Layer 6 — Index metadata
# ---------------------------------------------------------------------------

class IndexEntry(Base):
    __tablename__ = "index_entries"

    id = Column(String, primary_key=True, default=_uuid)
    chunk_id = Column(String, ForeignKey("chunks.id"), nullable=False, unique=True)
    chroma_id = Column(String, nullable=False)
    embedding_model = Column(String, nullable=False)
    indexed_at = Column(DateTime(timezone=True), default=_now, nullable=False)

    chunk = relationship("Chunk", back_populates="index_entry")


# ---------------------------------------------------------------------------
# Operational — Pipeline runs
# ---------------------------------------------------------------------------

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(String, primary_key=True, default=_uuid)
    raw_doc_id = Column(String, ForeignKey("raw_documents.id"), nullable=False)
    stage = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")  # pending, running, completed, failed
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_msg = Column(Text, nullable=True)

    raw_doc = relationship("RawDocument", back_populates="pipeline_runs")

    __table_args__ = (UniqueConstraint("raw_doc_id", "stage", name="uq_pipeline_run_doc_stage"),)


# ---------------------------------------------------------------------------
# Operational — Review queue
# ---------------------------------------------------------------------------

class ReviewQueueItem(Base):
    __tablename__ = "review_queue"

    id = Column(String, primary_key=True, default=_uuid)
    chunk_id = Column(String, ForeignKey("chunks.id"), nullable=True)
    canonical_doc_id = Column(String, ForeignKey("canonical_documents.id"), nullable=True)
    assigned_role = Column(String, nullable=False, default="reviewer")
    assigned_to = Column(String, nullable=True)
    reason = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="pending")   # pending, approved, rejected, escalated
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)
    due_at = Column(DateTime(timezone=True), nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by = Column(String, nullable=True)
    rejection_reason = Column(Text, nullable=True)

    chunk = relationship("Chunk", back_populates="review_items")
    canonical_doc = relationship("CanonicalDocument", back_populates="review_items")


# ---------------------------------------------------------------------------
# Operational — API keys
# ---------------------------------------------------------------------------

class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(String, primary_key=True, default=_uuid)
    key_hash = Column(String(64), nullable=False, unique=True, index=True)
    label = Column(String, nullable=False)
    rate_limit_per_minute = Column(Integer, nullable=False, default=60)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=_now, nullable=False)


# ---------------------------------------------------------------------------
# Operational — Rate limit persistence
# ---------------------------------------------------------------------------

class RateLimitEntry(Base):
    __tablename__ = "rate_limit_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_hash = Column(String(64), nullable=False, index=True)
    requested_at = Column(DateTime(timezone=True), nullable=False)
