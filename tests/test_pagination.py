"""Tests for paginated list endpoints."""


def _seed_review_items(db, count: int):
    """Seed `count` pending review items all linked to one canonical doc."""
    from ingestion.db import crud

    raw = crud.create_raw_doc(
        db,
        filename="pag.txt",
        original_path="/tmp/pag.txt",
        stored_path="/tmp/pag.txt",
        file_hash="pagtest01",
        file_size_bytes=1,
    )
    db.flush()
    canonical = crud.create_canonical(
        db,
        raw_doc_id=raw.id,
        content_type="general",
        authority_level="observed",
        status="review",
    )
    db.flush()
    for _ in range(count):
        crud.create_review_item(
            db,
            canonical_doc_id=canonical.id,
            assigned_role="reviewer",
            reason="test item",
        )
    db.commit()


def test_get_pending_paginated_returns_correct_page(db):
    from ingestion.db.crud import get_pending_review_items_paginated

    _seed_review_items(db, 10)

    page1 = get_pending_review_items_paginated(db, limit=4, offset=0)
    page2 = get_pending_review_items_paginated(db, limit=4, offset=4)
    page3 = get_pending_review_items_paginated(db, limit=4, offset=8)

    assert len(page1) == 4
    assert len(page2) == 4
    assert len(page3) == 2

    # No overlap between pages
    ids1 = {i.id for i in page1}
    ids2 = {i.id for i in page2}
    assert ids1.isdisjoint(ids2)


def test_get_pending_paginated_default_returns_all_up_to_100(db):
    from ingestion.db.crud import get_pending_review_items_paginated

    _seed_review_items(db, 5)
    result = get_pending_review_items_paginated(db)
    assert len(result) == 5


def test_get_queue_endpoint_accepts_limit_offset(db):
    """The review router get_queue function accepts limit and offset parameters."""
    from ingestion.api.routers.review import get_queue
    import inspect

    sig = inspect.signature(get_queue)
    assert "limit" in sig.parameters
    assert "offset" in sig.parameters
