"""
Interactive review queue CLI.

Usage:
    python scripts/review_cli.py                      # list pending items
    python scripts/review_cli.py --role reviewer      # filter by role
    python scripts/review_cli.py --approve <item_id> --reviewer alice
    python scripts/review_cli.py --reject <item_id> --reviewer alice --reason "Not relevant"
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from ingestion.db.engine import SessionLocal
from ingestion.db import crud
from ingestion.review.queue import ReviewQueue


def list_items(db, role: str | None = None):
    items = ReviewQueue.get_pending(db, role=role)
    if not items:
        print("No pending review items.")
        return
    print(f"\n{'ID':<36}  {'Role':<20}  {'Reason'}")
    print("-" * 80)
    for item in items:
        reason = (item.reason or "")[:60]
        print(f"{item.id}  {item.assigned_role:<20}  {reason}")
    print(f"\n{len(items)} item(s) pending.")


def show_item(db, item_id: str):
    item = crud.get_review_item(db, item_id)
    if item is None:
        print(f"Item {item_id} not found.")
        return
    print(f"\nItem:       {item.id}")
    print(f"Status:     {item.status}")
    print(f"Role:       {item.assigned_role}")
    print(f"Reason:     {item.reason}")
    print(f"Due at:     {item.due_at}")
    if item.chunk_id:
        chunk = crud.get_chunk(db, item.chunk_id)
        if chunk:
            print(f"\nChunk text preview:\n{chunk.text[:500]}")


def main():
    parser = argparse.ArgumentParser(description="Review queue CLI")
    parser.add_argument("--role", default=None, help="Filter by role")
    parser.add_argument("--show", default=None, metavar="ITEM_ID", help="Show item details")
    parser.add_argument("--approve", default=None, metavar="ITEM_ID")
    parser.add_argument("--reject", default=None, metavar="ITEM_ID")
    parser.add_argument("--reviewer", default="cli-user", help="Reviewer identity")
    parser.add_argument("--reason", default="", help="Rejection reason")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.show:
            show_item(db, args.show)
        elif args.approve:
            item = ReviewQueue.approve(db, args.approve, args.reviewer)
            print(f"Approved item {item.id} by {args.reviewer}")
        elif args.reject:
            if not args.reason:
                print("--reason is required for rejection")
                sys.exit(1)
            item = ReviewQueue.reject(db, args.reject, args.reviewer, args.reason)
            print(f"Rejected item {item.id} by {args.reviewer}: {args.reason}")
        else:
            list_items(db, role=args.role)
    finally:
        db.close()


if __name__ == "__main__":
    main()
