"""Add publication_date column to digital_object.

Revision ID: 0004
Revises:     0003
Create Date: 2026-03-24 00:00:00.000000

Motivation:
  The CESSDA SKG-IF source endpoint returns a `dateIssued` field for each research
  product, but this date was previously not extracted or stored.  Adding a dedicated
  nullable TEXT column makes the publication year/date available for display in the
  browse UI without requiring JSONB extraction at query time.

  Values are stored as returned by the source endpoint (ISO 8601 string or year-only),
  e.g. "2021" or "2021-03-15".
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "digital_object",
        sa.Column("publication_date", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("digital_object", "publication_date")
