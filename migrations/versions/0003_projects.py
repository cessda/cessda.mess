"""Add projects JSONB column to digital_object.

Revision ID: 0003
Revises:     0002
Create Date: 2024-01-03 00:00:00.000000

Motivation:
  OpenAIRE Graph API returns funding project metadata (project ID, code, title,
  funder name, funder short name, jurisdiction) for research products.  Previously
  this was stored in `topics` with scheme="project", which conflated funding
  provenance with subject classification and lost funder details.

  This migration adds a dedicated `projects` JSONB column so funding information
  is stored separately and completely.

  Each entry has the structure:
      {"id": "...", "code": "...", "title": "...", "funder": "...",
       "funder_short": "...", "funder_jurisdiction": "...", "source": "openaire"}
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "digital_object",
        sa.Column("projects", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("digital_object", "projects")
