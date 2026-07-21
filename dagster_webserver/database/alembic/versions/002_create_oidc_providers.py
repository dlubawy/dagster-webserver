"""Create oidc_providers table and add OIDC columns to users.

Revision ID: 002
Revises: 001
Create Date: 2026-07-19
"""

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oidc_providers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(64), unique=True, nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("issuer_url", sa.String(512), nullable=False),
        sa.Column("client_id", sa.String(256), nullable=False),
        sa.Column("client_secret", sa.String(1024), nullable=False),
        sa.Column(
            "scopes",
            sa.String(512),
            server_default="openid email profile",
        ),
        sa.Column("enabled", sa.Boolean, server_default="1"),
        sa.Column("display_order", sa.Integer, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )

    # Use batch mode for SQLite compatibility (ALTER TABLE constraints)
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("oidc_provider_id", sa.Integer, nullable=True),
        )
        batch_op.add_column(
            sa.Column("oidc_sub", sa.String(256), nullable=True),
        )
        batch_op.create_foreign_key(
            "fk_users_oidc_provider",
            "oidc_providers",
            ["oidc_provider_id"],
            ["id"],
        )
        batch_op.create_index("ix_users_oidc_sub", ["oidc_sub"])


def downgrade() -> None:
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_index("ix_users_oidc_sub")
        batch_op.drop_constraint("fk_users_oidc_provider", type_="foreignkey")
        batch_op.drop_column("oidc_sub")
        batch_op.drop_column("oidc_provider_id")
    op.drop_table("oidc_providers")
