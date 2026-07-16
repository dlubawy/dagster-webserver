"""Create roles and users tables, seed built-in roles.

Revision ID: 001
Revises:
Create Date: 2026-07-15
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── roles table ──────────────────────────────────────────────
    op.create_table(
        "roles",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(64), unique=True, nullable=False),
        sa.Column("permissions", sa.JSON, nullable=False),
        sa.Column("is_builtin", sa.Boolean, default=False, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index("ix_roles_name", "roles", ["name"], unique=True)

    # ── Seed built-in roles ──────────────────────────────────────
    conn = op.get_bind()
    from dagster_webserver.auth.roles import ROLE_PERMISSIONS, Role

    for role in Role:
        perm_map = {
            perm.name: enabled for perm, enabled in ROLE_PERMISSIONS[role].items()
        }
        conn.execute(
            sa.insert(
                sa.table(
                    "roles",
                    sa.column("name", sa.String),
                    sa.column("permissions", sa.JSON),
                    sa.column("is_builtin", sa.Boolean),
                )
            ).values(
                name=role.value,
                permissions=perm_map,
                is_builtin=True,
            )
        )

    # ── users table ──────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(128), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(1024), nullable=False),
        sa.Column("role_id", sa.Integer, sa.ForeignKey("roles.id"), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("display_name", sa.String(256), nullable=True),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_index("ix_users_role_id", "users", ["role_id"])


def downgrade() -> None:
    op.drop_index("ix_users_role_id")
    op.drop_index("ix_users_username")
    op.drop_table("users")
    op.drop_index("ix_roles_name")
    op.drop_table("roles")
