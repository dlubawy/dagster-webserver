"""Create agent tables, api_keys, user_role_assignments; add is_service_account to users.

Revision ID: 003
Revises: 002
Create Date: 2026-10-23
"""

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── agent_heartbeats table ───────────────────────────────────
    op.create_table(
        "agent_heartbeats",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("agent_id", sa.String(64), nullable=False),
        sa.Column("agent_labels", sa.JSON, nullable=True),
        sa.Column("agent_type", sa.String(64), nullable=True),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("metadata_", sa.JSON, nullable=True),
        sa.Column("errors", sa.JSON, nullable=True),
        sa.Column("code_server_heartbeats", sa.JSON, nullable=True),
        sa.Column("run_worker_statuses", sa.JSON, nullable=True),
    )
    op.create_index(
        "idx_agent_heartbeats_agent_id_ts",
        "agent_heartbeats",
        ["agent_id", sa.text("timestamp DESC")],
    )

    # ── agent_requests table (work queue) ────────────────────────
    op.create_table(
        "agent_requests",
        sa.Column(
            "id",
            sa.String(36),
            primary_key=True,
        ),  # UUID stored as string for SQLite compat
        sa.Column("request_api", sa.String(64), nullable=False),
        sa.Column("request_body", sa.Text(), nullable=False),
        sa.Column("location_name", sa.String(256), nullable=True),
        sa.Column("labels", sa.JSON, nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("assigned_to_agent", sa.String(64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("popped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("response_body", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_agent_requests_status_labels",
        "agent_requests",
        ["status"],
    )
    op.create_index(
        "idx_agent_requests_expires_at",
        "agent_requests",
        ["expires_at"],
    )

    # ── code_location_definitions table ──────────────────────────
    op.create_table(
        "code_location_definitions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("location_name", sa.String(256), unique=True, nullable=False),
        sa.Column("deploy_metadata", sa.JSON, nullable=False),
        sa.Column("labels", sa.JSON, nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="active"),
        sa.Column("last_loaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("load_error", sa.JSON, nullable=True),
        sa.Column("loaded_manifest", sa.JSON, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_code_location_definitions_status",
        "code_location_definitions",
        ["status"],
    )

    # ── code_location_snapshots table ────────────────────────────
    op.create_table(
        "code_location_snapshots",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("location_name", sa.String(256), nullable=False),
        sa.Column("snapshot_type", sa.String(32), nullable=False),
        sa.Column("sha1_hash", sa.String(40), nullable=False),
        sa.Column("size_bytes", sa.BigInteger, nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=True),
        sa.Column(
            "stored_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("location_name", "snapshot_type", "sha1_hash"),
    )
    op.create_index("idx_snapshots_hash", "code_location_snapshots", ["sha1_hash"])

    # ── api_keys table (user-owned API keys) ─────────────────────
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("key_hash", sa.String(128), unique=True, nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_api_keys_user_id", "api_keys", ["user_id"])

    # ── user_role_assignments table (per-location roles) ─────────
    op.create_table(
        "user_role_assignments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("role_id", sa.Integer, sa.ForeignKey("roles.id"), nullable=False),
        sa.Column("code_location_name", sa.String(256), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("user_id", "role_id", "code_location_name"),
    )
    op.create_index(
        "idx_user_role_assignments_user",
        "user_role_assignments",
        ["user_id"],
    )

    # ── Add is_service_account to users ──────────────────────────
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_service_account",
                sa.Boolean,
                nullable=False,
                server_default=sa.text("0"),
            )
        )

    # ── Migrate existing users.role_id into user_role_assignments ─
    conn = op.get_bind()
    result = conn.execute(
        sa.text("SELECT id, role_id FROM users WHERE role_id IS NOT NULL")
    )
    for row in result:
        conn.execute(
            sa.text(
                "INSERT INTO user_role_assignments (user_id, role_id, code_location_name) "
                "VALUES (:uid, :rid, NULL)"
            ),
            {"uid": row[0], "rid": row[1]},
        )


def downgrade() -> None:
    # Drop user_role_assignments first (FK reference)
    op.drop_index("idx_user_role_assignments_user")
    op.drop_table("user_role_assignments")

    # Drop api_keys
    op.drop_index("idx_api_keys_user_id")
    op.drop_table("api_keys")

    # Drop code_location_snapshots
    op.drop_index("idx_snapshots_hash")
    op.drop_table("code_location_snapshots")

    # Drop code_location_definitions
    op.drop_index("idx_code_location_definitions_status")
    op.drop_table("code_location_definitions")

    # Drop agent_requests
    op.drop_index("idx_agent_requests_expires_at")
    op.drop_index("idx_agent_requests_status_labels")
    op.drop_table("agent_requests")

    # Drop agent_heartbeats
    op.drop_index("idx_agent_heartbeats_agent_id_ts")
    op.drop_table("agent_heartbeats")

    # Remove is_service_account from users
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_column("is_service_account")
