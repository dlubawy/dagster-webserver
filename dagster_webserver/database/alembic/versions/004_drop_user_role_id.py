"""Drop users.role_id — superseded by user_role_assignments.

Each user's existing role_id is migrated into a UserRoleAssignment row
with code_location_name=NULL (global scope). The role_id column and its
foreign key are then dropped.

Revision ID: 004
Revises: 003
Create Date: 2026-10-23
"""

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Copy existing users.role_id into user_role_assignments (global scope)
    # Only insert if no global assignment already exists for that user.
    rows = conn.execute(
        sa.text(
            """
            SELECT u.id AS user_id, u.role_id
            FROM users u
            WHERE u.role_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM user_role_assignments ura
                  WHERE ura.user_id = u.id
                    AND ura.code_location_name IS NULL
              )
            """
        )
    ).fetchall()

    for row in rows:
        conn.execute(
            sa.text(
                """
                INSERT INTO user_role_assignments (user_id, role_id, code_location_name)
                VALUES (:user_id, :role_id, NULL)
                """
            ),
            {"user_id": row.user_id, "role_id": row.role_id},
        )

    # Drop the index on role_id first (it references the column we're about
    # to drop, and SQLite's batch rebuild would try to recreate it).
    op.drop_index("ix_users_role_id", table_name="users")

    # Drop the role_id column using batch_alter_table which handles SQLite's
    # limitation (can't DROP COLUMN referenced by FK). On PostgreSQL this
    # translates to a simple ALTER TABLE ... DROP COLUMN.
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_column("role_id")


def downgrade() -> None:
    # Re-add the column using batch_alter_table (same reason as upgrade)
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(sa.Column("role_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "users_role_id_fkey",
            "roles",
            ["role_id"],
            ["id"],
        )
        batch_op.create_index("ix_users_role_id", ["role_id"])

    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            UPDATE users
            SET role_id = (
                SELECT ura.role_id
                FROM user_role_assignments ura
                WHERE ura.user_id = users.id
                  AND ura.code_location_name IS NULL
                LIMIT 1
            )
            """
        )
    )
