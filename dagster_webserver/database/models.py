"""SQLAlchemy ORM models for the auth database.

Models are kept in a separate package (`dagster_webserver.database`) so that
database infrastructure (models, engine, migrations) evolves independently of
auth logic (providers, middleware, routes).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base for all auth database tables.

    Kept separate from Dagster's own metadata to avoid collisions with
    Dagster's run/event/schedule storage tables.
    """


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    permissions: Mapped[dict[str, bool]] = mapped_column(JSON, nullable=False)
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class OIDCProvider(Base):
    __tablename__ = "oidc_providers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    issuer_url: Mapped[str] = mapped_column(String(512), nullable=False)
    client_id: Mapped[str] = mapped_column(String(256), nullable=False)
    client_secret: Mapped[str] = mapped_column(String(1024), nullable=False)
    scopes: Mapped[str] = mapped_column(
        String(512),
        default="openid email profile",
        server_default="openid email profile",
    )
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    display_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    users: Mapped[list["User"]] = relationship("User", back_populates="oidc_provider")


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(1024), nullable=False)
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_service_account: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # OIDC linkage
    oidc_provider_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("oidc_providers.id"), nullable=True
    )
    oidc_sub: Mapped[str | None] = mapped_column(String(256), nullable=True)

    oidc_provider: Mapped["OIDCProvider | None"] = relationship(
        "OIDCProvider", back_populates="users"
    )
    role_assignments: Mapped[list["UserRoleAssignment"]] = relationship(
        "UserRoleAssignment", back_populates="user", cascade="all, delete-orphan"
    )
    api_keys: Mapped[list["ApiKey"]] = relationship(
        "ApiKey", back_populates="user", cascade="all, delete-orphan"
    )


class UserRoleAssignment(Base):
    __tablename__ = "user_role_assignments"
    __table_args__ = (UniqueConstraint("user_id", "role_id", "code_location_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False
    )
    role_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("roles.id"), nullable=False
    )
    code_location_name: Mapped[str | None] = mapped_column(
        String(256), nullable=True
    )  # NULL = global assignment
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    user: Mapped["User"] = relationship("User", back_populates="role_assignments")
    role: Mapped["Role"] = relationship("Role")


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False
    )
    key_hash: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    user: Mapped["User"] = relationship("User", back_populates="api_keys")


class AgentHeartbeat(Base):
    __tablename__ = "agent_heartbeats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_id: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_labels: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    agent_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    metadata_: Mapped[dict | None] = mapped_column("metadata_", JSON, nullable=True)
    errors: Mapped[list | None] = mapped_column(JSON, nullable=True)
    code_server_heartbeats: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    run_worker_statuses: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class AgentRequest(Base):
    __tablename__ = "agent_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID as string
    request_api: Mapped[str] = mapped_column(String(64), nullable=False)
    request_body: Mapped[str] = mapped_column(Text, nullable=False)
    location_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    labels: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    assigned_to_agent: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    popped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)


# Lifecycle statuses for code locations. Agents only serve locations with
# status 'active' (the workspace-entries API filters on it), so setting a
# location to 'paused' makes agents tear its server down; deletion removes
# the definition entirely.
CODE_LOCATION_STATUSES = ("active", "paused")


class CodeLocationDefinition(Base):
    __tablename__ = "code_location_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    location_name: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    deploy_metadata: Mapped[dict] = mapped_column(JSON, nullable=False)
    labels: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    last_loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    load_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    loaded_manifest: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class CodeLocationSnapshot(Base):
    __tablename__ = "code_location_snapshots"
    __table_args__ = (UniqueConstraint("location_name", "snapshot_type", "sha1_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    location_name: Mapped[str] = mapped_column(String(256), nullable=False)
    snapshot_type: Mapped[str] = mapped_column(String(32), nullable=False)
    sha1_hash: Mapped[str] = mapped_column(String(40), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    data: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    stored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
