"""SQLAlchemy ORM models for the auth database.

Models are kept in a separate package (`dagster_webserver.database`) so that
database infrastructure (models, engine, migrations) evolves independently of
auth logic (providers, middleware, routes).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
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

    users: Mapped[list["User"]] = relationship("User", back_populates="role")


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
    role_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("roles.id"), nullable=True
    )
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
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

    role: Mapped["Role | None"] = relationship("Role", back_populates="users")
    oidc_provider: Mapped["OIDCProvider | None"] = relationship(
        "OIDCProvider", back_populates="users"
    )
