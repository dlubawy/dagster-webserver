"""User storage backends and AuthUser dataclass.

Provides:
- ``AuthUser`` — immutable dataclass representing an authenticated user
- ``UserBackend`` — abstract base class for credential/user storage
- ``InMemoryUserBackend`` — in-memory store for simple deployments
- ``FileUserBackend`` — YAML/JSON file-based store
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("dagster-webserver.auth")


@dataclass(frozen=True)
class AuthUser:
    """Represents an authenticated user with a role assignment.

    Attributes:
        username: Unique identifier for the user.
        role: Role enum value (e.g. "viewer", "editor") or "custom".
        custom_permissions: Explicit permission map for custom roles.
        email: Optional email address.
        display_name: Optional human-readable name.
    """

    username: str
    role: str  # Role enum value or "custom"
    custom_permissions: dict[str, bool] | None = None
    email: str | None = None
    display_name: str | None = None


class UserBackend(ABC):
    """Abstract user storage backend.

    Subclass this to integrate with external identity providers
    (LDAP, OAuth2, etc.).
    """

    @abstractmethod
    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        """Verify credentials and return the user, or ``None`` if invalid."""
        ...

    @abstractmethod
    async def get_user(self, username: str) -> AuthUser | None:
        """Look up user by username (e.g. from session)."""
        ...


def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    """Hash a password with a random salt using SHA-256.

    Returns (hashed_password, salt).  For production deployments,
    ``bcrypt`` should be used instead — this simple hash is sufficient
    for the initial implementation and easy to swap out.
    """
    import secrets

    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256(f"{salt}{password}".encode("utf-8")).hexdigest()
    return hashed, salt


def _verify_password(password: str, hashed: str, salt: str) -> bool:
    """Verify a password against a stored hash."""
    expected, _ = _hash_password(password, salt)
    return hmac.compare_digest(expected, hashed)


@dataclass
class InMemoryUserBackend(UserBackend):
    """Simple in-memory user store.

    Users are defined at startup as a dict of
    ``{username: {password, role, ...}}``.

    Example::

        backend = InMemoryUserBackend({
            "admin": {"password": "changeme", "role": "admin"},
            "viewer": {"password": "view", "role": "viewer"},
        })
    """

    _raw_users: dict[str, dict[str, Any]]
    _users: dict[str, dict[str, Any]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._users = {}
        for username, data in self._raw_users.items():
            password = data["password"]
            if isinstance(password, str) and not password.startswith("$"):
                hashed, salt = _hash_password(password)
                data["password_hash"] = hashed
                data["salt"] = salt
            self._users[username] = data
        logger.info("Loaded %d users into InMemoryUserBackend", len(self._users))

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if not user_data:
            return None

        stored_hash = user_data.get("password_hash")
        salt = user_data.get("salt")
        if stored_hash and salt and _verify_password(password, stored_hash, salt):
            return self._build_user(username, user_data)
        return None

    async def get_user(self, username: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if user_data:
            return self._build_user(username, user_data)
        return None

    def _build_user(self, username: str, data: dict[str, Any]) -> AuthUser:
        return AuthUser(
            username=username,
            role=data.get("role", "viewer"),
            custom_permissions=data.get("custom_permissions"),
            email=data.get("email"),
            display_name=data.get("display_name"),
        )


class FileUserBackend(UserBackend):
    """YAML/JSON file-based user store.

    Expects a file with a top-level ``users`` key::

        users:
          admin:
            password: changeme
            role: admin
          viewer:
            password: view
            role: viewer

    Pre-hashed passwords are also supported::

        users:
          admin:
            password_hash: <sha256 hex>
            salt: <hex salt>
            role: admin
    """

    def __init__(self, file_path: str | Path) -> None:
        self._file_path = Path(file_path)
        self._users: dict[str, dict[str, Any]] = self._load_users()

    def _load_users(self) -> dict[str, dict[str, Any]]:
        if not self._file_path.exists():
            logger.warning(
                "Users file not found: %s — no users available", self._file_path
            )
            return {}

        import json as _json

        try:
            if self._file_path.suffix in (".yaml", ".yml"):
                import yaml as _yaml

                raw = _yaml.safe_load(self._file_path.read_text())
            else:
                raw = _json.loads(self._file_path.read_text())
        except Exception as e:
            logger.error("Failed to parse users file %s: %s", self._file_path, e)
            return {}

        users_raw = raw.get("users", raw) if isinstance(raw, dict) else {}
        users: dict[str, dict[str, Any]] = {}
        for username, data in users_raw.items():
            if not isinstance(data, dict):
                continue
            password = data.get("password")
            if password and isinstance(password, str) and not password.startswith("$"):
                hashed, salt = _hash_password(password)
                data["password_hash"] = hashed
                data["salt"] = salt
            users[username] = data

        logger.info("Loaded %d users from %s", len(users), self._file_path)
        return users

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if not user_data:
            return None

        stored_hash = user_data.get("password_hash")
        salt = user_data.get("salt")
        if stored_hash and salt and _verify_password(password, stored_hash, salt):
            return self._build_user(username, user_data)
        return None

    async def get_user(self, username: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if user_data:
            return self._build_user(username, user_data)
        return None

    def _build_user(self, username: str, data: dict[str, Any]) -> AuthUser:
        return AuthUser(
            username=username,
            role=data.get("role", "viewer"),
            custom_permissions=data.get("custom_permissions"),
            email=data.get("email"),
            display_name=data.get("display_name"),
        )

    def reload(self) -> None:
        """Reload users from file."""
        self._users = self._load_users()
