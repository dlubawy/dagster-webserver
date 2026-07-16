"""User storage backends and AuthUser dataclass.

Provides:
- ``AuthUser`` — immutable dataclass representing an authenticated user
- ``UserBackend`` — abstract base class for credential/user storage
- ``InMemoryUserBackend`` — in-memory store for simple deployments
- ``FileUserBackend`` — YAML/JSON file-based store

Password hashing uses **argon2-cffi** (argon2id, the OWASP 2025
recommended algorithm).  Pre-hashed passwords in the users file
should use the argon2 PHC string format (starts with ``$argon2id$``).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

logger = logging.getLogger("dagster-webserver.auth")

# Module-level hasher instance — safe to reuse across requests.
# Uses argon2id (hybrid of Argon2d + Argon2i), the OWASP 2025
# recommended memory-hard password hashing algorithm.
_PH = PasswordHasher()


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


def _hash_password(password: str) -> str:
    """Hash a password using argon2id.

    Returns an argon2 PHC string (e.g. ``$argon2id$v=19$m=65536...``).
    """
    return _PH.hash(password)


def _verify_password(password: str, hashed: str) -> bool:
    """Verify a password against an argon2 hash.

    Returns ``True`` if the password matches, ``False`` otherwise.
    """
    try:
        _PH.verify(hashed, password)
        return True
    except VerifyMismatchError:
        return False


def _is_argon2_hash(value: str) -> bool:
    """Check whether *value* looks like an argon2 PHC hash string."""
    return value.startswith("$argon2")


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

    Plain-text passwords are automatically hashed with argon2id at
    initialisation.  Pre-hashed passwords (``password_hash`` key with
    an argon2 PHC string) are accepted as-is.
    """

    _raw_users: dict[str, dict[str, Any]]
    _users: dict[str, dict[str, Any]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._users = {}
        for username, data in self._raw_users.items():
            password = data.get("password")
            if password and isinstance(password, str) and not _is_argon2_hash(password):
                data["password_hash"] = _hash_password(password)
            self._users[username] = data
        logger.info("Loaded %d users into InMemoryUserBackend", len(self._users))

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if not user_data:
            return None

        stored_hash = user_data.get("password_hash")
        if stored_hash and _verify_password(password, stored_hash):
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

    Plain-text passwords are automatically hashed with argon2id when
    the file is loaded.  Pre-hashed passwords are also supported::

        users:
          admin:
            password_hash: $argon2id$v=19$m=65536...
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
            if password and isinstance(password, str) and not _is_argon2_hash(password):
                data["password_hash"] = _hash_password(password)
            users[username] = data

        logger.info("Loaded %d users from %s", len(users), self._file_path)
        return users

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if not user_data:
            return None

        stored_hash = user_data.get("password_hash")
        if stored_hash and _verify_password(password, stored_hash):
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
