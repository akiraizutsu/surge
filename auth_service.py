"""User authentication and session management for Surge v2."""

from functools import wraps

from flask import jsonify, redirect, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

import database


# ── User operations ──────────────────────────────────────────────────────

def create_user(username, password, display_name, role="user", avatar_emoji="👤"):
    """Create a new user with hashed password. Returns user_id or None on conflict."""
    if not username or not password:
        return None
    pw_hash = generate_password_hash(password)
    return database.create_user(
        username=username.strip(),
        password_hash=pw_hash,
        display_name=display_name or username,
        role=role if role in ("owner", "user") else "user",
        avatar_emoji=avatar_emoji or "👤",
    )


def verify_login(username, password):
    """Verify username+password. Returns user dict (no hash) on success, None on failure."""
    if not username or not password:
        return None
    user = database.get_user_by_username(username.strip())
    if not user:
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    database.update_user_last_login(user["id"])
    # Strip hash before returning
    safe = {k: v for k, v in user.items() if k != "password_hash"}
    return safe


def get_user(user_id):
    """Return user dict by id (without password hash)."""
    user = database.get_user_by_id(user_id)
    if not user:
        return None
    return {k: v for k, v in user.items() if k != "password_hash"}


def get_user_by_username(username):
    user = database.get_user_by_username(username)
    if not user:
        return None
    return {k: v for k, v in user.items() if k != "password_hash"}


def change_password(user_id, current_password, new_password):
    """Change password for a user. Returns True on success, False otherwise."""
    if not current_password or not new_password:
        return False
    if len(new_password) < 4:
        return False
    user = database.get_user_by_id(user_id)
    if not user:
        return False
    if not check_password_hash(user["password_hash"], current_password):
        return False
    database.update_user_password(user_id, generate_password_hash(new_password))
    return True


def reset_password(user_id, new_password):
    """Admin password reset (no current password check). Used by admin.py CLI."""
    if not new_password:
        return False
    database.update_user_password(user_id, generate_password_hash(new_password))
    return True


def set_consent(user_id):
    """Mark user as having given consent to the collective knowledge terms."""
    database.update_user_consent(user_id)


def list_all_users():
    """Return all users (owner admin view, no password hashes)."""
    return database.list_users()


# ── Session helpers ──────────────────────────────────────────────────────

def login_session(user):
    """Set Flask session cookie after successful login."""
    session.clear()
    session["user_id"] = user["id"]
    session["username"] = user["username"]
    session["role"] = user["role"]
    session.permanent = True


def logout_session():
    session.clear()


def current_user():
    """Return current user dict from session, or None if not logged in."""
    user_id = session.get("user_id")
    if user_id is None:
        return None
    return get_user(user_id)


def current_user_id():
    """Return current user id, or None."""
    return session.get("user_id")


def is_owner():
    """True if current session user has role='owner'."""
    return session.get("role") == "owner"


# ── Decorators ────────────────────────────────────────────────────────────

def login_required(f):
    """Decorator: require logged-in user. Returns 401 for API, redirect for pages."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"error": "unauthorized"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def owner_required(f):
    """Decorator: require owner role. Returns 403 otherwise."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"error": "unauthorized"}), 401
            return redirect(url_for("login"))
        if session.get("role") != "owner":
            return jsonify({"error": "forbidden: owner only"}), 403
        return f(*args, **kwargs)
    return wrapper
