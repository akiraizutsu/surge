"""CLI utility for Surge v2 user management.

Usage:
    python admin.py list
    python admin.py add-user --username taro --display-name "Taro" --role user [--emoji 👨]
    python admin.py set-password --username taro
    python admin.py delete-user --username taro
    python admin.py seed  # seed users from SURGE_USERS env var (idempotent)
"""

import argparse
import getpass
import json
import os
import sys

# Load .env if present (dotenv optional, fallback to raw os.environ)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import database
import auth_service


def cmd_list(args):
    users = auth_service.list_all_users()
    if not users:
        print("(no users)")
        return
    print(f"{'ID':<4} {'USERNAME':<16} {'NAME':<16} {'ROLE':<8} {'AVATAR':<4} LAST LOGIN")
    print("-" * 72)
    for u in users:
        print(f"{u['id']:<4} {u['username']:<16} {u['display_name']:<16} {u['role']:<8} {u.get('avatar_emoji','👤'):<4} {u.get('last_login_at') or '-'}")


def cmd_add_user(args):
    if not args.password:
        args.password = getpass.getpass("Password: ")
        confirm = getpass.getpass("Confirm: ")
        if args.password != confirm:
            print("Error: passwords do not match", file=sys.stderr)
            sys.exit(1)
    user_id = auth_service.create_user(
        username=args.username,
        password=args.password,
        display_name=args.display_name or args.username,
        role=args.role,
        avatar_emoji=args.emoji or "👤",
    )
    if user_id:
        print(f"Created user {args.username} (id={user_id}, role={args.role})")
    else:
        print(f"Error: could not create user (username '{args.username}' may already exist)", file=sys.stderr)
        sys.exit(1)


def cmd_set_password(args):
    user = database.get_user_by_username(args.username)
    if not user:
        print(f"Error: user '{args.username}' not found", file=sys.stderr)
        sys.exit(1)
    new_pw = args.password
    if not new_pw:
        new_pw = getpass.getpass("New password: ")
        confirm = getpass.getpass("Confirm: ")
        if new_pw != confirm:
            print("Error: passwords do not match", file=sys.stderr)
            sys.exit(1)
    if auth_service.reset_password(user["id"], new_pw):
        print(f"Password updated for {args.username}")
    else:
        print("Error: password reset failed", file=sys.stderr)
        sys.exit(1)


def cmd_delete_user(args):
    user = database.get_user_by_username(args.username)
    if not user:
        print(f"Error: user '{args.username}' not found", file=sys.stderr)
        sys.exit(1)
    if not args.yes:
        confirm = input(f"Really delete user '{args.username}' (id={user['id']})? This deletes their notes too. [y/N]: ")
        if confirm.lower() != "y":
            print("Cancelled")
            return
    database.delete_user(user["id"])
    print(f"Deleted user {args.username}")


def cmd_seed(args):
    """Seed users from SURGE_USERS env var. Safe to run multiple times."""
    raw = os.environ.get("SURGE_USERS", "").strip()
    if not raw:
        print("SURGE_USERS env var not set. Skipping.")
        return

    try:
        users = json.loads(raw)
    except Exception as e:
        print(f"Error: SURGE_USERS is not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # Sort so owner comes first (id=1)
    users_sorted = sorted(users, key=lambda u: 0 if u.get("role") == "owner" else 1)

    created = 0
    skipped = 0
    for u in users_sorted:
        username = (u.get("username") or "").strip()
        password = u.get("password") or ""
        if not username or not password:
            print(f"Warning: skipping invalid entry: {u}", file=sys.stderr)
            continue
        existing = database.get_user_by_username(username)
        if existing:
            skipped += 1
            continue
        user_id = auth_service.create_user(
            username=username,
            password=password,
            display_name=u.get("display_name") or username,
            role=u.get("role") or "user",
            avatar_emoji=u.get("avatar_emoji") or "👤",
        )
        if user_id:
            created += 1
            print(f"  + {username} (id={user_id})")

    print(f"Done. Created: {created}, Skipped (existing): {skipped}")


def main():
    parser = argparse.ArgumentParser(description="Surge v2 user management")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List all users")

    p_add = sub.add_parser("add-user", help="Create a new user")
    p_add.add_argument("--username", required=True)
    p_add.add_argument("--display-name")
    p_add.add_argument("--password")
    p_add.add_argument("--role", choices=["user", "owner"], default="user")
    p_add.add_argument("--emoji", default="👤")

    p_pw = sub.add_parser("set-password", help="Reset a user's password")
    p_pw.add_argument("--username", required=True)
    p_pw.add_argument("--password")

    p_del = sub.add_parser("delete-user", help="Delete a user (and their notes)")
    p_del.add_argument("--username", required=True)
    p_del.add_argument("--yes", action="store_true")

    sub.add_parser("seed", help="Seed users from SURGE_USERS env var")

    args = parser.parse_args()

    # Ensure DB initialised
    database.init_db()

    if args.command == "list":
        cmd_list(args)
    elif args.command == "add-user":
        cmd_add_user(args)
    elif args.command == "set-password":
        cmd_set_password(args)
    elif args.command == "delete-user":
        cmd_delete_user(args)
    elif args.command == "seed":
        cmd_seed(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
