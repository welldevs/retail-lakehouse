#!/usr/bin/env python3
"""Generates the Airflow keys in .env, which is outside version control.

Compose refuses to start without AIRFLOW_SECRET_KEY and AIRFLOW_FERNET_KEY (`:?`
interpolation), so there is no path where an example value becomes the real key by
oversight. An already-set value is not overwritten: running twice neither invalidates open
sessions nor makes a Connection already encrypted with the previous key unreadable.

No third-party dependency: a Fernet key is exactly 32 random bytes in urlsafe
base64, which is what `cryptography.fernet.Fernet.generate_key()` produces.
"""

import base64
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(REPO, ".env")
KEYS = ("AIRFLOW_SECRET_KEY", "AIRFLOW_FERNET_KEY")


def main() -> int:
    if not os.path.exists(ENV_PATH):
        print(f"ERROR: {ENV_PATH} does not exist. Copy it from .env.example first:")
        print("    cp .env.example .env")
        return 2

    with open(ENV_PATH, encoding="utf-8") as handle:
        text = handle.read()
    changed = False

    for key in KEYS:
        found = re.search(rf"^{key}=(.*)$", text, re.M)
        if found and found.group(1).strip():
            print(f"  {key}: already set, kept")
            continue
        value = base64.urlsafe_b64encode(os.urandom(32)).decode()
        if found:
            text = re.sub(rf"^{key}=.*$", f"{key}={value}", text, flags=re.M)
        else:
            text = text.rstrip("\n") + f"\n{key}={value}\n"
        changed = True
        print(f"  {key}: generated")

    if changed:
        with open(ENV_PATH, "w", encoding="utf-8") as handle:
            handle.write(text)
        # The file now holds a real key; 600 is the right mode.
        os.chmod(ENV_PATH, 0o600)
        print(f"  .env written with mode 600")
    return 0


if __name__ == "__main__":
    sys.exit(main())
