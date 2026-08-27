"""Ponto de entrada: python -m simulated_oltp_source"""

from __future__ import annotations

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
