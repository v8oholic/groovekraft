#!/usr/bin/env python3

import logging
import signal
import sys

from shared.bootstrap import build_arg_parser, build_config


logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    print("You pressed Ctrl+C!")
    sys.exit(0)


def main() -> int:
    signal.signal(signal.SIGINT, signal_handler)
    parser = build_arg_parser()
    args = parser.parse_args()
    config = build_config(args, app_root=sys.path[0] or ".")

    if args.server:
        from webapp.server import run_server

        run_server(config)
        return 0

    from shared.gui import run_gui

    run_gui(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
