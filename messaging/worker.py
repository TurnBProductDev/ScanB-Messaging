"""Task reminders and escalations.

    python -m messaging.worker          loop every MSG_WORKER_INTERVAL_SECONDS
    python -m messaging.worker --once   one pass (a scheduled job, e.g. a Container Apps cron job)

Several copies may run at once; see services/reminders.py for why that is safe.
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from .config import get_settings
from .main import build_services

logger = logging.getLogger("messaging.worker")


async def run(once: bool) -> None:
    settings = get_settings()
    services = build_services(settings)
    try:
        while True:
            stats = await services.reminders.run_once()
            logger.info("reminder pass: tenants=%s tasks=%s sent=%s", stats.tenants, stats.tasks_checked, stats.sent)
            if once:
                return
            await asyncio.sleep(settings.worker_interval_seconds)
    finally:
        await services.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
    asyncio.run(run(args.once))


if __name__ == "__main__":
    main()
