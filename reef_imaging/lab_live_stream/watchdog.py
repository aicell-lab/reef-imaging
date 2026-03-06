"""
Watchdog for the reef-hamilton-camera Windows service.

Periodically polls the Hypha health endpoint. If the endpoint is unreachable
or returns a non-ok status, the Windows service is restarted.

Designed to run as its own Windows service via NSSM (as Administrator).
"""

import logging
import subprocess
import sys
import time

import httpx

HEALTH_URL = "https://hypha.aicell.io/reef-imaging/apps/reef-hamilton-feed/health"
SERVICE_NAME = "reef-hamilton-camera"
CHECK_INTERVAL = 60  # seconds between health checks
TIMEOUT = 15  # seconds for HTTP request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def check_health() -> bool:
    """Return True if the health endpoint reports ok, False otherwise."""
    try:
        response = httpx.get(HEALTH_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "ok":
                return True
            logging.warning("Health check returned non-ok status: %s", data)
        else:
            logging.warning("Health check HTTP %s from %s", response.status_code, HEALTH_URL)
    except Exception as e:
        logging.warning("Health check failed: %s", e)
    return False


def restart_service():
    """Stop then start the Windows service."""
    logging.info("Restarting service: %s", SERVICE_NAME)
    for action in ("stop", "start"):
        result = subprocess.run(
            ["sc", action, SERVICE_NAME],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logging.error("sc %s %s failed (code %s): %s", action, SERVICE_NAME, result.returncode, result.stderr.strip())
        else:
            logging.info("sc %s %s succeeded", action, SERVICE_NAME)
        if action == "stop":
            time.sleep(5)  # give the service time to stop before starting


def main():
    logging.info("Watchdog started. Monitoring %s every %ss", HEALTH_URL, CHECK_INTERVAL)
    while True:
        if check_health():
            logging.info("Health check OK")
        else:
            logging.error("Health check FAILED — restarting service")
            restart_service()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
