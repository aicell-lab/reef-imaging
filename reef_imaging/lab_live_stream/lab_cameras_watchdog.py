"""
Watchdog for the lab-cameras Linux service.

Periodically polls the /health endpoint of each lab camera Hypha service.
If either endpoint is unreachable or returns a non-ok status, the Linux
systemd service is restarted via systemctl.

Designed to run as a systemd service on the Linux workstation:
  systemctl enable lab-cameras-watchdog
  systemctl start lab-cameras-watchdog
"""

import logging
import os
import subprocess
import sys
import time

import httpx

BASE_URL = "https://hypha.aicell.io/reef-imaging/apps"
CAMERA_SERVICE_IDS = ["reef-lab-camera-1", "reef-lab-camera-2"]
LINUX_SERVICE_NAME = os.getenv("LAB_CAMERAS_SERVICE_NAME", "lab-cameras")
CHECK_INTERVAL = 60   # seconds between health checks
TIMEOUT = 15          # seconds for HTTP request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def check_health(service_id: str) -> bool:
    """Return True if the camera health endpoint reports ok, False otherwise."""
    url = f"{BASE_URL}/{service_id}/health"
    try:
        response = httpx.get(url, timeout=TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "ok":
                return True
            logging.warning("Camera %s health returned non-ok: %s", service_id, data)
        else:
            logging.warning("Camera %s health HTTP %s", service_id, response.status_code)
    except Exception as e:
        logging.warning("Camera %s health check failed: %s", service_id, e)
    return False


def restart_service():
    """Restart the Linux systemd service for lab cameras."""
    logging.info("Restarting systemd service: %s", LINUX_SERVICE_NAME)
    result = subprocess.run(
        ["systemctl", "restart", LINUX_SERVICE_NAME],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logging.error(
            "systemctl restart %s failed (code %s): %s",
            LINUX_SERVICE_NAME, result.returncode, result.stderr.strip(),
        )
    else:
        logging.info("systemctl restart %s succeeded", LINUX_SERVICE_NAME)
    # Wait for Hypha to deregister old services before the new ones come up
    time.sleep(15)


def main():
    logging.info(
        "Watchdog started. Monitoring %s every %ss",
        ", ".join(CAMERA_SERVICE_IDS), CHECK_INTERVAL,
    )
    while True:
        failed = [sid for sid in CAMERA_SERVICE_IDS if not check_health(sid)]
        if failed:
            logging.error("Health check FAILED for: %s — restarting service", failed)
            restart_service()
        else:
            logging.info("All cameras healthy")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
