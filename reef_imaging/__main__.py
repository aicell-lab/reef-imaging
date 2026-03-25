import asyncio

from reef_imaging.orchestrator import main as orchestrator_main


def main() -> None:
    raise SystemExit(asyncio.run(orchestrator_main()))


if __name__ == "__main__":
    main()
