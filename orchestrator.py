"""Discover sources under sources/*/ and run each as an isolated subprocess.

Each source is run by invoking this same script with `--run-source <name>` so
crashes/imports in one source can't affect others.
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from common.logging_setup import setup_logging
from common.secrets import load_env
from common.azure import ensure_azcopy

REPO_ROOT = Path(__file__).resolve().parent
SOURCES_DIR = REPO_ROOT / "sources"


def log_dir() -> Path:
    """Resolved lazily so VW_LOG_DIR from .env (loaded in main) is honored."""
    env = os.getenv("VW_LOG_DIR")
    if env:
        return Path(env)
    if os.name == "nt":
        return Path(r"C:\logs\vw-on-prem-collector")
    return REPO_ROOT / "logs"


def discover_sources() -> list[Path]:
    out = []
    for child in sorted(SOURCES_DIR.iterdir()):
        if not child.is_dir() or child.name.startswith("_") or child.name.startswith("."):
            continue
        if (child / "config.yaml").exists() and (child / "push.py").exists():
            out.append(child)
    return out


def shared_output_dirs(sources: list[Path]) -> list[tuple[str, list[str]]]:
    """Find selected sources that write to the same output_dir.

    Two sources sharing one directory cannot run concurrently: they collide on the
    `_parts_<object>` scratch dirs, and each one's end-of-run sweep deletes files the
    other has not uploaded yet. Sequentially they are fine — nothing interleaves.
    """
    from common.config import SourceConfig

    by_dir: dict[str, list[str]] = {}
    for s in sources:
        try:
            cfg = SourceConfig.load(s / "config.yaml")
        except Exception:  # noqa: BLE001
            continue  # a config that cannot load fails in its own subprocess, as before
        by_dir.setdefault(str(cfg.output_dir).rstrip("/\\").lower(), []).append(s.name)
    return [(d, names) for d, names in sorted(by_dir.items()) if len(names) > 1]


def run_one_subprocess(source_name: str, mode: str) -> int:
    log = logging.getLogger("orchestrator")
    log.info(f"[{source_name}] launching subprocess (mode={mode})")
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "orchestrator.py"),
         "--run-source", source_name, "--mode", mode],
        cwd=str(REPO_ROOT),
    )
    log.info(f"[{source_name}] subprocess exit={result.returncode}")
    return result.returncode


def run_in_process(source_name: str, mode: str) -> int:
    """In-process runner (used by `--run-source` worker invocations)."""
    from common.source import load_source

    src_dir = SOURCES_DIR / source_name
    setup_logging(source_name, log_dir() / source_name)
    log = logging.getLogger("orchestrator")
    try:
        source = load_source(src_dir, mode)
        return source.run()
    except Exception as e:  # noqa: BLE001
        log.exception(f"[{source_name}] FAILED: {e}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="vw-on-prem-collector orchestrator")
    parser.add_argument("--mode", choices=["full", "delta"], default="delta")
    parser.add_argument("--sources", help="Comma-separated source names (default: all discovered)")
    parser.add_argument("--parallel", action="store_true", help="Run sources concurrently")
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--run-source", help=argparse.SUPPRESS)  # internal worker mode
    args = parser.parse_args()

    load_env(REPO_ROOT)

    if args.run_source:
        return run_in_process(args.run_source, args.mode)

    setup_logging("orchestrator", log_dir())
    log = logging.getLogger("orchestrator")

    try:
        ensure_azcopy()
    except FileNotFoundError as e:
        log.error(str(e))
        log.error("Install AzCopy v10 (Windows: https://aka.ms/downloadazcopy-v10-windows, "
                  "Linux: https://aka.ms/downloadazcopy-v10-linux) and set AZCOPY_PATH in .env "
                  "if it is not on PATH")
        return 2

    discovered = discover_sources()
    if args.sources:
        wanted = {s.strip() for s in args.sources.split(",") if s.strip()}
        sources = [s for s in discovered if s.name in wanted]
        missing = wanted - {s.name for s in sources}
        if missing:
            log.error(f"Unknown sources: {missing}")
            return 2
    else:
        sources = discovered

    if not sources:
        log.error("No sources to run")
        return 2

    if args.parallel and len(sources) > 1:
        clashes = shared_output_dirs(sources)
        if clashes:
            log.error("Refusing to run in parallel: these sources share an output_dir "
                      "and would delete each other's in-flight files:")
            for d, names in clashes:
                log.error(f"    {d} <- {', '.join(names)}")
            log.error("Give each source its own output_dir, or drop --parallel.")
            return 2

    log.info(f"Running {len(sources)} source(s): {[s.name for s in sources]} (parallel={args.parallel})")

    results: dict[str, int] = {}
    if args.parallel and len(sources) > 1:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = {ex.submit(run_one_subprocess, s.name, args.mode): s.name for s in sources}
            for fut in as_completed(futures):
                name = futures[fut]
                results[name] = fut.result()
    else:
        for s in sources:
            results[s.name] = run_one_subprocess(s.name, args.mode)

    log.info("=" * 50)
    for name, code in results.items():
        log.info(f"  {name}: {'OK' if code == 0 else f'FAIL ({code})'}")
    log.info("=" * 50)
    return 0 if all(c == 0 for c in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
