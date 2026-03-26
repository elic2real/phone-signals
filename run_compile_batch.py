#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Iterable, List, Sequence

from cache_key_utils import directory_signature, ensure_dir


ROOT = Path(__file__).resolve().parent
COMPILER = ROOT / "run_market_node_compiler.py"


def load_lock_paths(lock_glob: str | None, locks_file: Path | None) -> List[Path]:
    paths: List[Path] = []
    if lock_glob:
        paths.extend(sorted(ROOT.glob(lock_glob)))
    if locks_file:
        for line in locks_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            paths.append((ROOT / line) if not line.startswith("/") else Path(line))
    deduped: List[Path] = []
    seen = set()
    for path in paths:
        rp = path.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        deduped.append(rp)
    return deduped


def node_key_from_lock(lock_path: Path) -> str:
    data = json.loads(lock_path.read_text())
    pair = str(data.get("pair", "UNKNOWN")).upper()
    weekday = str(data.get("weekday", "UNKNOWN")).lower()
    session = str(data.get("session", "UNKNOWN")).lower()
    return f"{pair}__{weekday}__{session}"


def _worker_compile(task: dict) -> dict:
    lock_path = Path(task["lock_path"]).resolve()
    node_key = task["node_key"]
    staging_root = Path(task["staging_root"]).resolve()
    compiler_args: Sequence[str] = task["compiler_args"]

    run_root = ensure_dir(staging_root / f"{node_key}__{uuid.uuid4().hex}")
    stage_dir = run_root / node_key
    ensure_dir(stage_dir.parent)
    log_path = run_root / "compile.log"

    cmd = [
        sys.executable,
        str(COMPILER),
        "--dataset-lock",
        str(lock_path),
        "--output-root",
        str(stage_dir),
        *compiler_args,
    ]

    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.run(cmd, stdout=log_file, stderr=subprocess.STDOUT, cwd=ROOT)
    if proc.returncode != 0:
        raise RuntimeError(f"Compiler failed for {node_key}; see {log_path}")
    return {
        "node_key": node_key,
        "stage_dir": str(stage_dir),
        "log_path": str(log_path),
    }


def compile_nodes(
    lock_paths: Iterable[Path],
    staging_root: Path,
    compiler_args: Sequence[str],
    max_workers: int,
) -> dict[str, dict]:
    ensure_dir(staging_root)
    tasks = []
    for lock_path in lock_paths:
        node_key = node_key_from_lock(lock_path)
        tasks.append(
            {
                "lock_path": str(lock_path),
                "node_key": node_key,
                "staging_root": str(staging_root),
                "compiler_args": list(compiler_args),
            }
        )

    results: dict[str, dict] = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_worker_compile, task): task for task in tasks}
        for fut in concurrent.futures.as_completed(future_map):
            task = future_map[fut]
            try:
                res = fut.result()
            except Exception as exc:
                raise RuntimeError(f"Node compile failed for {task['node_key']}: {exc}") from exc
            results[res["node_key"]] = res
    return results


def move_results(results: dict[str, dict], final_root: Path) -> None:
    ensure_dir(final_root)
    for node_key, payload in results.items():
        stage_dir = Path(payload["stage_dir"]).resolve()
        stage_parent = stage_dir.parent
        target = final_root / node_key
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(stage_dir), str(target))
        shutil.rmtree(stage_parent, ignore_errors=True)


def compare_trees(serial_results: dict[str, dict], parallel_results: dict[str, dict]) -> None:
    for node_key in serial_results:
        serial_dir = Path(serial_results[node_key]["stage_dir"]).resolve()
        parallel_dir = Path(parallel_results[node_key]["stage_dir"]).resolve()
        serial_sig = directory_signature(serial_dir)
        parallel_sig = directory_signature(parallel_dir)
        if serial_sig != parallel_sig:
            raise RuntimeError(
                f"Determinism mismatch for {node_key}: serial={serial_sig} parallel={parallel_sig}"
            )


def cleanup(results: dict[str, dict]) -> None:
    for payload in results.values():
        stage_dir = Path(payload["stage_dir"]).resolve()
        stage_parent = stage_dir.parent
        shutil.rmtree(stage_parent, ignore_errors=True)


def run_with_verification(
    lock_paths: List[Path],
    output_root: Path,
    compiler_args: Sequence[str],
    max_workers: int,
) -> None:
    verify_serial_root = ensure_dir(output_root / "__verify_serial")
    verify_parallel_root = ensure_dir(output_root / "__verify_parallel")

    print("[verify] Running serial baseline...", flush=True)
    serial_results = compile_nodes(lock_paths, verify_serial_root, compiler_args, max_workers=1)

    print("[verify] Running parallel batch...", flush=True)
    parallel_results = compile_nodes(lock_paths, verify_parallel_root, compiler_args, max_workers=max_workers)

    print("[verify] Comparing outputs...", flush=True)
    compare_trees(serial_results, parallel_results)

    print("[verify] Moving verified outputs into final directory...", flush=True)
    move_results(parallel_results, output_root)

    cleanup(serial_results)
    cleanup(parallel_results)


def run_normal(
    lock_paths: List[Path],
    output_root: Path,
    compiler_args: Sequence[str],
    max_workers: int,
) -> None:
    staging_root = ensure_dir(output_root / "__staging")
    results = compile_nodes(lock_paths, staging_root, compiler_args, max_workers)
    move_results(results, output_root)
    cleanup(results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parallel market node compiler")
    parser.add_argument(
        "--lock-glob",
        default="dataset_lock__*__*__*.json",
        help="Glob (relative to repo root) selecting dataset locks",
    )
    parser.add_argument(
        "--locks-file",
        type=Path,
        help="Optional file listing dataset lock paths (one per line)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=ROOT / "compiled_market_nodes",
        help="Destination root for compiled nodes",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum concurrent workers",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run serial + parallel builds and compare hashes before publishing",
    )
    parser.add_argument(
        "compiler_args",
        nargs=argparse.REMAINDER,
        help="Additional arguments forwarded to run_market_node_compiler.py",
    )
    args = parser.parse_args()
    if args.max_workers < 1:
        parser.error("--max-workers must be >= 1")
    return args


def main() -> None:
    args = parse_args()
    lock_paths = load_lock_paths(args.lock_glob, args.locks_file)
    if not lock_paths:
        raise SystemExit("No dataset locks matched")

    output_root = args.output_root.resolve()
    compiler_args = args.compiler_args or []

    print(json.dumps({"locks": len(lock_paths), "output_root": str(output_root), "verify": args.verify}, indent=2))

    if args.verify:
        run_with_verification(lock_paths, output_root, compiler_args, args.max_workers)
    else:
        run_normal(lock_paths, output_root, compiler_args, args.max_workers)


if __name__ == "__main__":
    main()
