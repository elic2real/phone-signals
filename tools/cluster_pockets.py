#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import numpy as np
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover
    KMeans = None
    StandardScaler = None


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _zscore(X: np.ndarray) -> np.ndarray:
    mu = np.mean(X, axis=0)
    sd = np.std(X, axis=0)
    sd = np.where(sd <= 1e-12, 1.0, sd)
    return (X - mu) / sd


def _kmeans_fallback(Z: np.ndarray, k: int, seed: int, iters: int = 50) -> np.ndarray:
    rng = np.random.default_rng(seed)
    n = Z.shape[0]
    idx = rng.choice(n, size=k, replace=False)
    centers = Z[idx].copy()
    labels = np.zeros(n, dtype=int)
    for _ in range(iters):
        d = ((Z[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
        new_labels = np.argmin(d, axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        for i in range(k):
            pts = Z[labels == i]
            if len(pts) == 0:
                centers[i] = Z[rng.integers(0, n)]
            else:
                centers[i] = pts.mean(axis=0)
    return labels


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", required=True)
    ap.add_argument("--k", type=int, default=40)
    ap.add_argument("--out-json", default="calibration/pocket_clusters_v1.json")
    ap.add_argument("--seed", type=int, default=1337)
    args = ap.parse_args()

    rows = list(csv.DictReader(Path(args.in_csv).open(encoding="utf-8")))
    feats = [
        "ddEph_min",
        "Eph_patch_min",
        "E_per_trade_min",
        "entries_h_min",
        "exits_h_min",
        "hold_sec_max",
        "touches_min",
        "dEph_proxy_min",
    ]
    X = np.array([[_f(r.get(c)) for c in feats] for r in rows], dtype=float)
    if len(rows) == 0:
        raise SystemExit("empty input")
    Z = StandardScaler().fit_transform(X) if StandardScaler is not None else _zscore(X)
    k = max(2, min(int(args.k), len(rows)))
    if KMeans is not None:
        km = KMeans(n_clusters=k, random_state=args.seed, n_init=20)
        labels = km.fit_predict(Z)
    else:
        labels = _kmeans_fallback(Z, k=k, seed=args.seed)

    pocket_to_cluster: dict[str, int] = {}
    clusters: dict[int, list[str]] = {}
    for r, lb in zip(rows, labels):
        key = str(r.get("target_key", "") or "")
        pocket_to_cluster[key] = int(lb)
        clusters.setdefault(int(lb), []).append(key)

    cluster_summaries = []
    for cid, keys in sorted(clusters.items()):
        sub = [rows[i] for i, lb in enumerate(labels) if int(lb) == cid]
        cluster_summaries.append(
            {
                "cluster_id": cid,
                "size": len(keys),
                "sample_keys": keys[:5],
                "ddEph_min_mean": float(np.mean([_f(r.get("ddEph_min")) for r in sub])),
                "Eph_patch_min_mean": float(np.mean([_f(r.get("Eph_patch_min")) for r in sub])),
            }
        )

    out = {
        "version": 1,
        "k": k,
        "source_csv": args.in_csv,
        "features": feats,
        "pocket_to_cluster": pocket_to_cluster,
        "cluster_summaries": cluster_summaries,
    }
    op = Path(args.out_json)
    op.parent.mkdir(parents=True, exist_ok=True)
    op.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(op)
    print(f"clusters={k} pockets={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
