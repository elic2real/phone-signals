#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path


def _load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _fail(msg: str):
    print(f"FAIL: {msg}")
    return False


def _ok(msg: str):
    print(f"OK: {msg}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Pre-live truth gate: artifacts, flags, replay proofs.")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--report", default="proof_artifacts/ceiling_campaign_report.json")
    ap.add_argument("--s1", default="proof_artifacts/FINAL_MAP_VERIFY_POSTCAL_S1.json")
    ap.add_argument("--s2", default="proof_artifacts/FINAL_MAP_VERIFY_POSTCAL_S2.json")
    args = ap.parse_args()

    ok = True

    # 1) no proof-force flags in current env
    if os.getenv("PROOF_FORCE_DECISION_TICKS", "0").strip() not in ("", "0", "false", "False", "FALSE"):
        ok &= _fail("PROOF_FORCE_DECISION_TICKS must be off")
    else:
        ok &= _ok("PROOF_FORCE_DECISION_TICKS off")
    if os.getenv("PROOF_FORCE_CAL_APPLY", "0").strip() not in ("", "0", "false", "False", "FALSE"):
        ok &= _fail("PROOF_FORCE_CAL_APPLY must be off")
    else:
        ok &= _ok("PROOF_FORCE_CAL_APPLY off")

    # 2) active artifacts present
    aa_path = Path(args.active_artifacts)
    if not aa_path.exists():
        ok &= _fail(f"missing active artifacts: {aa_path}")
        print("STOP")
        return 1
    aa = _load_json(aa_path)
    sessions = (aa.get("sessions") or {})
    for s in ("ASIA", "LONDON", "NY"):
        node = sessions.get(s) or {}
        patch = Path(str(node.get("patch", "")))
        spec = Path(str(node.get("vol_spec", "")))
        if not patch.exists():
            ok &= _fail(f"{s} patch missing: {patch}")
        else:
            ok &= _ok(f"{s} patch exists")
        if not spec.exists():
            ok &= _fail(f"{s} vol_spec missing: {spec}")
        else:
            ok &= _ok(f"{s} vol_spec exists")

    # 3) replay proofs present and passing
    for lbl, pth in (("S1", Path(args.s1)), ("S2", Path(args.s2))):
        if not pth.exists():
            ok &= _fail(f"{lbl} proof missing: {pth}")
            continue
        j = _load_json(pth)
        d = j.get("delta_vs_nopatch", {})
        ddEph = float(d.get("ddEph", -1))
        ddCAP = float(d.get("ddCAP_mean", -1))
        ddTail = float(d.get("ddTail_mean_Eph", -1))
        touched = int(d.get("touched_targets", 0))
        if ddEph > 0 and ddCAP >= 0 and ddTail >= 0 and touched >= 24:
            ok &= _ok(f"{lbl} dd-gate pass (ddEph={ddEph:.4f}, ddCAP={ddCAP:.4f}, ddTail={ddTail:.4f}, touched={touched})")
        else:
            ok &= _fail(f"{lbl} dd-gate fail (ddEph={ddEph:.4f}, ddCAP={ddCAP:.4f}, ddTail={ddTail:.4f}, touched={touched})")

    # 4) campaign report post_calibration_check pass
    rp = Path(args.report)
    if rp.exists():
        rj = _load_json(rp)
        pcc = rj.get("post_calibration_check", {})
        if bool(pcc.get("pass", False)):
            ok &= _ok("post_calibration_check pass")
        else:
            ok &= _fail("post_calibration_check missing or fail")
    else:
        ok &= _fail(f"missing report: {rp}")

    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

