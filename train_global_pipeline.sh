#!/usr/bin/env bash
set -euo pipefail

SESSION="${SESSION:-LONDON}"
PAIRS="${PAIRS:-EUR_USD USD_JPY AUD_JPY}"
STATS_PATH="${STATS_PATH:-stats/session_${SESSION}.json}"
OUT_ROOT="${OUT_ROOT:-reports/global_pipeline}"
SEED="${SEED:-123}"

ENTRY_SYNTH_RUNS="${ENTRY_SYNTH_RUNS:-12}"
ENTRY_MAX_CANDIDATES="${ENTRY_MAX_CANDIDATES:-${ENTRY_CANDIDATES:-24}}"
ENTRY_TOP_K="${ENTRY_TOP_K:-5}"

AEE_SYNTH_RUNS="${AEE_SYNTH_RUNS:-12}"
AEE_MAX_CANDIDATES="${AEE_MAX_CANDIDATES:-${AEE_CANDIDATES:-20}}"
AEE_TOP_K="${AEE_TOP_K:-5}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${OUT_ROOT}/${RUN_ID}"
ENTRY_DIR="${RUN_DIR}/entry"
AEE_DIR="${RUN_DIR}/aee"
SUMMARY_PATH="${RUN_DIR}/global_pipeline_summary.md"

mkdir -p "${ENTRY_DIR}" "${AEE_DIR}"
PRE_ENTRY_BASELINE="${RUN_DIR}/prev_entry_global_baseline.json"
PRE_AEE_BASELINE="${RUN_DIR}/prev_aee_global_baseline.json"
if [[ -f tunes/entry_global_baseline.json ]]; then cp tunes/entry_global_baseline.json "${PRE_ENTRY_BASELINE}"; fi
if [[ -f tunes/aee_global_baseline.json ]]; then cp tunes/aee_global_baseline.json "${PRE_AEE_BASELINE}"; fi

echo "[pipeline] run_id=${RUN_ID}"
echo "[pipeline] session=${SESSION}"
echo "[pipeline] pairs=${PAIRS}"
echo "[pipeline] stats=${STATS_PATH}"

if [[ ! -f "${STATS_PATH}" ]]; then
  echo "[pipeline] missing stats file: ${STATS_PATH}" >&2
  exit 1
fi

python3 -m py_compile train_entry_global.py train_aee_global.py

echo "[pipeline] step=entry_global"
python3 train_entry_global.py \
  --session "${SESSION}" \
  --pairs ${PAIRS} \
  --stats "${STATS_PATH}" \
  --synthetic-runs "${ENTRY_SYNTH_RUNS}" \
  --max-candidates "${ENTRY_MAX_CANDIDATES}" \
  --top-k "${ENTRY_TOP_K}" \
  --seed "${SEED}" \
  --outdir "${ENTRY_DIR}" \
  | tee "${ENTRY_DIR}/train_entry_stdout.json"

echo "[pipeline] step=aee_global"
python3 train_aee_global.py \
  --session "${SESSION}" \
  --pairs ${PAIRS} \
  --stats "${STATS_PATH}" \
  --synthetic-runs "${AEE_SYNTH_RUNS}" \
  --max-candidates "${AEE_MAX_CANDIDATES}" \
  --top-k "${AEE_TOP_K}" \
  --seed "$((SEED + 1))" \
  --outdir "${AEE_DIR}" \
  | tee "${AEE_DIR}/train_aee_stdout.json"

python3 - <<'PY' "${RUN_DIR}" "${SUMMARY_PATH}" "${SESSION}" "${PAIRS}" "${STATS_PATH}" "${RUN_ID}" "${PRE_ENTRY_BASELINE}" "${PRE_AEE_BASELINE}"
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
summary_path = Path(sys.argv[2])
session = sys.argv[3]
pairs = sys.argv[4]
stats_path = sys.argv[5]
run_id = sys.argv[6]
pre_entry_path = Path(sys.argv[7])
pre_aee_path = Path(sys.argv[8])

entry = json.loads(Path("tunes/entry_global_baseline.json").read_text(encoding="utf-8"))
aee = json.loads(Path("tunes/aee_global_baseline.json").read_text(encoding="utf-8"))
pre_entry = json.loads(pre_entry_path.read_text(encoding="utf-8")) if pre_entry_path.exists() else {}
pre_aee = json.loads(pre_aee_path.read_text(encoding="utf-8")) if pre_aee_path.exists() else {}

def _d(new_obj, old_obj, key):
    try:
        if key in new_obj and key in old_obj:
            return float(new_obj[key]) - float(old_obj[key])
    except Exception:
        return None
    return None

entry_score_delta = _d(entry, pre_entry, "score")
aee_score_delta = _d(aee, pre_aee, "score")

lines = [
    "# Global Training Pipeline Summary",
    f"- run_id: `{run_id}`",
    f"- session: `{session}`",
    f"- pairs: `{pairs}`",
    f"- stats_path: `{stats_path}`",
    "",
    "## Entry Baseline",
    f"- version: `{entry.get('version')}`",
    f"- score: `{entry.get('score')}`",
    f"- score_delta_vs_prev: `{entry_score_delta}`",
    f"- synthetic_pph_mean: `{entry.get('synthetic_pph_mean')}`",
    f"- synthetic_pips_mean: `{entry.get('synthetic_pips_mean')}`",
    f"- synthetic_tail_loss_rate: `{entry.get('synthetic_tail_loss_rate')}`",
    f"- friction_severity_mult: `{entry.get('friction_severity_mult')}`",
    "- knobs:",
]
for k, v in sorted((entry.get("knobs") or {}).items()):
    lines.append(f"  - `{k}`: `{v}`")

lines += [
    "",
    "## AEE Baseline",
    f"- version: `{aee.get('version')}`",
    f"- score: `{aee.get('score')}`",
    f"- score_delta_vs_prev: `{aee_score_delta}`",
    f"- pph_mean: `{aee.get('pph_mean')}`",
    f"- pips_mean: `{aee.get('pips_mean')}`",
    f"- capture_mean: `{aee.get('capture_mean')}`",
    f"- giveback_mean: `{aee.get('giveback_mean')}`",
    f"- dead_hold_rate: `{aee.get('dead_hold_rate')}`",
    f"- tail_loss_rate: `{aee.get('tail_loss_rate')}`",
    "- knobs:",
]
for k, v in sorted((aee.get("knobs") or {}).items()):
    lines.append(f"  - `{k}`: `{v}`")

lines += [
    "",
    "## Artifacts",
    f"- entry leaderboard: `{run_dir / 'entry' / 'entry_global_leaderboard.json'}`",
    f"- aee leaderboard: `{run_dir / 'aee' / 'aee_global_leaderboard.json'}`",
    f"- entry stdout: `{run_dir / 'entry' / 'train_entry_stdout.json'}`",
    f"- aee stdout: `{run_dir / 'aee' / 'train_aee_stdout.json'}`",
]

summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(summary_path)
PY

echo "[pipeline] summary=${SUMMARY_PATH}"
echo "[pipeline] done"
