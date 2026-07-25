"""Turn benchmark CSVs into the plots and tables used in report.md.

Run from the repo root (needs pandas + matplotlib locally, not in CI):
    python benchmarks/plot.py
"""
from __future__ import annotations

import glob
import os

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RAW = "benchmarks/raw"
OUT = "benchmarks"


def load() -> pd.DataFrame:
    frames = []
    for path in sorted(glob.glob(os.path.join(RAW, "*.csv"))):
        df = pd.read_csv(path)
        df["file"] = os.path.basename(path)
        frames.append(df)
    if not frames:
        raise SystemExit(f"no CSVs in {RAW} - run benchmarks/run_bench.sh first")
    df = pd.concat(frames, ignore_index=True)
    # Warm-up rows are recorded but never scored: the first requests pay
    # for cold caches and JIT-ish warmup in llama.cpp.
    return df[(~df["warmup"]) & (df["status"] == 200)]


def throughput(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate completion tokens/sec per (label, param) run."""
    rows = []
    for (label, mode, param), g in df.groupby(["label", "mode", "param"]):
        start = g["start_unix_ms"].min()
        end = (g["start_unix_ms"] + g["total_ms"]).max()
        window_s = (end - start) / 1000.0
        rows.append({
            "label": label,
            "mode": mode,
            "param": param,
            "requests": len(g),
            "tokens": int(g["tokens"].sum()),
            "window_s": round(window_s, 1),
            "tokens_per_s": round(g["tokens"].sum() / window_s, 2),
            "ttft_p50_ms": round(g["ttft_ms"].quantile(0.50)),
            "ttft_p95_ms": round(g["ttft_ms"].quantile(0.95)),
            "ttft_p99_ms": round(g["ttft_ms"].quantile(0.99)),
            "tpot_p50_ms": round(g["tpot_ms"].quantile(0.50), 2),
            "tpot_p99_ms": round(g["tpot_ms"].quantile(0.99), 2),
        })
    return pd.DataFrame(rows).sort_values(["mode", "param", "label"])


def plot_throughput(t: pd.DataFrame) -> None:
    closed = t[t["mode"] == "closed"].copy()
    closed["param"] = closed["param"].astype(int)
    closed = closed[closed["param"].isin([1, 8, 16])]
    pivot = closed.pivot(index="param", columns="label", values="tokens_per_s")
    ax = pivot.plot(kind="bar", figsize=(7, 4.5), rot=0)
    ax.set_xlabel("concurrency (closed loop)")
    ax.set_ylabel("completion tokens / second")
    ax.set_title("Throughput: sequential vs continuous batching")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "throughput.png"), dpi=140)
    plt.close()


def plot_ttft(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for label, g in df[df["mode"] == "closed"].groupby("label"):
        s = g["ttft_ms"].sort_values().reset_index(drop=True)
        ax.plot(s, (s.index + 1) / len(s), label=label)
    ax.set_xlabel("time to first token (ms)")
    ax.set_ylabel("fraction of requests")
    ax.set_title("TTFT distribution (all closed-loop runs)")
    ax.grid(alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "ttft_cdf.png"), dpi=140)
    plt.close()


def main() -> None:
    df = load()
    t = throughput(df)
    t.to_csv(os.path.join(OUT, "summary.csv"), index=False)
    print(t.to_string(index=False))

    seq = t[(t["label"] == "sequential") & (t["mode"] == "closed")]
    bat = t[(t["label"] == "batched") & (t["mode"] == "closed")]
    for conc in ("1", "8", "16"):
        s = seq[seq["param"] == conc]
        b = bat[bat["param"] == conc]
        if len(s) and len(b):
            mult = b["tokens_per_s"].iloc[0] / s["tokens_per_s"].iloc[0]
            print(f"concurrency {conc}: {mult:.2f}x")

    plot_throughput(t)
    plot_ttft(df)
    print("wrote summary.csv, throughput.png, ttft_cdf.png")


if __name__ == "__main__":
    main()
