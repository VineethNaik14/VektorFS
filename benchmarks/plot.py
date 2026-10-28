"""Turn benchmarks/results/*.csv into PNG charts (needs pandas + matplotlib)."""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

R = Path(__file__).parent / "results"


def save(fig, name):
    fig.tight_layout()
    fig.savefig(R / f"{name}.png", dpi=140)
    plt.close(fig)
    print(f"  -> benchmarks/results/{name}.png")


def throughput():
    df = pd.read_csv(R / "throughput.csv")
    g = df.groupby(["rf", "chunk_mib", "concurrency"])[["upload_mib_s", "download_mib_s"]].mean().reset_index()
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=False)
    for ax, col, title in ((axes[0], "upload_mib_s", "Upload"), (axes[1], "download_mib_s", "Download")):
        for (rf, ch), part in g.groupby(["rf", "chunk_mib"]):
            ax.plot(part["concurrency"], part[col], marker="o", label=f"RF={rf}, {ch} MiB chunks")
        ax.set_xscale("log", base=2); ax.set_xticks([1, 2, 4, 8]); ax.set_xticklabels([1, 2, 4, 8])
        ax.set_xlabel("parallel chunk transfers"); ax.set_ylabel("MiB/s"); ax.set_title(title)
        ax.set_ylim(bottom=0); ax.grid(alpha=.3); ax.legend(fontsize=8)
    save(fig, "throughput")


def distribution():
    df = pd.read_csv(R / "distribution.csv")
    configs = list(dict.fromkeys(df["config"]))
    fig, axes = plt.subplots(1, len(configs), figsize=(5.5 * len(configs), 4))
    for ax, cfg in zip(axes if len(configs) > 1 else [axes], configs):
        part = df[df["config"] == cfg]
        ax.bar(part["node"], part["chunks"])
        ax.set_title(f"{cfg}  (CV = {part['cv'].iloc[0]:.3f})")
        ax.set_ylabel("chunk replicas stored"); ax.grid(axis="y", alpha=.3)
    save(fig, "distribution")


def recovery():
    df = pd.read_csv(R / "recovery.csv")
    fig, ax = plt.subplots(figsize=(6, 4))
    x = range(len(df))
    ax.bar([i - .2 for i in x], df["detect_s"], .4, label="failure detected (heartbeat TTL)")
    ax.bar([i + .2 for i in x], df["fully_rereplicated_s"], .4, label="back to full replication")
    ax.set_xticks(list(x)); ax.set_xticklabels([f"kill {v}" for v in df["victim"]])
    ax.set_ylabel("seconds after SIGKILL"); ax.legend(fontsize=8); ax.grid(axis="y", alpha=.3)
    save(fig, "recovery")


if __name__ == "__main__":
    for fn in (throughput, distribution, recovery):
        if (R / f"{fn.__name__}.csv").exists():
            fn()
