"""
Computes MQTT test statistics from publisher / analyser TSV files and
produces box-and-whisker plots via matplotlib. Python preferred due to graphical math suite.

  - TSV files are written in append mode; [start_ts, end_ts] scopes a run.
  - Loss = join on (counter, pub_timestamp); missing analyser rows = lost++.
  - Out-of-order is measured on arrival order (file row order) before sort.
  - Gap uses recv_timestamp differences between strictly consecutive counters
    within the same burst (pub_ts proximity ≤ BURST_SECS).
"""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BURST_SECS = 30  # maximum burst window in seconds
MAX_LINE = 4096


@dataclass
class RowPair:
    counter: int
    pub_ts: int
    recv_ts: int = 0


@dataclass
class TestStats:
    pub_file: str = ""
    ana_file: str = ""
    sys_file: str = ""
    start_ts: int = 0
    end_ts: int = 0

    # test parameters
    pub_qos: int = 0
    sub_qos: int = 0
    delay_ms: int = 0
    msg_size: int = 0

    # computed fields
    pub_attempts: int = 0
    pub_success: int = 0
    pub_success_rate: float = 0.0
    exp_msg: int = 0
    actual_recv: int = 0
    loss_count: int = 0
    loss_perc: float = 0.0
    out_of_order_count: int = 0
    out_of_order_perc: float = 0.0
    dup_count: int = 0
    dup_perc: float = 0.0
    mean_rate_msgs_per_sec: float = 0.0
    mean_gap_ms: float = 0.0
    stddev_gap_ms: float = 0.0
    gap_sample_count: int = 0
    sys_metrics: dict = field(default_factory=dict)  # Store SYS metrics


@dataclass
class TestMetadata:
    pub_qos: int
    sub_qos: int
    delay_ms: int
    msg_size: int
    start_ts: int
    end_ts: int


def _ts_ms_to_local_iso(ts_ms: int) -> str:
    """Convert a Unix millisecond timestamp to a local-timezone ISO-8601 string."""
    from datetime import datetime
    return datetime.fromtimestamp(ts_ms / 1000.0).isoformat(timespec="seconds")


def _sort_key(r: RowPair):
    return (r.counter, r.pub_ts)


def _read_publisher(path: str, start_ts: int, end_ts: int
                    ) -> tuple[list[RowPair], int, int]:
    """
    Read publisher TSV; keep rows where pub_ts ∈ [start_ts, end_ts] and
    mqtt_success == 0.  Returns (sorted rows, attempts, successes).
    """
    rows: list[RowPair] = []
    attempts = 0
    successes = 0

    try:
        fp = open(path, newline="")
    except OSError as e:
        print(f"stats: cannot open publisher file: {path}  ({e})", file=sys.stderr)
        return [], 0, 0

    with fp:
        reader = csv.reader(fp, delimiter="\t")
        header = True
        for parts in reader:
            if header:
                header = False
                continue
            if len(parts) < 5:
                continue
            try:
                counter = int(parts[0])
                pub_ts = int(parts[1])
                # topic      = parts[2]
                # msg_size   = int(parts[3])
                mqtt_success = int(parts[4])
            except ValueError:
                continue

            if not (start_ts <= pub_ts <= end_ts):
                continue
            attempts += 1

            if mqtt_success != 0:
                continue
            successes += 1

            rows.append(RowPair(counter=counter, pub_ts=pub_ts, recv_ts=0))

    rows.sort(key=_sort_key)
    return rows, attempts, successes


def _read_analyser(path: str, start_ts: int, end_ts: int
                   ) -> tuple[list[RowPair], int, int]:
    """
    Read analyser TSV; keep rows where pub_ts ∈ [start_ts, end_ts].
    Computes out-of-order and duplicate counts on *arrival order* before sort.
    Returns (sorted rows, ooo_count, dup_count).
    """
    rows: list[RowPair] = []
    ooo_count = 0
    dup_count = 0

    try:
        fp = open(path, newline="")
    except OSError as e:
        print(f"stats: cannot open analyser file: {path}  ({e})", file=sys.stderr)
        return [], 0, 0

    prev_counter: Optional[int] = None
    prev_pub_ts: Optional[int] = None

    with fp:
        reader = csv.reader(fp, delimiter="\t")
        header = True
        for parts in reader:
            if header:
                header = False
                continue
            if len(parts) < 9:
                continue
            try:
                counter = int(parts[0])
                pub_ts = int(parts[1])
                # topic  = parts[2]
                # msg_size = int(parts[3])
                recv_ts = int(parts[4])
                # latency  = int(parts[5])
                # pub_qos  = int(parts[6])
                # sub_qos  = int(parts[7])
                # delay_ms = int(parts[8])
            except ValueError:
                continue

            if not (start_ts <= pub_ts <= end_ts):
                continue

            # out-of-order: counter went backwards in arrival order
            if prev_counter is not None and counter < prev_counter:
                ooo_count += 1

            # duplicate: identical (counter, pub_ts) pair
            if (prev_counter is not None
                    and counter == prev_counter
                    and pub_ts == prev_pub_ts):
                dup_count += 1

            rows.append(RowPair(counter=counter, pub_ts=pub_ts, recv_ts=recv_ts))
            prev_counter = counter
            prev_pub_ts = pub_ts

    rows.sort(key=_sort_key)
    return rows, ooo_count, dup_count


def _calc_loss(pub: list[RowPair], ana: list[RowPair]) -> int:
    """
    Merge-join on (counter, pub_ts).  Publisher rows with no matching
    analyser row are counted as lost.
    """
    loss = 0
    pi = ai = 0
    pub_n = len(pub)
    ana_n = len(ana)

    while pi < pub_n and ai < ana_n:
        pk = _sort_key(pub[pi])
        ak = _sort_key(ana[ai])
        if pk == ak:
            pi += 1;
            ai += 1  # match!
        elif pk < ak:
            loss += 1;
            pi += 1  # sent but never received
        else:
            ai += 1  # received but not in publisher (unexpected)

    loss += pub_n - pi  # remaining publisher rows = lost
    return loss


def _calc_gaps(rows: list[RowPair]
               ) -> tuple[float, float, int, list[float]]:
    """
    For each pair of adjacent sorted rows with strictly consecutive counters
    and pub_ts difference ≤ BURST_SECS*1000, compute the recv_ts gap (ms).

    Returns (mean_gap, stddev_gap, sample_count, raw_gap_list).
    raw_gap_list is provided for downstream box-plot use.
    """
    gap_values: list[float] = []

    for i in range(len(rows) - 1):
        if rows[i + 1].counter != rows[i].counter + 1:
            continue
        pub_diff = rows[i + 1].pub_ts - rows[i].pub_ts
        if not (0 <= pub_diff <= BURST_SECS * 1000):
            continue
        gap = rows[i + 1].recv_ts - rows[i].recv_ts
        if gap < 0:
            continue  # assume measurement artefact
        gap_values.append(float(gap))

    n = len(gap_values)
    if n == 0:
        return 0.0, 0.0, 0, []

    mean = sum(gap_values) / n
    var = sum((g - mean) ** 2 for g in gap_values) / n
    stddev = math.sqrt(var)
    return mean, stddev, n, gap_values


def calc_test_stats(stats: TestStats) -> int:
    """Populate *stats* in-place. Returns 0 on success, -1 on failure."""

    pub_rows, stats.pub_attempts, stats.pub_success = _read_publisher(
        stats.pub_file, stats.start_ts, stats.end_ts
    )
    if not pub_rows and stats.pub_attempts == 0:
        print(f"stats: no publisher data in window for {stats.pub_file}",
              file=sys.stderr)
        return -1

    if stats.pub_attempts > 0:
        # BUG FIX: original C code had pub_success_rate / pub_attempts (uses
        # uninitialised 0.0); corrected to pub_success / pub_attempts.
        stats.pub_success_rate = stats.pub_success / stats.pub_attempts * 100.0

    stats.exp_msg = len(pub_rows)

    ana_rows, ooo, dup = _read_analyser(
        stats.ana_file, stats.start_ts, stats.end_ts
    )
    if not ana_rows:
        print(f"stats: no analyser data in window for {stats.ana_file}",
              file=sys.stderr)
        return -1

    stats.actual_recv = len(ana_rows)
    stats.out_of_order_count = ooo
    stats.dup_count = dup

    if stats.actual_recv > 0:
        stats.out_of_order_perc = ooo / stats.actual_recv * 100.0
        stats.dup_perc = dup / stats.actual_recv * 100.0

    # Loss
    stats.loss_count = _calc_loss(pub_rows, ana_rows)
    if stats.exp_msg > 0:
        stats.loss_perc = stats.loss_count / stats.exp_msg * 100.0

    # Gaps
    (stats.mean_gap_ms,
     stats.stddev_gap_ms,
     stats.gap_sample_count,
     _gap_raw) = _calc_gaps(ana_rows)

    # Mean rate
    ana_n = len(ana_rows)
    if ana_n >= 2:
        duration_ms = ana_rows[-1].recv_ts - ana_rows[0].recv_ts
        if duration_ms > 0:
            stats.mean_rate_msgs_per_sec = ana_n / (duration_ms / 1000.0)
        else:
            stats.mean_rate_msgs_per_sec = ana_n / BURST_SECS
    elif ana_n == 1:
        stats.mean_rate_msgs_per_sec = 1.0 / BURST_SECS

    # Stash raw gaps on the object for plotting convenience
    stats._gap_raw = _gap_raw  # type: ignore[attr-defined]

    return 0


def correlate_with_sys(stats: TestStats) -> int:
    """Extract SYS topic values that fall within the test window and store them in stats.sys_metrics."""
    if not stats.sys_file:
        return 0

    try:
        fp = open(stats.sys_file, newline="")
    except OSError as e:
        print(f"stats: cannot open sys file: {stats.sys_file}  ({e})", file=sys.stderr)
        return -1

    # Define the SYS topics to extract and their corresponding column names
    SYS_TOPICS = {
        "publish/messages/dropped": "sys_publish_messages_dropped",
        "heap/current": "sys_heap_current",
        "load/messages/received/1min": "sys_load_messages_received_1min",
        "load/publish/dropped/1min": "sys_load_publish_dropped_1min",
        "store/messages/bytes": "sys_store_messages_bytes",
        "messages/stored": "sys_messages_stored",
    }

    with fp:
        reader = csv.reader(fp, delimiter="\t")
        header = True
        for parts in reader:
            if header:
                header = False
                continue
            if len(parts) < 3:
                continue
            try:
                recv_ts = int(parts[0])
                topic = parts[1]
                value = parts[2]
            except ValueError:
                continue

            if not (stats.start_ts <= recv_ts <= stats.end_ts):
                continue

            # Check if the topic matches any of the predefined keys
            for key, metric_name in SYS_TOPICS.items():
                if key in topic:
                    # Store the last value for this metric in the window
                    stats.sys_metrics[metric_name] = value
                    break

    return 0


def read_test_metadata(analyser_dir: str) -> list[TestMetadata]:
    """
    Read test_timestamps.tsv and return a list of TestMetadata.

    NOTE: the C write_ts_meta() has a double-format-string bug that corrupts
    this file (string-literal address printed as %d). If the file yields no
    valid rows, call discover_tests_from_files() instead.
    """
    path = Path(analyser_dir) / "test_timestamps.tsv"
    results: list[TestMetadata] = []

    try:
        fp = open(path, newline="")
    except OSError as e:
        print(f"stats: cannot open metadata file: {path}  ({e})", file=sys.stderr)
        return results

    with fp:
        reader = csv.reader(fp, delimiter="\t")
        header = True
        for parts in reader:
            if header:
                header = False
                continue
            if len(parts) < 6:
                continue
            try:
                pq = int(parts[0])
                sq = int(parts[1])
                d = int(parts[2])
                sz = int(parts[3])
                st = int(parts[4])
                et = int(parts[5])
            except ValueError:
                continue
            # Sanity-check: QoS must be 0-2, timestamps must be positive and
            # in order.  The C bug produces pointer-sized garbage values here.
            if not (0 <= pq <= 2 and 0 <= sq <= 2):
                continue
            if st <= 0 or et <= 0 or et < st:
                continue
            results.append(TestMetadata(
                pub_qos=pq, sub_qos=sq, delay_ms=d, msg_size=sz,
                start_ts=st, end_ts=et,
            ))

    return results


def discover_tests_from_files(analyser_dir: str,
                              publisher_dir: str) -> list[TestStats]:
    """
    Bypass the broken test_timestamps.tsv by scanning the analyser directory
    for files matching  pq{N}_sq{N}_d{N}_s{N}.tsv  and deriving every
    parameter from the filename and file contents.

    For each analyser file:
      • pub_qos, sub_qos, delay_ms, msg_size  ← parsed from filename
      • start_ts = min(pub_timestamp) in the file
      • end_ts   = max(pub_timestamp) in the file
      • pub_file = <publisher_dir>/pq{pub_qos}_d{delay_ms}_s{msg_size}.tsv
                   (publisher files omit sub_qos — matches your directory layout)
      • ana_file = the analyser file itself

    Returns a list of fully-populated TestStats objects ready to pass to
    calc_test_stats().  Files are returned sorted by (pub_qos, sub_qos,
    delay_ms, msg_size) for a deterministic report order.
    """
    import re

    ana_dir = Path(analyser_dir)
    pub_dir = Path(publisher_dir)
    pattern = re.compile(
        r"^pq(\d+)_sq(\d+)_d(\d+)_s(\d+)\.tsv$"
    )

    results: list[TestStats] = []

    for ana_path in sorted(ana_dir.glob("pq*_sq*_d*_s*.tsv")):
        m = pattern.match(ana_path.name)
        if not m:
            continue

        pq = int(m.group(1))
        sq = int(m.group(2))
        d = int(m.group(3))
        sz = int(m.group(4))

        # Derive the publisher file (no sub_qos in publisher naming).
        pub_path = pub_dir / f"pq{pq}_d{d}_s{sz}.tsv"
        if not pub_path.exists():
            print(f"  [warn] publisher file not found, skipping: {pub_path}", file=sys.stderr)
            continue

        # Scan the analyser file for the timestamp window.
        min_ts: Optional[int] = None
        max_ts: Optional[int] = None
        try:
            with open(ana_path, newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                header = True
                for parts in reader:
                    if header:
                        header = False
                        continue
                    if len(parts) < 2:
                        continue
                    try:
                        pub_ts = int(parts[1])
                    except ValueError:
                        continue
                    if min_ts is None or pub_ts < min_ts:
                        min_ts = pub_ts
                    if max_ts is None or pub_ts > max_ts:
                        max_ts = pub_ts
        except OSError as e:
            print(f"  [warn] cannot read {ana_path}: {e}", file=sys.stderr)
            continue

        if min_ts is None:
            print(f"  [warn] no data in {ana_path.name}, skipping.", file=sys.stderr)
            continue

        results.append(TestStats(
            pub_file=str(pub_path),
            ana_file=str(ana_path),
            sys_file="",  # filled in by caller if needed
            start_ts=min_ts,
            end_ts=max_ts,
            pub_qos=pq,
            sub_qos=sq,
            delay_ms=d,
            msg_size=sz,
        ))

    results.sort(key=lambda s: (s.pub_qos, s.sub_qos, s.delay_ms, s.msg_size))
    return results


def print_test_stats(stats: TestStats) -> None:
    print(f"[REPORT] Test: pq={stats.pub_qos} sq={stats.sub_qos} "
          f"delay={stats.delay_ms}ms size={stats.msg_size}")
    print(f"[REPORT] Publisher attempts: {stats.pub_attempts}")
    print(f"[REPORT] Publisher successes: {stats.pub_success} "
          f"({stats.pub_success_rate:.2f}%)")
    print(f"[REPORT] Expected (sent): {stats.exp_msg}")
    print(f"[REPORT] Received: {stats.actual_recv}")
    print(f"[REPORT] Lost: {stats.loss_count} ({stats.loss_perc:.4f}%)")
    print(f"[REPORT] Out of order: {stats.out_of_order_count} "
          f"({stats.out_of_order_perc:.4f}%)")
    print(f"[REPORT] Duplicates: {stats.dup_count} ({stats.dup_perc:.4f}%)")
    print(f"[REPORT] Mean rate: {stats.mean_rate_msgs_per_sec:.2f}msg/s")
    print(f"[REPORT] Mean gap (between message): {stats.mean_gap_ms:.3f}ms "
          f"(n={stats.gap_sample_count})")
    print(f"[REPORT] Stddev gap: {stats.stddev_gap_ms:.3f}ms")


def build_stats_tsv_header(fp) -> None:
    fp.write(
        "pub_qos\tsub_qos\tdelay_ms\tmsg_size\t"
        "pub_attempts\tpub_successes\tpub_success_rate\t"
        "expected\treceived\tlost\tloss_pct\t"
        "out_of_order\tooo_pct\t"
        "duplicates\tdup_pct\t"
        "mean_rate_msg_per_s\t"
        "mean_gap_ms\tstddev_gap_ms\tgap_samples\t"

        "sys_publish_messages_dropped\t"
        "sys_heap_current\t"
        "sys_load_messages_received_1min\t"
        "sys_load_publish_dropped_1min\t"
        "sys_store_messages_bytes\t"
        "sys_messages_stored\n"
    )


def build_stats_tsv_row(fp, stats: TestStats) -> None:
    fp.write(
        f"{stats.pub_qos}\t{stats.sub_qos}\t{stats.delay_ms}\t{stats.msg_size}\t"
        f"{stats.pub_attempts}\t{stats.pub_success}\t{stats.pub_success_rate:.2f}\t"
        f"{stats.exp_msg}\t{stats.actual_recv}\t"
        f"{stats.loss_count}\t{stats.loss_perc:.4f}\t"
        f"{stats.out_of_order_count}\t{stats.out_of_order_perc:.4f}\t"
        f"{stats.dup_count}\t{stats.dup_perc:.4f}\t"
        f"{stats.mean_rate_msgs_per_sec:.2f}\t"
        f"{stats.mean_gap_ms:.3f}\t{stats.stddev_gap_ms:.3f}\t"
        f"{stats.gap_sample_count}\t"

        f"{stats.sys_metrics.get('sys_publish_messages_dropped', 'N/A')}\t"
        f"{stats.sys_metrics.get('sys_heap_current', 'N/A')}\t"
        f"{stats.sys_metrics.get('sys_load_messages_received_1min', 'N/A')}\t"
        f"{stats.sys_metrics.get('sys_load_publish_dropped_1min', 'N/A')}\t"
        f"{stats.sys_metrics.get('sys_store_messages_bytes', 'N/A')}\t"
        f"{stats.sys_metrics.get('sys_messages_stored', 'N/A')}\n"
    )

def plot_box_whisker(all_stats: list[TestStats],
                     output_path: Optional[str] = None,
                     battery_start_ts: Optional[int] = None,
                     battery_end_ts: Optional[int] = None,
                     figsize: tuple = (40, 10)) -> None:
    """
    Produce a 2×2 grid of box-and-whisker plots from a list of TestStats.

    Parameters
    ----------
    all_stats         : list of populated TestStats objects.
    output_path       : if given, save figure here instead of showing.
    battery_start_ts  : Unix ms of the first test start (for subtitle).
    battery_end_ts    : Unix ms of the last test end   (for subtitle).
    figsize           : (width, height) in inches.  Widen to spread x-labels.
    """
    if not all_stats:
        print("plot_box_whisker: no stats to plot.", file=sys.stderr)
        return

    gap_records = []
    scalar_records = []

    for s in all_stats:
        # Updated label to include msg_size
        label = f"QoS {s.pub_qos}/{s.sub_qos}\n{s.delay_ms}ms delay\n{s.msg_size}B"
        gap_raw = getattr(s, "_gap_raw", [])
        for g in gap_raw:
            gap_records.append({"label": label, "gap_ms": g})
        scalar_records.append({
            "label": label,
            "loss_pct": s.loss_perc,
            "ooo_pct": s.out_of_order_perc,
            "dup_pct": s.dup_perc,
        })

    gap_df = pd.DataFrame(gap_records)
    scalar_df = pd.DataFrame(scalar_records)

    fig, axes = plt.subplots(2, 2, figsize=figsize)

    title = "MQTT Test Statistics – Box & Whisker"
    if battery_start_ts is not None and battery_end_ts is not None:
        subtitle = (f"battery start: {_ts_ms_to_local_iso(battery_start_ts)}"
                    f"  –  end: {_ts_ms_to_local_iso(battery_end_ts)}")
        fig.suptitle(f"{title}\n{subtitle}", fontsize=13, fontweight="bold", linespacing=1.6)
    else:
        fig.suptitle(title, fontsize=14, fontweight="bold")

    def _box(ax, df, col, title, ylabel, colour, cap_multiplier=4.0):
        groups = [grp[col].values for _, grp in df.groupby("label", sort=False)]
        labels = [lbl for lbl, _ in df.groupby("label", sort=False)]

        # Calculate outliers for each group
        outlier_counts = []
        for group in groups:
            q1 = np.percentile(group, 25)
            q3 = np.percentile(group, 75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = [x for x in group if x < lower_bound or x > upper_bound]
            outlier_counts.append(len(outliers))

        annotated_labels = []
        for label, count in zip(labels, outlier_counts):
            if count > 100:
                annotated_labels.append(f"{label} (***)")
            elif count > 10:
                annotated_labels.append(f"{label} (**)")
            elif count > 0:
                annotated_labels.append(f"{label} (*)")
            else:
                annotated_labels.append(label)

        # Plot the boxplot
        bp = ax.boxplot(
            groups,
            tick_labels=annotated_labels,  # fixed: use tick_labels instead of labels
            patch_artist=True,
            medianprops=dict(color="black", linewidth=2),
            flierprops=dict(marker='o', markersize=3, alpha=0.5),
            showfliers=True  # still show outliers beyond the cap
        )

        # Set box colors
        for patch in bp["boxes"]:
            patch.set_facecolor(colour)
            patch.set_alpha(0.6)

        # Cap y-axis based on reasonable range
        all_values = np.concatenate(groups)
        q99 = np.percentile(all_values, 99)
        q1 = np.percentile(all_values, 1)

        iqr_all = np.percentile(all_values, 75) - np.percentile(all_values, 25)
        proposed_cap = max(q99, np.percentile(all_values, 75) + cap_multiplier * iqr_all)

        ax.set_ylim(q1, proposed_cap)

        # Add a note if data was capped
        max_actual = np.max(all_values)
        if max_actual > proposed_cap:
            ax.text(0.98, 0.02, f"{max_actual:.0f}ms outlier clipped",
                    transform=ax.transAxes, ha='right', va='bottom',
                    fontsize=8, style='italic', color='gray')

        ax.set_title(title, fontsize=11)
        ax.set_ylabel(ylabel)
        ax.set_xlabel("QoS pair / delay / msg_size")
        ax.tick_params(axis="x", labelsize=6, rotation=45)
        ax.grid(axis="y", linestyle="--", alpha=0.5)

    if not gap_df.empty:
        _box(axes[0, 0], gap_df, "gap_ms", "Inter-message gap", "ms", "#4C72B0")
    else:
        axes[0, 0].set_title("Inter-message gap (no data)")

    _box(axes[0, 1], scalar_df, "loss_pct", "Message loss", "%", "#DD8452")
    _box(axes[1, 0], scalar_df, "ooo_pct", "Out-of-order", "%", "#55A868")
    _box(axes[1, 1], scalar_df, "dup_pct", "Duplicates", "%", "#C44E52")

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Figure saved to {output_path}")
    else:
        plt.show()

def _build_combo_palette(combos: list[str]) -> dict[str, tuple]:
    """
    Assign a visually distinct RGBA colour to each unique combo label.

    Strategy: interleave three large colormaps (tab20, tab20b, tab20c) which
    together give 60 distinct colours — more than enough for 54 tests.
    Falls back to the hsv colourmap for any overflow.

    Uses matplotlib.colormaps (the stable registry API, mpl >= 3.5).
    """
    import matplotlib
    colours: list[tuple] = []
    for name in ("tab20", "tab20b", "tab20c"):
        cmap = matplotlib.colormaps[name]
        for i in range(20):
            colours.append(cmap(i / 20.0))

    n = len(combos)
    if n > len(colours):
        extra = matplotlib.colormaps["hsv"]
        colours += [extra(i / n) for i in range(n - len(colours))]

    return {combo: colours[i] for i, combo in enumerate(combos)}


def plot_msg_per_time(
        analyser_dir: str,
        metadata: list[TestMetadata],
        bucket_secs: int = 1,
        output_path: Optional[str] = None,
        battery_start_ts: Optional[int] = None,
        battery_end_ts: Optional[int] = None,
        figsize: tuple = (28, 9),
) -> None:
    """
    For each test in *metadata*, read its analyser TSV, bin received messages
    into *bucket_secs*-second windows relative to start_ts, then plot all
    series on one axes.
    """
    if not metadata:
        print("plot_msg_per_time: no metadata entries.", file=sys.stderr)
        return

    bucket_ms = bucket_secs * 1000

    # Derive all unique combo labels in metadata order (preserves test order).
    seen: set[str] = set()
    ordered_combos: list[str] = []
    for m in metadata:
        lbl = f"pq{m.pub_qos}_sq{m.sub_qos}_d{m.delay_ms}_s{m.msg_size}"
        if lbl not in seen:
            seen.add(lbl)
            ordered_combos.append(lbl)

    palette = _build_combo_palette(ordered_combos)

    fig, ax = plt.subplots(figsize=figsize)

    main_title = (f"Messages received per {bucket_secs}s window  "
                  f"(tests 1–{len(metadata)})")
    if battery_start_ts is not None and battery_end_ts is not None:
        subtitle = (f"battery start: {_ts_ms_to_local_iso(battery_start_ts)}"
                    f"  –  end: {_ts_ms_to_local_iso(battery_end_ts)}")
        fig.suptitle(f"{main_title}\n{subtitle}",
                     fontsize=12, fontweight="bold", linespacing=1.6)
    else:
        fig.suptitle(main_title, fontsize=13, fontweight="bold")
    ax.set_xlabel(f"Time since test start (s), bucketed to {bucket_secs}s")
    ax.set_ylabel("Messages received")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Track which combos we have already added a legend entry for.
    legend_added: set[str] = set()

    for test_idx, m in enumerate(metadata, start=1):
        combo = f"pq{m.pub_qos}_sq{m.sub_qos}_d{m.delay_ms}_s{m.msg_size}"
        ana_path = Path(analyser_dir) / f"{combo}.tsv"

        if not ana_path.exists():
            print(f"  [warn] test {test_idx}: analyser file not found: {ana_path}", file=sys.stderr)
            continue

        # Read recv_timestamps in the test window.
        bucket_counts: dict[int, int] = defaultdict(int)
        try:
            with open(ana_path, newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                header = True
                for parts in reader:
                    if header:
                        header = False
                        continue
                    if len(parts) < 5:
                        continue
                    try:
                        pub_ts = int(parts[1])
                        recv_ts = int(parts[4])
                    except ValueError:
                        continue
                    if not (m.start_ts <= pub_ts <= m.end_ts):
                        continue
                    # Bucket index relative to test start (in seconds).
                    bucket = ((recv_ts - m.start_ts) // bucket_ms) * bucket_secs
                    bucket_counts[bucket] += 1
        except OSError as e:
            print(f"  [warn] test {test_idx}: cannot open {ana_path}: {e}", file=sys.stderr)
            continue

        if not bucket_counts:
            continue

        xs = sorted(bucket_counts)
        ys = [bucket_counts[x] for x in xs]
        colour = palette[combo]

        # Only add a legend entry the first time this combo appears.
        label = combo if combo not in legend_added else None
        legend_added.add(combo)

        ax.plot(xs, ys,
                marker="o", markersize=3, linewidth=1.4,
                color=colour, label=label, alpha=0.85)

    # Build a clean legend (one entry per combo, sorted).
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(
            handles, labels,
            title="Test combination",
            fontsize=7,
            title_fontsize=8,
            loc="upper right",
            ncol=max(1, len(handles) // 18),  # auto-columnise for many entries
            framealpha=0.7,
        )

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Figure saved to {output_path}")
    else:
        plt.show()


def _run_all(analyser_dir: str,
             publisher_dir: str,
             sys_dir: Optional[str] = None,
             bucket_secs: int = 10) -> None:
    """
    Discover every test combo from the analyser directory, compute stats,
    write a summary TSV, and produce both plots.

    Parameters
    ----------
    analyser_dir  : directory holding pq*_sq*_d*_s*.tsv analyser files.
    publisher_dir : directory holding pq*_d*_s*.tsv publisher files.
    sys_dir       : optional directory for SYS broker TSV files.
    bucket_secs   : bucket width for the msg-per-time plot.
    """
    all_stats = discover_tests_from_files(analyser_dir, publisher_dir)
    if not all_stats:
        print("No test files discovered – aborting.", file=sys.stderr)
        return

    print(f"Discovered {len(all_stats)} test combination(s).")

    # Attach sys_file paths if a sys directory was provided.
    if sys_dir:
        for s in all_stats:
            sys_path = (Path(sys_dir) /
                        f"sys_pq{s.pub_qos}_sq{s.sub_qos}_d{s.delay_ms}_s{s.msg_size}.tsv")
            s.sys_file = str(sys_path) if sys_path.exists() else ""

    summary_path = Path(analyser_dir) / "stats_summary.tsv"
    computed: list[TestStats] = []

    with open(summary_path, "w") as tsv_out:
        build_stats_tsv_header(tsv_out)

        for s in all_stats:
            if calc_test_stats(s) == 0:
                print_test_stats(s)
                if s.sys_file:
                    correlate_with_sys(s)
                build_stats_tsv_row(tsv_out, s)
                computed.append(s)
            else:
                print(f"  [warn] calc_test_stats failed for "
                      f"pq{s.pub_qos}_sq{s.sub_qos}_d{s.delay_ms}_s{s.msg_size}",
                      file=sys.stderr)

    print(f"\nSummary TSV written to {summary_path}")

    # Overall battery window (min start → max end across all computed tests).
    battery_start = min(s.start_ts for s in computed) if computed else None
    battery_end = max(s.end_ts for s in computed) if computed else None

    # Build TestMetadata list from computed stats for plot_msg_per_time.
    metadata = [
        TestMetadata(
            pub_qos=s.pub_qos, sub_qos=s.sub_qos,
            delay_ms=s.delay_ms, msg_size=s.msg_size,
            start_ts=s.start_ts, end_ts=s.end_ts,
        )
        for s in computed
    ]

    plot_box_whisker(
        computed,
        output_path=str(Path(analyser_dir) / "boxplot.png"),
        battery_start_ts=battery_start,
        battery_end_ts=battery_end,
    )
    plot_msg_per_time(
        analyser_dir,
        metadata,
        bucket_secs=bucket_secs,
        output_path=str(Path(analyser_dir) / "msg_per_time.png"),
        battery_start_ts=battery_start,
        battery_end_ts=battery_end,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="MQTT test statistics – discovers tests from file layout",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python stats.py out/analyser out/publisher
  python stats.py out/analyser out/publisher --sys-dir out/sys
  python stats.py out/analyser out/publisher --bucket-secs 5
""",
    )
    parser.add_argument(
        "analyser_dir",
        help="Directory containing pq*_sq*_d*_s*.tsv analyser files",
    )
    parser.add_argument(
        "publisher_dir",
        help="Directory containing pq*_d*_s*.tsv publisher files (no sub_qos)",
    )
    parser.add_argument(
        "--sys-dir", default=None,
        help="Optional directory containing SYS broker TSV files",
    )
    parser.add_argument(
        "--bucket-secs", type=int, default=10,
        help="Time bucket width for msg-per-time plot (default: 10)",
    )
    args = parser.parse_args()

    _run_all(
        analyser_dir=args.analyser_dir,
        publisher_dir=args.publisher_dir,
        sys_dir=args.sys_dir,
        bucket_secs=args.bucket_secs,
    )
