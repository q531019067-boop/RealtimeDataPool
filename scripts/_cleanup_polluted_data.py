"""Clean up polluted data from the 5h live run (2026-07-02 10:37-15:30).

Strategy: delete snapshots fetched between 10:37 and 15:30 today.
This is the 640k rows of "ok" but mostly throttled/slow data with
44% missing orderbook — better to start fresh.

Usage:  python scripts/_cleanup_polluted_data.py [--dry-run]
"""
import argparse
import sqlite3
import time
from pathlib import Path

DB = Path(r"C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool\data\rdp.db")

POLLUTED_START = time.mktime(time.strptime("2026-07-02 10:37:43", "%Y-%m-%d %H:%M:%S"))
POLLUTED_END = time.mktime(time.strptime("2026-07-02 15:30:00", "%Y-%m-%d %H:%M:%S"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="只统计不删")
    parser.add_argument("--yes", action="store_true", help="跳过确认")
    args = parser.parse_args()

    con = sqlite3.connect(str(DB))
    cur = con.cursor()

    # 1. 统计污染行数
    cur.execute(
        "SELECT COUNT(*) FROM snapshots WHERE fetched_at >= ? AND fetched_at <= ?",
        (POLLUTED_START, POLLUTED_END),
    )
    polluted_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM snapshots")
    total_count = cur.fetchone()[0]

    # 用文件大小代替 dbstat
    db_bytes = DB.stat().st_size

    print(f"=== 污染数据统计 ===")
    print(f"  时间窗:     {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(POLLUTED_START))}")
    print(f"          到 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(POLLUTED_END))}")
    print(f"  受影响行数: {polluted_count:,}  ({polluted_count/total_count*100:.1f}% of total)")
    print(f"  当前总行数: {total_count:,}")
    print(f"  DB 大小:    {db_bytes/1024/1024:.0f} MB")

    if polluted_count == 0:
        print("\n  无需清理")
        return

    if args.dry_run:
        print("\n  [DRY-RUN] 未实际删除")
        return

    if not args.yes:
        print()
        confirm = input(f"  确认删除 {polluted_count:,} 行? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("  取消")
            return

    print(f"\n  开始删除 {polluted_count:,} 行...")
    t0 = time.time()
    cur.execute(
        "DELETE FROM snapshots WHERE fetched_at >= ? AND fetched_at <= ?",
        (POLLUTED_START, POLLUTED_END),
    )
    con.commit()
    elapsed = time.time() - t0
    deleted = cur.rowcount

    # 重建索引 (DELETE 不会自动 shrink)
    print("  VACUUM (回收空间,可能慢)...")
    cur.execute("VACUUM")

    cur.execute("SELECT COUNT(*) FROM snapshots")
    new_total = cur.fetchone()[0]
    new_bytes = DB.stat().st_size

    print(f"\n=== 完成 ===")
    print(f"  删除耗时:    {elapsed:.1f}s")
    print(f"  删除行数:    {deleted:,}")
    print(f"  剩余行数:    {new_total:,}")
    print(f"  DB 大小:     {db_bytes/1024/1024:.0f} MB → {new_bytes/1024/1024:.0f} MB  (省 {(db_bytes-new_bytes)/1024/1024:.0f} MB)")

    con.close()


if __name__ == "__main__":
    main()
