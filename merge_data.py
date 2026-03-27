"""
Merge CSV files from all 4 scraping accounts into 3 final CSVs.
Run by GitHub Actions merge workflow after all accounts push their data.
"""
import csv
import glob
import os
from pathlib import Path


def merge_platform(platform: str) -> int:
    """Merge all account CSVs for a given platform into one final CSV."""
    all_rows = []
    fieldnames = []

    pattern = f"data/account*/{platform}_products.csv"
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"[{platform}] No files found matching: {pattern}")
        return 0

    for fpath in files:
        account = Path(fpath).parent.name
        try:
            with open(fpath, encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not fieldnames and reader.fieldnames:
                    fieldnames = reader.fieldnames
                rows = list(reader)
                all_rows.extend(rows)
                print(f"  [{platform}] {account}: {len(rows)} products from {fpath}")
        except Exception as e:
            print(f"  [{platform}] ERROR reading {fpath}: {e}")

    if not all_rows:
        print(f"[{platform}] No data to merge.")
        return 0

    out_path = f"{platform}_products.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[{platform}] Merged {len(all_rows)} total products -> {out_path}")
    return len(all_rows)


if __name__ == "__main__":
    print("=== Merging scraping data from all accounts ===\n")
    totals = {}
    for platform in ["amazon", "noon", "jumia"]:
        totals[platform] = merge_platform(platform)
        print()

    print("=== Summary ===")
    for platform, count in totals.items():
        print(f"  {platform}: {count} products")
    print(f"  GRAND TOTAL: {sum(totals.values())} products")
