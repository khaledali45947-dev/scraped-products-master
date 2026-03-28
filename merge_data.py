"""
Merge CSV files from all 5 active scraping accounts into 3 final CSVs.
Run by GitHub Actions merge workflow after all accounts push their data.

Amazon merges BOTH:
  - data/account*/amazon_products.csv       (category groups scraper)
  - data/account*/amazon_products_nodes.csv  (browse nodes scraper)
and deduplicates by product_id/asin.

Note: account3 (suspended) is excluded from merging.
"""
import csv
import glob
import os
from pathlib import Path

# account3 was suspended — skip its stale data
EXCLUDED_ACCOUNTS = {"account3"}
# GitHub has a 100 MB file size limit — split if larger than 90 MB
MAX_FILE_SIZE_MB = 90


def merge_platform(platform: str) -> int:
    """Merge all account CSVs for a given platform into one final CSV."""
    all_rows = []
    fieldnames = []
    seen_ids = set()

    # For amazon, also include nodes + search CSVs
    if platform == "amazon":
        patterns = [
            f"data/account*/{platform}_products.csv",
            f"data/account*/{platform}_products_nodes.csv",
            f"data/account*/{platform}_products_search.csv",
        ]
    else:
        patterns = [f"data/account*/{platform}_products.csv"]

    files = []
    for pattern in patterns:
        for f in sorted(glob.glob(pattern)):
            account = Path(f).parent.name
            if account in EXCLUDED_ACCOUNTS:
                print(f"  [{platform}] Skipping {account} (excluded)")
                continue
            files.append(f)

    if not files:
        print(f"[{platform}] No files found")
        return 0

    for fpath in files:
        account = Path(fpath).parent.name
        fname = Path(fpath).name
        try:
            with open(fpath, encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not fieldnames and reader.fieldnames:
                    fieldnames = list(reader.fieldnames)
                    # Ensure we have a unified ID column for dedup
                rows = list(reader)

                new_count = 0
                for row in rows:
                    # Deduplicate by product_id or asin
                    pid = row.get("product_id") or row.get("asin") or ""
                    if pid and pid in seen_ids:
                        continue
                    if pid:
                        seen_ids.add(pid)
                    all_rows.append(row)
                    new_count += 1

                print(f"  [{platform}] {account}/{fname}: {len(rows)} total, {new_count} new (deduped)")
        except Exception as e:
            print(f"  [{platform}] ERROR reading {fpath}: {e}")

    if not all_rows:
        print(f"[{platform}] No data to merge.")
        return 0

    # Make sure fieldnames include all columns from both CSVs
    all_keys = set()
    for row in all_rows:
        all_keys.update(row.keys())
    # Keep original order, append any new columns at end
    for key in sorted(all_keys):
        if key not in fieldnames:
            fieldnames.append(key)

    out_path = f"{platform}_products.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    # Check file size — split if over limit to avoid GitHub's 100 MB cap
    file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
    if file_size_mb > MAX_FILE_SIZE_MB:
        print(f"[{platform}] File is {file_size_mb:.1f} MB (>{MAX_FILE_SIZE_MB} MB) — splitting …")
        half = len(all_rows) // 2
        for part_idx, (start, end) in enumerate([(0, half), (half, len(all_rows))], 1):
            part_path = f"{platform}_products_part{part_idx}.csv"
            with open(part_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(all_rows[start:end])
            part_size = os.path.getsize(part_path) / (1024 * 1024)
            print(f"  [{platform}] Part {part_idx}: {end - start} rows ({part_size:.1f} MB) → {part_path}")
        # Remove the oversized single file
        os.remove(out_path)
        print(f"  [{platform}] Removed oversized {out_path}, using split files instead")
    else:
        print(f"[{platform}] Merged {len(all_rows)} unique products -> {out_path} ({file_size_mb:.1f} MB)")

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
