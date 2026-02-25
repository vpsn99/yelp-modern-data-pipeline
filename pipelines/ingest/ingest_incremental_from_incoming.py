import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl
from rich import print

INCOMING_DIR = Path("data/raw/incoming")
PROCESSED_DIR = Path("data/raw/processed")
STAGED_REVIEWS_DIR = Path("data/staged/reviews")
MANIFEST_PATH = Path("manifests/processed_files.json")

BATCH_SIZE = 250_000


@dataclass
class ManifestEntry:
    filename: str
    processed_at_utc: str
    record_count: int


def ensure_dirs() -> None:
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    STAGED_REVIEWS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {"processed_files": []}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def is_already_processed(manifest: dict, filename: str) -> bool:
    return any(x["filename"] == filename for x in manifest["processed_files"])


def write_partitioned_reviews(df: pl.DataFrame) -> None:
    # Parse date and derive review_year
    df = df.with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False).alias("review_ts")
    ).with_columns(
        pl.col("review_ts").dt.year().alias("review_year")
    )

    # Partition manually by year and write parquet parts
    for year, group in df.partition_by("review_year", as_dict=True).items():
        year_path = STAGED_REVIEWS_DIR / f"review_year={year}"
        year_path.mkdir(parents=True, exist_ok=True)

        file_name = f"part_{int(datetime.now().timestamp() * 1000)}.parquet"
        group.write_parquet(year_path / file_name, compression="zstd")


def ingest_one_jsonl_file(path: Path) -> int:
    total = 0
    batch: list[dict] = []

    print(f"[bold green]Ingesting {path.name}[/bold green]")

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            batch.append(json.loads(line))

            if len(batch) >= BATCH_SIZE:
                df = pl.DataFrame(batch)
                write_partitioned_reviews(df)
                total += len(batch)
                print(f"[yellow]{path.name}: processed {total:,}[/yellow]")
                batch = []

        if batch:
            df = pl.DataFrame(batch)
            write_partitioned_reviews(df)
            total += len(batch)

    print(f"[bold green]{path.name}: DONE total {total:,}[/bold green]")
    return total


def main() -> None:
    ensure_dirs()
    manifest = load_manifest()

    files = sorted(INCOMING_DIR.glob("reviews_*.json*"))
    if not files:
        print("[cyan]No incoming review files found.[/cyan]")
        return

    for fp in files:
        if is_already_processed(manifest, fp.name):
            print(f"[dim]Skipping already processed: {fp.name}[/dim]")
            continue

        count = ingest_one_jsonl_file(fp)

        # Move raw file to processed folder (audit trail)
        dest = PROCESSED_DIR / fp.name
        shutil.move(str(fp), str(dest))

        manifest["processed_files"].append(
            {
                "filename": fp.name,
                "processed_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "record_count": count,
            }
        )
        save_manifest(manifest)

    print("[bold green]Incremental ingestion complete.[/bold green]")


if __name__ == "__main__":
    main()