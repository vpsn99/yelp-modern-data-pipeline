import json
from pathlib import Path
import polars as pl
from datetime import datetime
from rich import print

RAW_PATH = Path("data/raw/yelp_academic_dataset_review.json")
STAGED_PATH = Path("data/staged/reviews")

BATCH_SIZE = 250_000


def ensure_dirs():
    STAGED_PATH.mkdir(parents=True, exist_ok=True)


def process_batch(records):
    df = pl.DataFrame(records)

    df = df.with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False)
    )

    df = df.with_columns(
        pl.col("date").dt.year().alias("review_year")
    )

    # Split by year manually
    for year, group in df.partition_by("review_year", as_dict=True).items():
        year_path = STAGED_PATH / f"review_year={year}"
        year_path.mkdir(parents=True, exist_ok=True)

        file_name = f"part_{datetime.now().timestamp()}.parquet"
        group.write_parquet(
            year_path / file_name,
            compression="zstd"
        )

def stage_reviews():
    ensure_dirs()

    batch = []
    total = 0

    print("[bold green]Starting ingestion...[/bold green]")

    with RAW_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            batch.append(json.loads(line))

            if len(batch) >= BATCH_SIZE:
                process_batch(batch)
                total += len(batch)
                print(f"[yellow]Processed {total:,} records[/yellow]")
                batch = []

        # Final leftover batch
        if batch:
            process_batch(batch)
            total += len(batch)

    print(f"[bold green]Finished. Total records processed: {total:,}[/bold green]")


if __name__ == "__main__":
    stage_reviews()