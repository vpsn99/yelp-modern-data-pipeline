import json
from pathlib import Path
from datetime import datetime
import polars as pl
from rich import print

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")

# Tunable: these files are smaller than reviews, but still use batching
BATCH_SIZE = 200_000


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_parquet_batch(df: pl.DataFrame, out_dir: Path, prefix: str) -> None:
    _ensure_dir(out_dir)
    ts = int(datetime.now().timestamp() * 1000)
    out_file = out_dir / f"{prefix}_part_{ts}.parquet"
    df.write_parquet(out_file, compression="zstd")


def stage_json_lines_to_parquet(
    input_file: Path,
    output_dir: Path,
    prefix: str,
    batch_size: int = BATCH_SIZE,
    cast_cols: dict | None = None,
) -> int:
    """
    Streams a line-delimited JSON file and writes Parquet in batches.
    Returns total records processed.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    _ensure_dir(output_dir)

    total = 0
    batch: list[dict] = []

    print(f"[bold green]Staging {input_file.name} → {output_dir}[/bold green]")

    with input_file.open("r", encoding="utf-8") as f:
        for line in f:
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                df = pl.DataFrame(batch)

                # Optional type casting
                if cast_cols:
                    for col, dtype in cast_cols.items():
                        if col in df.columns:
                            df = df.with_columns(pl.col(col).cast(dtype, strict=False))

                _write_parquet_batch(df, output_dir, prefix)
                total += len(batch)
                print(f"[yellow]{prefix}: processed {total:,}[/yellow]")
                batch = []

        if batch:
            df = pl.DataFrame(batch)
            if cast_cols:
                for col, dtype in cast_cols.items():
                    if col in df.columns:
                        df = df.with_columns(pl.col(col).cast(dtype, strict=False))
            _write_parquet_batch(df, output_dir, prefix)
            total += len(batch)

    print(f"[bold green]{prefix}: DONE total {total:,} records[/bold green]")
    return total


def main():
    # business
    stage_json_lines_to_parquet(
        input_file=RAW_DIR / "yelp_academic_dataset_business.json",
        output_dir=STAGED_DIR / "business",
        prefix="business",
        cast_cols={
            "stars": pl.Float64,
            "review_count": pl.Int64,
            "is_open": pl.Int64,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
        },
    )

    # users
    stage_json_lines_to_parquet(
        input_file=RAW_DIR / "yelp_academic_dataset_user.json",
        output_dir=STAGED_DIR / "users",
        prefix="users",
        cast_cols={
            "review_count": pl.Int64,
            "average_stars": pl.Float64,
            "fans": pl.Int64,
        },
    )

    # Optional: tip + checkin (uncomment if you want them now)
    # stage_json_lines_to_parquet(
    #     input_file=RAW_DIR / "yelp_academic_dataset_tip.json",
    #     output_dir=STAGED_DIR / "tips",
    #     prefix="tips",
    # )
    # stage_json_lines_to_parquet(
    #     input_file=RAW_DIR / "yelp_academic_dataset_checkin.json",
    #     output_dir=STAGED_DIR / "checkins",
    #     prefix="checkins",
    # )


if __name__ == "__main__":
    main()