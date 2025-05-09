"""
    python audit_profile.py --pattern "output_processed_json/*.json" --fast
"""
import argparse, glob, xxhash, json
from pathlib import Path
import polars as pl
from tqdm import tqdm
from ydata_profiling import ProfileReport
from simhash import Simhash

def load_concat(pattern: str) -> pl.DataFrame:
    dfs = [pl.read_ndjson(f) for f in tqdm(glob.glob(pattern))]
    return pl.concat(dfs)

def fast_duplicate(df: pl.DataFrame) -> dict:
    df = df.with_columns(
        pl.col("review_content")
          .str.strip_chars()
          .str.to_lowercase()
          .map_elements(lambda x: xxhash.xxh64(x).intdigest(),
                        return_dtype=pl.UInt64)     # exact-dup hash
          .alias("h64")
    )

    exact = int(df.select(pl.col("h64").is_duplicated().sum()).item())

    from simhash import Simhash
    df = df.with_columns(
        pl.col("review_content")
          .map_elements(lambda x: Simhash(x.split(), f=64).value,
                        return_dtype=pl.UInt64)     # simhash 64-bit
          .alias("sim")
    )
    near = int(df.select(pl.col("sim").is_duplicated().sum()).item() - exact)
    return {"exact_duplicate": exact, "near_duplicate": max(near, 0)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pattern", required=True)
    ap.add_argument("--out_dir", default="reports/data_audit")
    ap.add_argument("--fast", action="store_true")
    args = ap.parse_args()

    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    df = load_concat(args.pattern)

    basic = {"n_rows": len(df), "n_movies": df.select(pl.col("movie_id").n_unique()).item()}
    if args.fast:
        basic.update(fast_duplicate(df))
    else:
        raise NotImplementedError("Use --fast for big dataset")

    (out / "basic_stats.json").write_text(json.dumps(basic, indent=2))

    # ---- profile on sample
    sample = df.sample(n=min(50_000, len(df)), seed=42).to_pandas()
    ProfileReport(sample, title="Data profile (sample)", explorative=True,
                  minimal=True).to_file(out / "profile.html")

    print("✅ Done. See reports in", out.resolve())

if __name__ == "__main__":
    main()
