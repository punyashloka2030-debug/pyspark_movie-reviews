"""IMDb movie review analysis using PySpark.

This script is intentionally written as a small, beginner-friendly Spark
application.  All dataset processing is done with Spark DataFrame operations;
Pandas is not used.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = PROJECT_ROOT / "data" / "IMDB Dataset.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"


def build_spark_session() -> SparkSession:
    """Create a local SparkSession suitable for this assignment."""

    return (
        SparkSession.builder
        .appName("IMDb Movie Review Analysis")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def main() -> None:
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_FILE}\n"
            "Download the Kaggle dataset and place 'IMDB Dataset.csv' in data/."
        )

    # Creating this directory here makes the program work on a fresh checkout.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # The explicit schema keeps both input columns as strings.  multiLine
        # is important because a review may contain embedded line breaks.
        imdb_schema = StructType(
            [
                StructField("review", StringType(), nullable=True),
                StructField("sentiment", StringType(), nullable=True),
            ]
        )

        reviews_df = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .schema(imdb_schema)
            .csv(str(DATA_FILE))
        )

        print("\n=== Task 1: Dataset loaded ===")
        print(f"Input file: {DATA_FILE}")

        print("\n=== Task 2: Schema ===")
        reviews_df.printSchema()

        print("=== First 5 rows ===")
        reviews_df.show(5, truncate=100)

        # Keep a small sample output as well as printing it.  This makes the
        # first five loaded rows easy to inspect without opening the full CSV.
        sample_output = OUTPUT_DIR / "sample_reviews"
        (
            reviews_df.limit(5).coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(sample_output))
        )

        # Keep rows with a real review.  trim removes whitespace at both ends,
        # and length lets us reject empty or whitespace-only values.
        cleaned_df = reviews_df.filter(
            F.col("review").isNotNull()
            & (F.length(F.trim(F.col("review"))) > 0)
        )

        print("\n=== Task 4: Review counts ===")
        total_reviews = cleaned_df.count()
        positive_reviews = cleaned_df.filter(
            F.lower(F.trim(F.col("sentiment"))) == "positive"
        ).count()
        negative_reviews = cleaned_df.filter(
            F.lower(F.trim(F.col("sentiment"))) == "negative"
        ).count()
        print(f"Total cleaned reviews: {total_reviews}")
        print(f"Positive reviews: {positive_reviews}")
        print(f"Negative reviews: {negative_reviews}")

        # length() is a native PySpark SQL function, so the transformation is
        # performed by Spark rather than by a Python loop.
        transformed_df = cleaned_df.withColumn(
            "review_length", F.length(F.col("review"))
        )
        long_reviews_df = transformed_df.filter(F.col("review_length") > 500)

        print("\n=== Task 5: Reviews longer than 500 characters ===")
        long_reviews_df.select("review", "sentiment", "review_length").show(
            10, truncate=120
        )
        print(f"Reviews longer than 500 characters: {long_reviews_df.count()}")

        long_sample_output = OUTPUT_DIR / "long_reviews_sample"
        (
            long_reviews_df.limit(5).coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(long_sample_output))
        )

        # Spark writes CSV output as a directory containing part files.  A
        # single output partition keeps this small assignment easy to inspect,
        # while the processing above remains Spark DataFrame processing.
        cleaned_output = OUTPUT_DIR / "cleaned_reviews"
        long_output = OUTPUT_DIR / "long_reviews"
        (
            transformed_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(cleaned_output))
        )
        (
            long_reviews_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(long_output))
        )

        print("\n=== Task 6: Output saved ===")
        print(f"Cleaned and transformed CSV: {cleaned_output}")
        print(f"Long-review CSV: {long_output}")
        print(f"Five long-review samples CSV: {long_sample_output}")
        print(f"First 5 sample rows CSV: {sample_output}")
        print("Output CSV generation: SUCCESS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
