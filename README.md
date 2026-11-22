# Homework 1.2 – PySpark Movie Review Dataset Analysis

This project analyzes the IMDb Movie Review Dataset (50,000 reviews) with
PySpark DataFrame operations.

## Project structure

```text
pyspark-movie-reviews/
├── data/                  # Place the downloaded CSV here (not committed)
├── output/                # Generated Spark CSV directories (not committed)
├── src/
│   └── main.py
├── requirements.txt
├── spark_env.sh
├── .gitignore
└── README.md
```

## Requirements

- Linux
- Python 3.11 or newer
- Java 17, 21, or 25 for Spark 4.3

The setup used for this project is Python 3.14 and a user-local Java 17
installation.  No system-wide Python or Java configuration is required.

## Setup

From this project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If Java 17 is installed in the user-local location used by this project,
load the complete environment with:

```bash
source ./spark_env.sh
```

## Download the dataset

Download the public Kaggle dataset from:

<https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews>

The Kaggle API command below downloads version 1 without requiring the
original ZIP or CSV to be committed to Git:

```bash
curl -fL \
  'https://www.kaggle.com/api/v1/datasets/download/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews?datasetVersionNumber=1' \
  -o /tmp/imdb-movie-reviews.zip
unzip -o /tmp/imdb-movie-reviews.zip -d data
```

The extracted file must be named `data/IMDB Dataset.csv`.

## Run the analysis

```bash
source ./spark_env.sh
python src/main.py
```

The program prints the schema, five sample rows, cleaned/positive/negative
counts, examples of reviews longer than 500 characters, and output locations.
It creates:

- `output/cleaned_reviews/`: cleaned data with `review_length`
- `output/long_reviews/`: only reviews whose length is greater than 500
- `output/long_reviews_sample/`: five examples from the long-review results
- `output/sample_reviews/`: exactly the first five loaded rows

Spark writes CSV data as directories containing a header and a `part-*.csv`
file.  Both output directories are ignored by Git.
