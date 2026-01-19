#!/usr/bin/env bash

set -euo pipefail

BASE_URL="https://aqicn.org/data-platform/covid19/report/48852-12462758"
OUTDIR="waqi-covid-data"

mkdir -p "$OUTDIR"

echo "Downloading WAQI COVID-19 datasets into $OUTDIR..."

# Year / period list
PERIODS=(
  2015H1
  2016H1
  2017H1
  2018H1
  2019Q1 2019Q2 2019Q3 2019Q4
  2020Q1 2020Q2 2020Q3 2020Q4
  2021Q1 2021Q2 2021Q3 2021Q4
  2022Q1 2022Q2 2022Q3 2022Q4
  2023Q1 2023Q2 2023Q3 2023Q4
  2026
)

for period in "${PERIODS[@]}"; do
  outfile="$OUTDIR/waqi-covid-${period}.csv"
  url="$BASE_URL/$period"

  echo "  → $outfile"
  curl --compressed --fail -o "$outfile" "$url"
done

echo "Downloading station metadata..."

curl --compressed --fail \
  -o "$OUTDIR/airquality-covid19-cities.json" \
  "https://aqicn.org/data-platform/covid19/airquality-covid19-cities.json"

echo "All downloads completed successfully."