import pandas as pd
import glob

input_files = glob.glob("waqi-covid-data/*.csv")
output_dir = "datasets_landing_parquet/"

for csv_file in input_files:
    # e.g. `waqi-covid-2019Q1.csv`
    base = csv_file.split("/")[-1].replace(".csv", "")
    parquet_file = f"{output_dir}{base}.parquet"

    print(f"Converting {csv_file} → {parquet_file}")

    df = pd.read_csv(csv_file, skiprows=4)
    df.to_parquet(parquet_file, engine="pyarrow", index=False)