
import os
import sys
import pandas as pd
import gzip
import re
sys.path.append('/home/git_sync/shared')
import internal_tools
# Define S3 and Redshift parameters
s3_secret_name = "bauerS3_master_creds"
bucket_name = "bauer-caspian"
prefix = "apple-news-revenue"
file_name = "apple_plus_revenue"
file_format = "csv"
redshift_db_schema = "transactions"
table_name = "apple_plus_revenue"
redshift_db_secret_name = "redshift_bmg-live-pub-cluster"
EXPECTED_COLUMNS = [
    "currency", "storefront_name", "channel_id", "channel_name", "share_amount", "file", "exchange"
]
def process_files():
    """Extracts and processes files from the current directory."""
    files = [f for f in os.listdir() if "txt.gz" in f]
    if not files:
        print("No 'txt.gz' files found in the directory!")
        return pd.DataFrame(columns=EXPECTED_COLUMNS)  
    data_full = pd.DataFrame()
    # Loop through files
    for i, file in enumerate(files):
        if "N2_" in file:
            print(f"Processing File: {i+1} of {len(files)} - {file}")
            try:
                with gzip.open(file, 'rt') as f:
                    file_lines = f.readlines()
                headers = file_lines[4].strip().split("\t")
                data_lines = [line.strip().split("\t") for line in file_lines[5:] if "Total Amount" not in line]
                df = pd.DataFrame(data_lines, columns=headers)
                df.columns = df.columns.str.lower().str.replace(" ", "_")  # Normalize column names
                df["file"] = file  
                df = df[EXPECTED_COLUMNS[:-1]]  
                data_full = pd.concat([data_full, df], ignore_index=True)
            except Exception as e:
                print(f"Error processing {file}: {e}")
                continue
    print("Final Data Columns Before Exchange Merge:", data_full.columns)
    print("Number of Records Processed:", len(data_full))
    return data_full
def fetch_currency_exchange(data_full):
    """Fetches currency exchange rates, ensuring only valid currencies are processed."""
    if 'currency' not in data_full.columns:
        raise KeyError("Missing 'currency' column in data_full DataFrame")
    currencies = data_full['currency'].unique()
    currency_frame = pd.DataFrame()
    
    currency_pattern = re.compile(r"^[A-Z]{3}$")
    valid_currencies = [c for c in currencies if currency_pattern.match(str(c))]
    invalid_currencies = [c for c in currencies if not currency_pattern.match(str(c))]
    
    if invalid_currencies:
        print(f"Ignoring invalid currency values: {invalid_currencies}")
    for i, currency in enumerate(valid_currencies):
        print(f"Fetching exchange rate for currency {i+1} of {len(valid_currencies)}: {currency}")
        try:
            exchange = internal_tools.fetch_exchange_rate(currency, "GBP", "latest")
            currency_frame = pd.concat(
                [currency_frame, pd.DataFrame({'currency': [currency], 'exchange': [exchange]})],
                ignore_index=True
            )
        except Exception as e:
            print(f"Error fetching exchange rate for {currency}: {e}")
            continue
    print("Final Currency DataFrame:", currency_frame)
    return currency_frame
def main():
    """Main function to process files, fetch exchange rates, and load data into S3 and Redshift."""
    data_full = process_files()
    if data_full.empty:
        print("No valid data to process. Exiting.")
        return
    currency_frame = fetch_currency_exchange(data_full)
    # Merge exchange rates into data_full
    data_full = data_full.merge(currency_frame, on='currency', how='left')
    print("Final Data Columns After Exchange Merge:", data_full.columns)
    # Save to S3
    internal_tools.load_df_to_s3(
        df=data_full, 
        s3_secret_name=s3_secret_name,
        bucket_name=bucket_name,
        prefix=prefix,
        file_key_name=f"{file_name}.{file_format}",
        delimiter="|"
    )
    print("Data successfully written to S3")
    # Load data into Redshift
    internal_tools.load_from_s3_to_rs(
        s3_secret_name=s3_secret_name,
        bucket_name=bucket_name,
        prefix=prefix,
        file_name=file_name,
        file_format=file_format,
        redshift_db_schema=redshift_db_schema,
        redshift_db_secret_name=redshift_db_secret_name,
        delimiter="|",
        truncate_table=False
    )
if __name__ == "__main__":
    main()
