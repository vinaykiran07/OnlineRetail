import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split

# Step 1: Extract
def extract_data():
    url = "Online Retail.xlsx"
    df = pd.read_excel(url)
    print("Data extracted successfully.")
    return df

# Step 2: Transform
def transform_data(df):
    # Drop rows with missing CustomerID (they can't be tracked)
    df = df.dropna(subset=['CustomerID'])

    # Remove cancelled orders (InvoiceNo starting with 'C')
    df = df[~df['InvoiceNo'].astype(str).str.startswith('C')]

    # Remove rows with negative or zero Quantity or UnitPrice
    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]

    # Create a new column: TotalPrice
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    # Convert InvoiceDate to datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    print("🔧 Data transformed successfully.")
    return df

# Step 3: Load
def load_data(df, output_path="cleaned_online_retail.csv"):
    df.to_csv(output_path, index=False)
    print(f"📁 Data loaded to: {output_path}")

# Main ETL Pipeline
def run_pipeline():
    df_raw = extract_data()
    df_clean = transform_data(df_raw)
    load_data(df_clean)

if __name__ == "__main__":
    run_pipeline()
