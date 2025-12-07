import sys
import os
import cloudscraper
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from io import StringIO

url = 'https://www.trendforce.com/price/dram/dram_spot'
csv = 'dram_prices.csv'


    
    
def new_get_dram_data():
    scraper = cloudscraper.create_scraper()

    try: 
        response = scraper.get(url)
        if response.status_code!= 200:
            print(f"Error: {response.status_code}")
            return None
        
        tables = pd.read_html(StringIO(response.text))

        if not tables:
            print("No tables")
            return None
        
        target_df = None
        for df in tables:
            if "Session Average" in df.columns:
                target_df = df
                break

        if target_df is None:
            print("Could not find the price table.")
            return None
        
        results = []
        for index, row in target_df.iterrows():
            if pd.isna(row.get("Item")):
                continue

            results.append({
                "Date": datetime.now().strftime("%Y-%m-%d"),
                "Item": row["Item"],
                "Session_Avg": row["Session Average"]
            })
        
        return results
    
    except Exception as e:
        print(f"Error occured: {e}")
        return None
    

def update_csv(new_data):
    if not new_data:
        print("N/A")
        return
    
    df_new = pd.DataFrame(new_data)

    if os.path.exists(csv) and os.path.getsize(csv) > 0:
        try:
            df_old = pd.read_csv(csv)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['Date', 'Item'], keep='last', inplace=True)
        except pd.errors.EmptyDataError:
            df_combined = df_new
    else:
        df_combined = df_new

    df_combined.to_csv(csv, index=False)
    print(f"susscessfully updated {csv}")
        
if __name__ == "__main__":
    data = new_get_dram_data()
    update_csv(data)