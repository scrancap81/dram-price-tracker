import sys
import os
import cloudscraper
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import time 
from io import StringIO

# change name url 
# change file names
# update urls
# update code pull
# push git


url = {
    'nand_flash_spot': 'https://www.trendforce.com/price/flash/flash_spot',
    'nand_flash_contract': 'https://www.trendforce.com/price/flash/flash_contract',
    'wafer_spot': 'https://www.trendforce.com/price/flash/wafer_spot',
    'memory_card_spot': 'https://www.trendforce.com/price/flash/memCard_spot',
    'oem_ssd': 'https://www.trendforce.com/price/flash/pcc_oem_ssd_contract',
    'ssd_street': 'https://www.trendforce.com/price/flash/ssd_street',

    'dram_spot': 'https://www.trendforce.com/price/dram/dram_spot',
    'dram_contract': 'https://www.trendforce.com/price/dram/dram_contract',
    'dram_module': 'https://www.trendforce.com/price/dram/module_spot',
    'gddr_spot': 'https://www.trendforce.com/price/dram/gddr_spot',
}

csv = 'dram_prices.csv'
    
def get_nand_flash_spot_data():
    return scrape_trendforce_data(url['nand_flash_spot'], "NAND")

def get_nand_flash_contract_data():
    return scrape_trendforce_data(url['nand_flash_contract'], "NAND")

def get_wafer_spot_data():
    return scrape_trendforce_data(url['wafer_spot'], "NAND")

def get_memory_card_spot_data():
    return scrape_trendforce_data(url['memory_card_spot'], "NAND")

def get_oem_ssd_data():
    return scrape_trendforce_data(url['oem_ssd'], "NAND")

def get_ssd_street_data():
    return scrape_trendforce_data(url['ssd_street'], "NAND")

def get_dram_spot_data():
    return scrape_trendforce_data(url['dram_spot'], "DRAM")

def get_dram_contract_data():
    return scrape_trendforce_data(url['dram_contract'], 'DRAM')

def get_dram_module_data():
    return scrape_trendforce_data(url['dram_module'], 'DRAM')

def get_gddr_spot_data():
    return scrape_trendforce_data(url['gddr_spot'], "DRAM")

def scrape_trendforce_data(url, type_label):
    scraper = cloudscraper.create_scraper()

    try:
        response = scraper.get(url)
        if response.status_code != 200:
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
                "Type": type_label,
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

    scraping_jobs =[
        get_nand_flash_contract_data,
        get_nand_flash_spot_data,
        get_memory_card_spot_data,
        get_oem_ssd_data,
        get_wafer_spot_data,          
        get_dram_module_data,
        get_dram_spot_data,
        get_dram_contract_data,
        get_wafer_spot_data,
        get_ssd_street_data,
    ]

    for job in scraping_jobs:
        data = job()
        update_csv(data)
        time.sleep(2)


