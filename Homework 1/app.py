import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import time

def filter_codes(codes):
    return [
        code for code in codes
        if not any(char.isdigit() for char in code) and not code.startswith(('E'))
    ]

# FUNCTION TO GET CODES
def fetch_codes():
    url = "https://www.mse.mk/en/stats/symbolhistory/ADIN"
    with requests.Session() as session:
        response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    dropdown = soup.find("select", id="Code")
    if not dropdown:
        return []
    codes = [option.text.strip() for option in dropdown.find_all("option")]
    return filter_codes(codes)

# FUNCTION TO GET LAST UPDATE DATE
def fetch_last_update_date(code):
    path = f"{code}.csv"
    try:
        df = pd.read_csv(path)
        return pd.to_datetime(df['Date']).max()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return None

# FUNCTION TO FETCH DATA FOR A CODE
def fetch_data_for_code(session, code, start_date, end_date):
    url = (
        f"https://www.mse.mk/en/stats/symbolhistory/{code}"
        f"?FromDate={start_date.strftime('%m/%d/%Y')}"
        f"&ToDate={end_date.strftime('%m/%d/%Y')}"
    )
    response = session.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    tbody = soup.select_one('tbody')
    if not tbody:
        return []
    return [[cell.get_text(strip=True) for cell in row.find_all('td')] for row in tbody.find_all('tr')]

# FUNCTION TO UPDATE DATA FOR A CODE
def update_data_for_code(session, code):
    current_date = datetime.now()
    last_update = fetch_last_update_date(code)
    all_data = []

    if last_update:
        start_date = last_update + timedelta(days=1)
    else:
        start_date = current_date - timedelta(days=3650)

    # Fetch data for each year concurrently
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        while start_date <= current_date:
            year_end = datetime(start_date.year, 12, 31)
            end_date = min(year_end, current_date)
            futures.append(executor.submit(fetch_data_for_code, session, code, start_date, end_date))
            start_date = end_date + timedelta(days=1)

        for future in as_completed(futures):
            all_data.extend(future.result())

    if all_data:
        save_data_to_csv(code, all_data)

# FUNCTION TO SAVE DATA TO CSV
def save_data_to_csv(code, data):
    columns = ['Date', 'LastTradePrice', 'Max', 'Min', 'Avg. Price', '%chg.', 'Volume', 'Turnover in BEST', 'TotalTurnover']
    df = pd.DataFrame(data, columns=columns)

    file_path = f"{code}.csv"
    df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)

# MAIN EXECUTION
if __name__ == "__main__":
    start_time = time.time()

    codes = fetch_codes()
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(update_data_for_code, session, code): code for code in codes}
            for future in as_completed(futures):
                code = futures[future]
                try:
                    future.result()
                    print(f"{code} is successfully scraped.")
                except Exception as e:
                    print(f"Error updating {code}: {e}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")  # Print the elapsed time
