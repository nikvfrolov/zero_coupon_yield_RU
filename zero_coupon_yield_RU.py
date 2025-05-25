from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import time
import datetime
import pandas as pd
import os

# File to store results
OUTPUT_FILE = "all_zcyc_cbr_backup.tsv"
SAVE_EVERY_N_DAYS = 20  # How often to save intermediate results

# Configure headless Chrome
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 10)

# Target page and date range
base_url = "https://www.cbr.ru/hd_base/zcyc_params/zcyc/"
start_date = datetime.date(2005, 5, 24)
end_date = datetime.date.today()

# Resume from previous run if backup exists
if os.path.exists(OUTPUT_FILE):
    df_existing = pd.read_csv(OUTPUT_FILE, sep='\t')
    processed_dates = set(df_existing['date'].unique())
    print(f"Resuming... {len(processed_dates)} dates already processed")
else:
    df_existing = pd.DataFrame(columns=["date", "term_years", "yield_percent"])
    processed_dates = set()

# List to store new data
all_data = []
day_count = 0
current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime("%d.%m.%Y")
    iso_date = current_date.strftime("%Y-%m-%d")

    if iso_date in processed_dates:
        current_date += datetime.timedelta(days=1)
        continue

    print(f"→ {date_str}")

    try:
        # Try opening the site; restart browser if session is invalid
        try:
            driver.get(base_url)
        except (InvalidSessionIdException, WebDriverException):
            print("⟳ Restarting browser...")
            try:
                driver.quit()
            except:
                pass
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            wait = WebDriverWait(driver, 10)
            driver.get(base_url)

        time.sleep(1)

        # Locate and set date input field
        input_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='UniDbQuery.To']")))
        button_elem = driver.find_element(By.CSS_SELECTOR, ".datepicker-filter_button")
        driver.execute_script(f"arguments[0].value = '{date_str}';", input_elem)
        button_elem.click()

        # Wait for table to load
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.data")))
        time.sleep(0.5)

        # Extract table data
        table = driver.find_element(By.CSS_SELECTOR, "table.data")
        rows = table.find_elements(By.TAG_NAME, "tr")

        terms = [cell.text for cell in rows[0].find_elements(By.TAG_NAME, "th")][1:]
        values = [cell.text for cell in rows[1].find_elements(By.TAG_NAME, "td")[1:]]

        # Skip weekends and holidays
        if all(v in ("–", "-", "") for v in values):
            print("   ⤷ No data (weekend/holiday)")
        else:
            for term, yld in zip(terms, values):
                all_data.append({
                    "date": iso_date,
                    "term_years": float(term.replace(",", ".")),
                    "yield_percent": float(yld.replace(",", "."))
                })
            print(f"   ✔ Added: {len(terms)} points")

    except Exception as e:
        print(f"   ⚠ Skipped due to error: {e}")

    # Periodically save results
    day_count += 1
    if day_count % SAVE_EVERY_N_DAYS == 0 and all_data:
        df_part = pd.DataFrame(all_data)
        df_existing = pd.concat([df_existing, df_part]).drop_duplicates(subset=["date", "term_years"])
        df_existing.to_csv(OUTPUT_FILE, sep='\t', index=False)
        print(f"💾 Saved intermediate results to {OUTPUT_FILE}")
        all_data = []

    current_date += datetime.timedelta(days=1)

# Final save
driver.quit()
if all_data:
    df_part = pd.DataFrame(all_data)
    df_existing = pd.concat([df_existing, df_part]).drop_duplicates(subset=["date", "term_years"])
df_existing.to_csv(OUTPUT_FILE, sep='\t', index=False)
print(f"✅ Done. Full dataset saved to {OUTPUT_FILE}")
