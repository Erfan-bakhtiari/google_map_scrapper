import asyncio
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import time
import csv
import os
from datetime import datetime
import threading

# Define your lists here
cities = [
    #     'Abadan',
    # 'Abashiqeh',
    # 'Ahar',
    # 'Ahvaz',
    # 'Arak',
    # 'Ardabil',
    # 'Ardebil',
    # 'Bandar Abbas',
    # 'Bandar Anzali',
    # 'Bardsir',
    # 'Birjand',
    # 'Bojnurd',
    # 'Borujerd',
    # 'Bushehr',
    # 'Damghan',
    # 'Darab',
    # 'Dezful',
    # 'Eslamshahr',
    # 'Esfahan',
    # 'Gachsaran',
    # 'Gorgan',
    # 'Gonbad-e Kavus',
    # 'Hamadan',
    'Ilam',
    # 'Isfahan',
    'Jalalabad',
    'Kerman',
    'Kermanshah',
    'Khorramabad',
    'Khorramshahr',
    'Khomein',
    'Khoramshahr',
    'Kish',
    'Kohgiluyeh',
    'Kordkuy',
    'Lar',
    'Mahabad',
    'Mamasani',
    'Maragheh',
    'Mashhad',
    'Masjed Soleyman',
    'Mohabad',
    'Qazvin',
    'Qom',
    'Rasht',
    'Sabzevar',
    'Sanandaj',
    'Sari',
    'Saveh',
    'Semnan',
    'Shahrekord',
    'Shiraz',
    'Sirjan',
    'Tabriz',
    'Tehran',
    'Torbat-e Heydarieh',
    'Urmia',
    'Yazd',
    'Zanjan',
    'Zahedan',
    'Abarkuh',
    'Abovay',
    'Adar',
    'Ahar',
    'Ahvaz',
    'Aleshtar',
    'Aliabad',
    'Aligudarz',
    'Amol',
    'Amiriyeh',
    'Andimeshk',
    'Anzali',
    'Ardakan',
    'Ardestan',
    'Ardabil',
    'Ashkezar',
    'Astara',
    'Ahar',
    'Baladeh',
    'Bam',
    'Bandar Lengeh',
    'Borujen',
    'Bukan',
    'Chabahar',
    'Dehloran',
    'Dezful',
    'Divandarreh',
    'Dorud',
    'Eslamshahr',
    'Fasa',
    'Fereydunshahr',
    'Firuzabad',
    'Gerash',
    'Ghasr-e Qand',
    'Golpayegan',
    'Gorgan',
    'Gorveh',
    'Hashtgerd',
    'Hormozgan',
    'Ilam',
    'Iranshahr',
    'Jahrom',
    'Javanrud',
    'Karaj',
    'Kashan',
    'Kazerun',
    'Khoy',
    'Khomeyni Shahr',
    'Khorramabad',
    'Kish Island',
    'Kohgiluyeh',
    'Kordkuy',
    'Kuhdasht',
    'Lar',
    'Mahabad',
    'Marand',
    'Maragheh',
    'Mashhad',
    'Maybod',
    'Mehriz',
    'Meybod',
    'Mohabad',
    'Najafabad',
    'Namak',
    'Neka',
    'Qazvin',
    'Qaem Shahr',
    'Qom',
    'Rafsanjan',
    'Rasht',
    'Sabzevar',
    'Sahneh',
    'Sabzevar',
    'Saqqez',
    'Sanandaj',
    'Sirjan',
    'Shahr-e Kord',
    'Shiraz',
    'Shahreza',
    'Somireh',
    'Tabriz',
    'Tehran',
    'Torbat-e Jam',
    'Urmia',
    'Yasuj',
    'Yasavi',
    'Yazd',
    'Zabol',
    'Zanjan',
    'Zahedan',
    'Zarand',
    'Zirab',
    # Add more cities as needed
]

keywords = [
    'Coffee',
    'قهوه',
    'Espresso',
    'اسپرسو',
    # 'Cappuccino',
    # 'کاپوچینو',
    # 'Latte',
    # 'لاته',
    # 'Americano',
    # 'آمریکانو',
    'Barista',
    'باریستا',
    'Beans',
    'دانه‌های قهوه',
    'Roasting',
    'برشته‌سازی',
    'Grinder',
    'آسیاب قهوه',
    'Brew',
    'دم کردن',
    'Caffeine',
    'کافئین',
    'Decaf',
    'بدون کافئین',
    # 'Macchiato',
    # 'ماکیاتو',
    # 'Frappuccino',
    # 'فراپچینو',
    # 'Mocha',
    # 'موکا',
    'Arabica',
    'عربیکا',
    'Robusta',
    'روبوستا',
    'Cafe',
    'کافه',
    'Coffee shop',
    'کافی‌شاپ',
    'Coffee maker',
    'دستگاه قهوه‌ساز',
    # 'French press',
    # 'پرس فرانسوی',
    'Instant coffee',
    'قهوه فوری',
    # 'Pour-over',
    # 'پور اور',
    'Espresso machine',
    'دستگاه اسپرسو',
    # 'Latte art',
    # 'هنر لاته',
    'Single-origin',
    # 'منشا واحد',
    'Blend',
    # 'مخلوط',
    # 'Organic coffee',
    # 'قهوه ارگانیک',
    'Coffee bean',
    'دانه قهوه',
    'Cupping',
    # 'تست قهوه',
    # 'Tamping',
    # 'فشرده‌سازی',
    # 'Crema',
    # 'کرما',
    # 'Extraction',
    # 'استخراج',
    # 'Syrup',
    # 'شربت',
    # 'Steam wand',
    # 'دسته بخار',
    # Add more keywords as needed
]

# CSV Configuration
OUTPUT_FILE = "scraped_addresses_async.csv"
CSV_HEADERS = ['City', 'Keyword', 'URL']

# Initialize a thread lock for CSV writing
csv_lock = threading.Lock()

# Ensure the output CSV has headers
def initialize_csv():
    if not os.path.isfile(OUTPUT_FILE):
        with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            writer.writeheader()
        print(f"[{datetime.now()}] Initialized CSV file with headers.")

# Set up Selenium WebDriver
def set_up_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Optional: To reduce detection
    chrome_options.add_argument('log-level=3')  # Suppress logs

    # Initialize the WebDriver (Ensure chromedriver is in PATH)
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# Construct the search URL
def construct_url(base_url, city, keyword):
    query = f"{city} {keyword}".replace(' ', '+')
    return f"{base_url}{query}"

# Scrape links for a single city-keyword pair with controlled scrolling and scraping
def scrape_links_controlled(base_url, city, keyword, scroll_duration=10, max_cycles=100):
    driver = set_up_driver()
    search_url = construct_url(base_url, city, keyword)
    print(f"[{datetime.now()}] Scraping URL: {search_url}")
    
    try:
        driver.get(search_url)
    except Exception as e:
        print(f"[{datetime.now()}] Failed to load URL {search_url}: {e}")
        driver.quit()
        return []
    
    time.sleep(5)  # Initial wait for the page to load. Adjust if necessary.
    
    links = set()
    cycles = 0
    scrollable_section_xpath = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]'  # May need adjustment

    try:
        scrollable_section = driver.find_element(By.XPATH, scrollable_section_xpath)
    except NoSuchElementException:
        print(f"[{datetime.now()}] Scrollable section not found for {city} - {keyword}.")
        driver.quit()
        return []
    
    # Start the controlled scroll and scrape cycles
    while cycles < max_cycles:
        cycles += 1
        print(f"[{datetime.now()}] Cycle {cycles}: Scrolling for {scroll_duration} seconds.")
        
        # Scroll for the specified duration
        start_time = time.time()
        while time.time() - start_time < scroll_duration:
            try:
                driver.execute_script("arguments[0].scrollBy(0, 300);", scrollable_section)
            except Exception as e:
                print(f"[{datetime.now()}] Error during scrolling: {e}")
                break
            time.sleep(1)  # Adjust scroll interval as needed
        print(f"[{datetime.now()}] Cycle {cycles}: Finished scrolling.")
        
        # Allow content to load
        time.sleep(3)
        
        # Scrape the currently loaded links
        new_links = set()
        result_links_xpath = './/a[contains(@href, "/maps/place/")]'
        try:
            elements = scrollable_section.find_elements(By.XPATH, result_links_xpath)
            for elem in elements:
                try:
                    href = elem.get_attribute('href')
                    if href and "/maps/place/" in href:
                        new_links.add(href)
                except StaleElementReferenceException:
                    continue  # Element is no longer attached to the DOM
        except Exception as e:
            print(f"[{datetime.now()}] Error extracting elements for {city} - {keyword}: {e}")
            break  # Exit the cycle on critical error
        
        # Determine newly discovered links
        newly_discovered = new_links - links
        if not newly_discovered:
            print(f"[{datetime.now()}] Cycle {cycles}: No new links found. Ending scraping for {city} - {keyword}.")
            break  # No new links found; terminate the scraping process
        
        links.update(new_links)
        print(f"[{datetime.now()}] Cycle {cycles}: Found {len(newly_discovered)} new links. Total links: {len(links)}.")
    
    driver.quit()
    return list(links)

# Save data to CSV in a thread-safe manner
def save_to_csv(data, headers):
    if not data:
        return
    with csv_lock:  # Ensure only one thread writes at a time
        with open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writerows(data)
    print(f"[{datetime.now()}] Saved {len(data)} records to CSV.")

# Asynchronous wrapper for scraping a single task
async def scrape_task(executor, base_url, city, keyword):
    loop = asyncio.get_event_loop()
    try:
        links = await loop.run_in_executor(executor, scrape_links_controlled, base_url, city, keyword)
    except Exception as e:
        print(f"[{datetime.now()}] Unhandled exception for {city} - {keyword}: {e}")
        links = []
    
    data_to_save = [{'City': city, 'Keyword': keyword, 'URL': link} for link in links]
    if data_to_save:
        save_to_csv(data_to_save, CSV_HEADERS)
        print(f"[{datetime.now()}] Saved {len(data_to_save)} records for {city} - {keyword}")
    else:
        print(f"[{datetime.now()}] No records found for {city} - {keyword}")

# Main asynchronous function
async def main_async():
    initialize_csv()
    base_url = "https://www.google.com/maps/search/"
    
    # Define the maximum number of concurrent threads
    max_workers = 5  # Adjust based on your system's capabilities
    
    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = []
        for city in cities:
            for keyword in keywords:
                task = scrape_task(executor, base_url, city, keyword)
                tasks.append(task)
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"[{datetime.now()}] Asynchronous scraping completed.")

# Entry point
if __name__ == "__main__":
    start_time = time.time()
    try:
        asyncio.run(main_async())
    except Exception as e:
        print(f"[{datetime.now()}] Fatal error: {e}")
    end_time = time.time()
    print(f"Total time taken: {end_time - start_time:.2f} seconds")