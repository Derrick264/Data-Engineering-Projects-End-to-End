import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def parse_review_block(review_meta, soup):
    review_id = review_meta.get('id')
    if not review_id:
        return None
#add
    company = review_meta.select_one('meta[itemprop="name"]')['content'] if review_meta.select_one('meta[itemprop="name"]') else ""
    author_span = review_meta.find('span', itemprop="author")
    job_title = author_span.select_one('meta[itemprop="jobTitle"]')['content'] if author_span and author_span.select_one('meta[itemprop="jobTitle"]') else ""
    location = author_span.select_one('meta[itemprop="workLocation"]')['content'] if author_span and author_span.select_one('meta[itemprop="workLocation"]') else ""
    review_date = review_meta.select_one('meta[itemprop="datePublished"]')['content'] if review_meta.select_one('meta[itemprop="datePublished"]') else ""
    rating = review_meta.select_one('meta[itemprop="ratingValue"]')['content'] if review_meta.select_one('meta[itemprop="ratingValue"]') else ""

    pros, cons = "", ""
    review_body_span = review_meta.find('span', itemprop='reviewBody')
    if review_body_span:
        full_text = review_body_span.text.replace('\xa0', ' ').strip()
        if "Dislikes:" in full_text:
            parts = full_text.split("Dislikes:")
            pros = parts[0].replace("Likes:", "").strip()
            cons = parts[1].strip()
        else:
            pros = full_text.replace("Likes:", "").strip()

    department = ""
    visible_review_div = soup.find('div', id=review_id)
    if visible_review_div:
        info_container = visible_review_div.find('div', class_="flex mt-1")
        if info_container:
            p_tags = info_container.find_all('p')
            if p_tags:
                department_text = p_tags[-1].text.strip()
                if "Department" in department_text:
                    department = department_text

    return {
        "ReviewID": review_id,
        "Company": company,
        "JobTitle": job_title,
        "Department": department,
        "Location": location,
        "ReviewDate": review_date,
        "OverallRating": rating,
        "Pros": pros,
        "Cons": cons
    }

def scrape_reviews(company_slug, num_pages=3, delay=1, save_csv=True):
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"
    backup_dir = project_root / "Backup" / "reviews"
    meta_dir = data_dir  # Keep metadata next to CSV

    base_url = f"https://www.ambitionbox.com/reviews/{company_slug}-reviews"
    headers = {'User-Agent': 'Mozilla/5.0'}
    reviews_data = []

    # == Track last scraped page number ==
    latest_path = data_dir / f"{company_slug}_reviews.csv"
    meta_path = meta_dir / f"{company_slug}_last_page.txt"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(backup_dir, exist_ok=True)

    # ==== Load last scraped page index ====
    last_scraped_page = 0
    if meta_path.exists():
        try:
            with open(meta_path, 'r') as f:
                last_scraped_page = int(f.read().strip())
        except Exception:
            last_scraped_page = 0

    print(f" Last scraped page: {last_scraped_page}")
    start_page = last_scraped_page + 1
    end_page = start_page + num_pages - 1

    for page in range(start_page, end_page + 1):
        page_url = f"{base_url}?page={page}"
        print(f"→ Scraping page {page}: {page_url}")
        try:
            res = requests.get(page_url, headers=headers, verify=False)
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_url}: {e}")
            break

        soup = BeautifulSoup(res.content, "html.parser")
        review_meta_tags = soup.find_all("span", attrs={"itemscope": True, "itemtype": "https://schema.org/Review"})
        if not review_meta_tags:
            print(f"No reviews found on page {page}. Possibly last page. Stopping.")
            break

        for review_meta in review_meta_tags:
            parsed_data = parse_review_block(review_meta, soup)
            if parsed_data:
                reviews_data.append(parsed_data)

        # Update last scraped page number
        with open(meta_path, 'w') as f:
            f.write(str(page))

        time.sleep(delay)

    new_df = pd.DataFrame(reviews_data)

    if save_csv and not new_df.empty:
        # === Load existing data and append ===
        if latest_path.exists():
            existing_df = pd.read_csv(latest_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.drop_duplicates(subset="ReviewID", inplace=True)
        else:
            combined_df = new_df

        # Save updated full version
        combined_df.to_csv(latest_path, index=False)
        print(f"✅ Main file updated: {latest_path} (total {len(combined_df)} reviews)")

        # Save versioned backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{company_slug}_reviews_{timestamp}.csv"
        combined_df.to_csv(backup_path, index=False)
        print(f" Backup saved to: {backup_path}")

    return new_df

# For standalone testing
if __name__ == "__main__":
    df = scrape_reviews("nineleaps-technology-solutions", num_pages=1)
    print(df.head())
