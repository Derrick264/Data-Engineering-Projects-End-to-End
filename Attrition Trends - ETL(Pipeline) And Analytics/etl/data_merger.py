import pandas as pd
import os
import random
from pathlib import Path
from faker import Faker
from datetime import timedelta, date
from etl.utils import save_with_backup

fake = Faker()

import uuid

def generate_fake_rows(n, real_df):
    """Generate n fake rows following the schema of merged_data."""
    job_titles = real_df["job_title"].dropna().unique().tolist()
    departments = real_df["department"].dropna().unique().tolist()
    locations = real_df["location"].dropna().unique().tolist()
    salary_bands = real_df["salary_band"].dropna().unique().tolist()
    genders = ["Male", "Female"]

    today = date.today()
    fake_rows = []

    for _ in range(n):
        # Always unique because UUID4
        review_id = f"reviews-{uuid.uuid4().hex}"

        company = "Nineleaps Technology Solutions"
        job_title = random.choice(job_titles)
        department = random.choice(departments)
        location = random.choice(locations)

        joining_date = fake.date_between(start_date="-10y", end_date="-1y")
        status = random.choices(["Active", "Exited"], weights=[0.6, 0.4])[0]
        if status == "Exited":
            min_exit_date = joining_date + timedelta(days=400)
            exit_date = (
                fake.date_between(start_date=min_exit_date, end_date=today)
                if min_exit_date < today
                else pd.NaT
            )
        else:
            exit_date = pd.NaT

        review_date = fake.date_between(start_date=joining_date, end_date="today")

        overall_rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 25, 35, 25])[0]
        engagement_score = round(random.uniform(4, 9), 1)
        performance_rating = random.choice([1, 2, 3, 4, 5])

        salary_band = (
            random.choice(salary_bands) if salary_bands else random.choice(["A", "B", "C"])
        )
        gender = random.choice(genders)
        age = random.randint(22, 55)

        employee_id = f"FAKE{random.randint(10000,99999)}"
        name = fake.name()

        pros = fake.sentence(nb_words=5)
        cons = fake.sentence(nb_words=5)

        fake_rows.append([
            review_id,
            company,
            job_title,
            department,
            location,
            review_date,
            overall_rating,
            pros,
            cons,
            employee_id,
            name,
            status,
            joining_date,
            exit_date,
            engagement_score,
            performance_rating,
            salary_band,
            gender,
            age,
        ])

    fake_df = pd.DataFrame(fake_rows, columns=real_df.columns)
    return fake_df



def merge_with_faker(fake_count=20):
    """Merge HRMS + Reviews, then add fake rows."""
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"
    backup_dir = project_root / "Backup" / "merged"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(backup_dir, exist_ok=True)

    # Load datasets
    hrms_path = data_dir / "hrms_latest.csv"
    reviews_path = data_dir / "nineleaps-technology-solutions_reviews.csv"
    enriched_path = data_dir / "reviews_enriched_latest.csv"

    df_hrms = pd.read_csv(hrms_path, parse_dates=["joining_date", "exit_date"])
    df_reviews = pd.read_csv(reviews_path, parse_dates=["ReviewDate"])

    # Load existing enriched reviews if present
    if enriched_path.exists():
        existing_enriched_df = pd.read_csv(enriched_path, parse_dates=["review_date"])
        already_merged_ids = set(existing_enriched_df['review_id'])
    else:
        existing_enriched_df = pd.DataFrame()
        already_merged_ids = set()

    # Filter only fresh (unmerged) reviews
    df_reviews = df_reviews[~df_reviews['ReviewID'].isin(already_merged_ids)]

    if df_reviews.empty:
        print("No new reviews to process.")
        return existing_enriched_df

    # Clean department fields
    df_hrms['department'] = df_hrms['department'].str.strip()
    df_reviews['Department'] = df_reviews['Department'].str.replace('Department', '', regex=False).str.strip()

    # Enrich fresh reviews
    enriched_reviews = []
    for _, review in df_reviews.iterrows():
        dept_loc_employees = df_hrms[
            (df_hrms['department'].str.lower() == review['Department'].lower()) &
            (df_hrms['location'].str.lower() == str(review['Location']).lower())
        ]
        if not dept_loc_employees.empty:
            mapped_emp = dept_loc_employees.sample(1).iloc[0]
        else:
            dept_employees = df_hrms[df_hrms['department'].str.lower() == review['Department'].lower()]
            mapped_emp = dept_employees.sample(1).iloc[0] if not dept_employees.empty else df_hrms.sample(1).iloc[0]

        enriched_reviews.append({
            "review_id": review['ReviewID'],
            "company": review['Company'],
            "job_title": review['JobTitle'],
            "department": review['Department'],
            "location": review['Location'],
            "review_date": review['ReviewDate'],
            "overall_rating": review['OverallRating'],
            "pros": review['Pros'],
            "cons": review['Cons'],
            "employee_id": mapped_emp['employee_id'],
            "name": mapped_emp['name'],
            "status": mapped_emp['status'],
            "joining_date": mapped_emp['joining_date'],
            "exit_date": mapped_emp['exit_date'],
            "engagement_score": mapped_emp['engagement_score'],
            "performance_rating": mapped_emp['performance_rating'],
            "salary_band": mapped_emp['salary_band'],
            "gender": mapped_emp['gender'],
            "age": mapped_emp['age']
        })

    new_enriched_df = pd.DataFrame(enriched_reviews)

    # Merge with old
    full_enriched_df = pd.concat([existing_enriched_df, new_enriched_df], ignore_index=True)

    # Add fake rows
    fake_df = generate_fake_rows(fake_count, full_enriched_df)
    final_df = pd.concat([full_enriched_df, fake_df], ignore_index=True)

    # Save with backup
    save_with_backup(final_df, enriched_path, backup_dir, prefix="reviews_enriched")

    return final_df


if __name__ == "__main__":
    df = merge_with_faker()
    print(df.tail(10))
