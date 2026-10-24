"""
Generates fake, messy customer + order data so the pipeline has something real to clean.

The "mess" is on purpose:
  - the same person shows up multiple times (signed up on web AND mobile)
  - emails have random capital letters / extra spaces
  - phone numbers are in different formats
Run:  python data/generate_data.py
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent

FIRST = ["Ava", "Liam", "Mia", "Noah", "Zoe", "Ethan", "Priya", "Arjun", "Sara", "Leo"]
LAST = ["Patel", "Kim", "Garcia", "Smith", "Nguyen", "Reddy", "Brown", "Lee"]
SOURCES = ["web", "mobile", "store"]


def messy_email(email):
    """Randomly mess up an email: upper-case letters and stray spaces."""
    if random.random() < 0.4:
        email = email.upper() if random.random() < 0.5 else email.capitalize()
    if random.random() < 0.3:
        email = f"  {email} "
    return email


def messy_phone(digits):
    """Same phone number, different formats."""
    fmt = random.choice([
        "{}{}{}",            # 2175550123
        "({}) {}-{}",        # (217) 555-0123
        "{}-{}-{}",          # 217-555-0123
        "+1 {} {} {}",       # +1 217 555 0123
    ])
    return fmt.format(digits[:3], digits[3:6], digits[6:])


def main(n_people=200):
    customer_rows, order_rows = [], []
    record_id, order_id = 1, 1

    for p in range(n_people):
        first, last = random.choice(FIRST), random.choice(LAST)
        email = f"{first}.{last}{p}@example.com".lower()
        phone = f"217555{p:04d}"

        # each real person has 1-3 records (the duplicates we must merge)
        my_records = []
        for _ in range(random.choice([1, 1, 2, 3])):
            customer_rows.append({
                "record_id": record_id,
                "first_name": first,
                "last_name": last,
                "email": messy_email(email),
                "phone": messy_phone(phone),
                "source": random.choice(SOURCES),
                "signup_date": str(date(2024, 1, 1) + timedelta(days=random.randint(0, 600))),
            })
            my_records.append(record_id)
            record_id += 1

        # orders are attached to *any* of the person's records
        for _ in range(random.randint(0, 8)):
            order_rows.append({
                "order_id": order_id,
                "record_id": random.choice(my_records),
                "amount": round(random.uniform(5, 300), 2),
                "order_date": str(date(2025, 1, 1) + timedelta(days=random.randint(0, 600))),
            })
            order_id += 1

    for name, rows in [("customers.csv", customer_rows), ("orders.csv", order_rows)]:
        with open(OUT / name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {len(rows)} rows -> data/{name}")


if __name__ == "__main__":
    main()
