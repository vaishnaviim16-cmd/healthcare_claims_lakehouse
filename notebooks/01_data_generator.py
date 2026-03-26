from __future__ import annotations
import sys
import json
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from src.common.utils import ensure_dir, project_root

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)


def make_dirs() -> dict[str, Path]:
    root = project_root()
    base = root / "data" / "landing"

    dirs = {
        "claims_batch_001": ensure_dir(base / "claims" / "batch_001"),
        "claims_batch_002": ensure_dir(base / "claims" / "batch_002"),
        "members_batch_001": ensure_dir(base / "members" / "batch_001"),
        "members_batch_002": ensure_dir(base / "members" / "batch_002"),
        "providers_batch_001": ensure_dir(base / "providers" / "batch_001"),
        "providers_batch_002": ensure_dir(base / "providers" / "batch_002"),
        "payments_batch_001": ensure_dir(base / "payments" / "batch_001"),
        "payments_batch_002": ensure_dir(base / "payments" / "batch_002"),
    }
    return dirs


def generate_members(n: int = 200) -> pd.DataFrame:
    plans = ["PLN_BASIC", "PLN_SILVER", "PLN_GOLD"]
    genders = ["F", "M"]

    rows = []
    for i in range(1, n + 1):
        member_id = f"M{i:05d}"
        rows.append(
            {
                "member_id": member_id,
                "full_name": fake.name(),
                "gender": random.choice(genders),
                "dob": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
                "city": fake.city(),
                "state": fake.state(),
                "plan_id": random.choice(plans),
                "updated_at": datetime(2025, 12, 1, 10, 0, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Inject a few quality issues
    df.loc[5, "member_id"] = None
    df.loc[10, "plan_id"] = None

    return df


def generate_member_updates(base_members: pd.DataFrame) -> pd.DataFrame:
    updates = base_members.sample(10, random_state=42).copy()
    updates["city"] = updates["city"].apply(lambda _: fake.city())
    updates["plan_id"] = "PLN_GOLD"
    updates["updated_at"] = datetime(2025, 12, 15, 11, 0, 0).isoformat()

    new_rows = []
    for i in range(201, 211):
        new_rows.append(
            {
                "member_id": f"M{i:05d}",
                "full_name": fake.name(),
                "gender": random.choice(["F", "M"]),
                "dob": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
                "city": fake.city(),
                "state": fake.state(),
                "plan_id": random.choice(["PLN_BASIC", "PLN_SILVER", "PLN_GOLD"]),
                "updated_at": datetime(2025, 12, 15, 11, 0, 0).isoformat(),
            }
        )

    return pd.concat([updates, pd.DataFrame(new_rows)], ignore_index=True)


def generate_providers(n: int = 50) -> pd.DataFrame:
    specialties = ["Cardiology", "Orthopedics", "Dermatology", "General Medicine", "Neurology"]

    rows = []
    for i in range(1, n + 1):
        provider_id = f"P{i:04d}"
        rows.append(
            {
                "provider_id": provider_id,
                "provider_name": fake.company(),
                "specialty": random.choice(specialties),
                "network_flag": random.choice(["Y", "N"]),
                "address": fake.address().replace("\n", ", "),
                "city": fake.city(),
                "state": fake.state(),
                "updated_at": datetime(2025, 12, 1, 9, 0, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Inject issue
    df.loc[3, "specialty"] = None
    return df


def generate_provider_updates(base_providers: pd.DataFrame) -> pd.DataFrame:
    updates = base_providers.sample(8, random_state=99).copy()

    # simulate SCD2 style attribute changes later
    updates["specialty"] = updates["specialty"].fillna("General Medicine")
    updates["specialty"] = updates["specialty"].apply(lambda _: random.choice(["Cardiology", "Neurology"]))
    updates["network_flag"] = updates["network_flag"].map({"Y": "N", "N": "Y"})
    updates["address"] = updates["address"].apply(lambda _: fake.address().replace("\n", ", "))
    updates["updated_at"] = datetime(2025, 12, 20, 9, 30, 0).isoformat()

    return updates


def generate_claims(members: pd.DataFrame, providers: pd.DataFrame, n: int = 1000) -> pd.DataFrame:
    diagnosis_codes = ["D001", "D002", "D003", "D004", "D005"]
    procedure_codes = ["PROC100", "PROC200", "PROC300", "PROC400", "PROC500"]
    statuses = ["APPROVED", "DENIED", "PENDING"]

    valid_member_ids = [x for x in members["member_id"].dropna().tolist()]
    valid_provider_ids = [x for x in providers["provider_id"].dropna().tolist()]

    rows = []
    for i in range(1, n + 1):
        claim_id = f"C{i:06d}"
        service_date = fake.date_between(start_date="-180d", end_date="-30d")
        received_date = service_date + timedelta(days=random.randint(0, 10))
        updated_at = datetime(2025, 12, 2, 8, 0, 0) + timedelta(minutes=i)

        claim_amount = round(random.uniform(500, 25000), 2)
        approved_amount = round(claim_amount * random.uniform(0.4, 1.0), 2)

        rows.append(
            {
                "claim_id": claim_id,
                "claim_line_id": f"CL{i:07d}",
                "member_id": random.choice(valid_member_ids),
                "provider_id": random.choice(valid_provider_ids),
                "payer_id": random.choice(["PAY001", "PAY002", "PAY003"]),
                "diagnosis_code": random.choice(diagnosis_codes),
                "procedure_code": random.choice(procedure_codes),
                "claim_amount": claim_amount,
                "approved_amount": approved_amount,
                "claim_status": random.choice(statuses),
                "service_date": service_date.isoformat(),
                "received_date": received_date.isoformat(),
                "updated_at": updated_at.isoformat(),
                "source_delete_flag": "N",
            }
        )

    df = pd.DataFrame(rows)

    # Duplicate records
    duplicate_rows = df.sample(15, random_state=1).copy()
    df = pd.concat([df, duplicate_rows], ignore_index=True)

    # Inject invalid values
    df.loc[2, "member_id"] = None
    df.loc[4, "claim_amount"] = -500.00
    df.loc[6, "service_date"] = "2025-99-99"
    df.loc[8, "claim_status"] = "UNKNOWN"
    df.loc[9, "provider_id"] = None

    return df


def generate_claim_updates(base_claims: pd.DataFrame) -> pd.DataFrame:
    updates = base_claims.sample(30, random_state=7).copy()

    # Keep only unique claim_id rows for updates
    updates = updates.drop_duplicates(subset=["claim_id"]).copy()

    # Simulate changed amounts and status
    updates["claim_amount"] = updates["claim_amount"].abs() + 750
    updates["approved_amount"] = updates["claim_amount"] * 0.85
    updates["claim_status"] = random.choice(["APPROVED", "DENIED"])
    updates["updated_at"] = datetime(2025, 12, 18, 10, 0, 0).isoformat()

    # Simulate hard deletes
    delete_rows = updates.sample(5, random_state=11).copy()
    delete_rows["source_delete_flag"] = "Y"
    delete_rows["updated_at"] = datetime(2025, 12, 19, 10, 30, 0).isoformat()

    # Add new records with schema drift column
    new_rows = []
    start_index = 2001
    for i in range(start_index, start_index + 50):
        service_date = fake.date_between(start_date="-20d", end_date="today")
        received_date = service_date + timedelta(days=random.randint(0, 5))
        claim_amount = round(random.uniform(800, 18000), 2)

        new_rows.append(
            {
                "claim_id": f"C{i:06d}",
                "claim_line_id": f"CL{i:07d}",
                "member_id": f"M{random.randint(1, 210):05d}",
                "provider_id": f"P{random.randint(1, 50):04d}",
                "payer_id": random.choice(["PAY001", "PAY002", "PAY003"]),
                "diagnosis_code": random.choice(["D001", "D002", "D003", "D004", "D005"]),
                "procedure_code": random.choice(["PROC100", "PROC200", "PROC300", "PROC400", "PROC500"]),
                "claim_amount": claim_amount,
                "approved_amount": round(claim_amount * 0.80, 2),
                "claim_status": random.choice(["APPROVED", "DENIED", "PENDING"]),
                "service_date": service_date.isoformat(),
                "received_date": received_date.isoformat(),
                "updated_at": datetime(2025, 12, 18, 12, 0, 0).isoformat(),
                "source_delete_flag": "N",
                "submission_channel": random.choice(["PORTAL", "EMAIL", "API"]),  # schema drift
            }
        )

    updates["submission_channel"] = random.choice(["PORTAL", "EMAIL", "API"])
    delete_rows["submission_channel"] = random.choice(["PORTAL", "EMAIL", "API"])

    return pd.concat([updates, delete_rows, pd.DataFrame(new_rows)], ignore_index=True)


def generate_payments(claims_df: pd.DataFrame) -> pd.DataFrame:
    payment_rows = []
    approved_claims = claims_df[claims_df["claim_status"] == "APPROVED"].drop_duplicates(subset=["claim_id"]).head(400)

    for i, row in enumerate(approved_claims.itertuples(index=False), start=1):
        payment_rows.append(
            {
                "payment_id": f"PMT{i:06d}",
                "claim_id": row.claim_id,
                "paid_amount": round(float(row.approved_amount), 2),
                "payment_date": (datetime.fromisoformat(row.received_date) + timedelta(days=random.randint(2, 15))).date().isoformat(),
                "payment_status": random.choice(["PAID", "PARTIAL", "ON_HOLD"]),
            }
        )

    df = pd.DataFrame(payment_rows)

    # Inject one invalid value
    if not df.empty:
        df.loc[0, "paid_amount"] = -100.0

    return df


def generate_payment_updates(base_claim_updates: pd.DataFrame) -> pd.DataFrame:
    approved_new = base_claim_updates[base_claim_updates["claim_status"] == "APPROVED"].drop_duplicates(subset=["claim_id"]).head(40)

    rows = []
    for i, row in enumerate(approved_new.itertuples(index=False), start=5001):
        rows.append(
            {
                "payment_id": f"PMT{i:06d}",
                "claim_id": row.claim_id,
                "paid_amount": round(float(row.approved_amount), 2),
                "payment_date": (datetime.fromisoformat(row.received_date) + timedelta(days=random.randint(1, 10))).date().isoformat(),
                "payment_status": random.choice(["PAID", "PARTIAL"]),
            }
        )

    return pd.DataFrame(rows)


def write_json_lines(df: pd.DataFrame, output_file: Path):
    with open(output_file, "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            f.write(json.dumps(record, default=str) + "\n")


def main():
    dirs = make_dirs()

    print("Generating members...")
    members_df = generate_members()
    member_updates_df = generate_member_updates(members_df)

    print("Generating providers...")
    providers_df = generate_providers()
    provider_updates_df = generate_provider_updates(providers_df)

    print("Generating claims...")
    claims_df = generate_claims(members_df, providers_df)
    claim_updates_df = generate_claim_updates(claims_df)

    print("Generating payments...")
    payments_df = generate_payments(claims_df)
    payment_updates_df = generate_payment_updates(claim_updates_df)

    print("Writing batch_001 files...")
    claims_df.to_csv(dirs["claims_batch_001"] / "claims.csv", index=False)
    write_json_lines(members_df, dirs["members_batch_001"] / "members.json")
    providers_df.to_csv(dirs["providers_batch_001"] / "providers.csv", index=False)
    payments_df.to_parquet(dirs["payments_batch_001"] / "payments.parquet", index=False)

    print("Writing batch_002 files...")
    claim_updates_df.to_csv(dirs["claims_batch_002"] / "claims.csv", index=False)
    write_json_lines(member_updates_df, dirs["members_batch_002"] / "members.json")
    provider_updates_df.to_csv(dirs["providers_batch_002"] / "providers.csv", index=False)
    payment_updates_df.to_parquet(dirs["payments_batch_002"] / "payments.parquet", index=False)

    print("\nData generation complete.")
    
    print(f"claims batch_001 rows    : {len(claims_df)}")
    print(f"claims batch_002 rows    : {len(claim_updates_df)}")
    print(f"members batch_001 rows   : {len(members_df)}")
    print(f"members batch_002 rows   : {len(member_updates_df)}")
    print(f"providers batch_001 rows : {len(providers_df)}")
    print(f"providers batch_002 rows : {len(provider_updates_df)}")
    print(f"payments batch_001 rows  : {len(payments_df)}")
    print(f"payments batch_002 rows  : {len(payment_updates_df)}")


if __name__ == "__main__":
    main()