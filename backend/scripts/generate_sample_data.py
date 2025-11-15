#!/usr/bin/env python3
"""
Sample Data Generator - Creates FAKE motor insurance data for testing

What this script does:
  1. Creates realistic motor insurance policy records (JSON format)
  2. Generates both VALID and INVALID records to test validators
  3. Saves to data/input/events/motor_policy/

What this script does NOT do:
  - Does NOT run the pipeline
  - Does NOT need PySpark/Spark
  - Does NOT process data

It ONLY creates test data files!
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_plate_number():
    """Generate random plate number"""
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
    numbers = ''.join(random.choices('0123456789', k=3))
    return f"{letters}-{numbers}"


def generate_policy_number():
    """Generate random policy number"""
    return f"POL{random.randint(100000, 999999)}"


def generate_valid_record(index):
    """Generate a valid motor policy record"""
    return {
        "policy_number": generate_policy_number(),
        "driver_age": random.randint(18, 75),
        "plate_number": generate_plate_number(),
        "premium": round(random.uniform(800, 2500), 2),
        "vehicle_type": random.choice(["sedan", "suv", "truck", "motorcycle"]),
        "coverage_type": random.choice(["comprehensive", "third_party", "collision"]),
        "policy_start_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "policy_end_date": (datetime.now() + timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
        "driver_name": f"Driver_{index}",
        "driver_license": f"DL{random.randint(1000000, 9999999)}"
    }


def generate_invalid_record(index, error_type):
    """Generate an invalid motor policy record"""
    record = generate_valid_record(index)

    if error_type == "null_age":
        record["driver_age"] = None
    elif error_type == "empty_plate":
        record["plate_number"] = ""
    elif error_type == "null_plate":
        record["plate_number"] = None
    elif error_type == "invalid_age":
        record["driver_age"] = random.choice([15, 105, -5])
    elif error_type == "empty_policy":
        record["policy_number"] = ""

    return record


def generate_sample_data(output_dir, num_valid=100, num_invalid=20):
    """
    Generate sample motor insurance data

    Args:
        output_dir: Output directory path
        num_valid: Number of valid records
        num_invalid: Number of invalid records
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_records = []

    # Generate valid records
    print(f"Generating {num_valid} valid records...")
    for i in range(num_valid):
        all_records.append(generate_valid_record(i))

    # Generate invalid records
    print(f"Generating {num_invalid} invalid records...")
    error_types = ["null_age", "empty_plate", "null_plate", "invalid_age", "empty_policy"]
    for i in range(num_invalid):
        error_type = random.choice(error_types)
        all_records.append(generate_invalid_record(i + num_valid, error_type))

    # Shuffle records
    random.shuffle(all_records)

    # Write to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"motor_policy_{timestamp}.json"

    with open(output_file, 'w') as f:
        for record in all_records:
            f.write(json.dumps(record) + '\n')

    print(f"✅ Generated {len(all_records)} records")
    print(f"📁 Output file: {output_file}")
    print(f"   - Valid records: {num_valid}")
    print(f"   - Invalid records: {num_invalid}")

    # Generate summary
    summary = {
        "total_records": len(all_records),
        "valid_records": num_valid,
        "invalid_records": num_invalid,
        "file_path": str(output_file),
        "generated_at": datetime.now().isoformat()
    }

    summary_file = output_path / f"summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"📊 Summary file: {summary_file}")


def generate_batch_files(output_dir, num_files=5, records_per_file=50):
    """Generate multiple batch files"""
    for i in range(num_files):
        print(f"\n=== Generating batch file {i + 1}/{num_files} ===")
        generate_sample_data(
            output_dir,
            num_valid=records_per_file,
            num_invalid=random.randint(5, 15)
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate sample motor insurance data")
    parser.add_argument(
        "--output-dir",
        default="./data/input/events/motor_policy",
        help="Output directory for generated data"
    )
    parser.add_argument(
        "--num-valid",
        type=int,
        default=100,
        help="Number of valid records to generate"
    )
    parser.add_argument(
        "--num-invalid",
        type=int,
        default=20,
        help="Number of invalid records to generate"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Generate multiple batch files"
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=5,
        help="Number of batch files to generate (with --batch)"
    )

    args = parser.parse_args()

    print("=" * 80)
    print("OMINIMO - Sample Data Generator")
    print("=" * 80)
    print()

    if args.batch:
        generate_batch_files(args.output_dir, args.num_files)
    else:
        generate_sample_data(args.output_dir, args.num_valid, args.num_invalid)

    print()
    print("=" * 80)
    print("✅ Data generation complete!")
    print("=" * 80)
    print()
    print("Next steps:")
    print(f"1. Check the generated files: {args.output_dir}")
    print("2. Run the pipeline to process this data")
    print()