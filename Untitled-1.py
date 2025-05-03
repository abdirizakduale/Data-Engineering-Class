#!/usr/bin/env python3
import csv
from datetime import datetime

def main():
    invalid_name = 0
    invalid_hire = 0

    with open('employees.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # 1) existing‐value assertion on name
            if not row.get('name', '').strip():
                invalid_name += 1

            # 2) existence assertion: hire_date no earlier than 2015-01-01
            hd = row.get('hire_date', '').strip()
            try:
                hire_dt = datetime.strptime(hd, '%Y-%m-%d')
                if hire_dt < datetime(2015, 1, 1):
                    invalid_hire += 1
            except ValueError:
                # malformed or missing date also counts as violation
                invalid_hire += 1

    # report counts
    print(f'Name violations: {invalid_name}')
    print(f'Hire-date violations: {invalid_hire}')

if __name__ == '__main__':
    main()