#!/usr/bin/env python3
import csv
from datetime import datetime
from collections import Counter
from scipy.stats import shapiro

def main():
    invalid_name = 0
    invalid_hire = 0
    invalid_birth_hire = 0
    invalid_manager = 0

    # ——— Pass 1: gather all employee IDs ———
    all_ids = set()
    with open('employees.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                all_ids.add(int(row['eid']))
            except (ValueError, KeyError):
                pass

    # ——— Pass 2: validate row‐level assertions ———
    salaries = []
    with open('employees.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 1) Name non‐null
            if not row.get('name','').strip():
                invalid_name += 1

            # 2) Hire date ≥ 2015‑01‑01
            try:
                hire_dt = datetime.strptime(row.get('hire_date','').strip(), '%Y-%m-%d')
                if hire_dt < datetime(2015, 1, 1):
                    invalid_hire += 1
            except ValueError:
                invalid_hire += 1

            # 3) Birth date < hire date
            try:
                birth_dt = datetime.strptime(row.get('birth_date','').strip(), '%Y-%m-%d')
                if birth_dt >= hire_dt:
                    invalid_birth_hire += 1
            except (ValueError, UnboundLocalError):
                invalid_birth_hire += 1

            # 4) Manager exists
            rep = row.get('reports_to','').strip()
            try:
                if int(rep) not in all_ids:
                    invalid_manager += 1
            except ValueError:
                invalid_manager += 1

            # collect salary for normality test
            try:
                salaries.append(float(row.get('salary','')))
            except ValueError:
                pass

    # ——— 5) Global normality assertion on salaries ———
    stat, p_value = shapiro(salaries)
    normality_passed = (p_value >= 0.05)

    # ——— Report everything ———
    print(f'Name violations:               {invalid_name}')
    print(f'Hire‐date < 2015‑01‑01:        {invalid_hire}')
    print(f'Birth ≥ Hire date:             {invalid_birth_hire}')
    print(f'Unknown manager violations:    {invalid_manager}')
    print(f'Shapiro–Wilk p‑value:         {p_value:.5f}')
    print('Salaries normally distributed: ', 'YES' if normality_passed else 'NO')

if __name__ == '__main__':
    main()