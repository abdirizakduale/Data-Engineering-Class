#!/usr/bin/env python3
import argparse, csv, io, os, time
import psycopg2

DBNAME, DBUSER, DBPWD = "postgres", "postgres", "password"
HOST, PORT = "localhost", 5432
TABLE = "CensusData"

def init_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("-d", "--datafile", required=True)
    p.add_argument("-c", "--createtable", action="store_true")
    p.add_argument("--method", choices=("copy", "insert"), default="copy")
    return p.parse_args()

def dbconnect():
    conn = psycopg2.connect(host=HOST, port=PORT, dbname=DBNAME, user=DBUSER, password=DBPWD)
    conn.autocommit = True
    return conn

DDL = f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
    CensusTract         NUMERIC,
    State               TEXT,
    County              TEXT,
    TotalPop            INTEGER,
    Men                 INTEGER,
    Women               INTEGER,
    Hispanic            DECIMAL,
    White               DECIMAL,
    Black               DECIMAL,
    Native              DECIMAL,
    Asian               DECIMAL,
    Pacific             DECIMAL,
    Citizen             DECIMAL,
    Income              DECIMAL,
    IncomeErr           DECIMAL,
    IncomePerCap        DECIMAL,
    IncomePerCapErr     DECIMAL,
    Poverty             DECIMAL,
    ChildPoverty        DECIMAL,
    Professional        DECIMAL,
    Service             DECIMAL,
    Office              DECIMAL,
    Construction        DECIMAL,
    Production          DECIMAL,
    Drive               DECIMAL,
    Carpool             DECIMAL,
    Transit             DECIMAL,
    Walk                DECIMAL,
    OtherTransp         DECIMAL,
    WorkAtHome          DECIMAL,
    MeanCommute         DECIMAL,
    Employed            INTEGER,
    PrivateWork         DECIMAL,
    PublicWork          DECIMAL,
    SelfEmployed        DECIMAL,
    FamilyWork          DECIMAL,
    Unemployment        DECIMAL
);
"""

CONSTRAINTS = f"""
ALTER TABLE {TABLE} ADD PRIMARY KEY (CensusTract);
CREATE INDEX idx_{TABLE}_State ON {TABLE}(State);
"""

def sanitize_row(row: dict) -> list[str]:
    cleaned = []
    for k in (
        'TractId','State','County','TotalPop','Men','Women','Hispanic','White',
        'Black','Native','Asian','Pacific','VotingAgeCitizen','Income',
        'IncomeErr','IncomePerCap','IncomePerCapErr','Poverty','ChildPoverty',
        'Professional','Service','Office','Construction','Production','Drive',
        'Carpool','Transit','Walk','OtherTransp','WorkAtHome','MeanCommute',
        'Employed','PrivateWork','PublicWork','SelfEmployed','FamilyWork',
        'Unemployment'
    ):
        v = row.get(k, "")
        if v == "":
            v = "0"
        if k == "County":
            v = v.replace("'", "")
        cleaned.append(v)
    return cleaned

def write_temp_clean_csv(src_csv: str) -> str:
    tmp_path = src_csv + ".clean"
    with open(src_csv, newline="") as inp, open(tmp_path, "w", newline="") as outp:
        rdr, wtr = csv.DictReader(inp), csv.writer(outp)
        wtr.writerow(rdr.fieldnames)
        for row in rdr:
            wtr.writerow(sanitize_row(row))
    return tmp_path

def load_with_copy(conn, csv_file: str):
    clean_csv = write_temp_clean_csv(csv_file)
    with conn.cursor() as cur, open(clean_csv, "r") as f:
        print(f"COPY-loading from {clean_csv}")
        start = time.perf_counter()
        cur.copy_expert(f"COPY {TABLE} FROM STDIN WITH CSV HEADER", f)
        secs = time.perf_counter() - start
        print(f"Finished COPY. Elapsed: {secs:0.4f} s")
    os.remove(clean_csv)

def row2vals(row):
    for key in row:
        if row[key] == "":
            row[key] = 0
        if key == "County":
            row[key] = row[key].replace("'", "")
    return f"""
       {row['TractId']},
       '{row['State']}',
       '{row['County']}',
       {row['TotalPop']},
       {row['Men']},
       {row['Women']},
       {row['Hispanic']},
       {row['White']},
       {row['Black']},
       {row['Native']},
       {row['Asian']},
       {row['Pacific']},
       {row['VotingAgeCitizen']},
       {row['Income']},
       {row['IncomeErr']},
       {row['IncomePerCap']},
       {row['IncomePerCapErr']},
       {row['Poverty']},
       {row['ChildPoverty']},
       {row['Professional']},
       {row['Service']},
       {row['Office']},
       {row['Construction']},
       {row['Production']},
       {row['Drive']},
       {row['Carpool']},
       {row['Transit']},
       {row['Walk']},
       {row['OtherTransp']},
       {row['WorkAtHome']},
       {row['MeanCommute']},
       {row['Employed']},
       {row['PrivateWork']},
       {row['PublicWork']},
       {row['SelfEmployed']},
       {row['FamilyWork']},
       {row['Unemployment']}
    """

def load_with_inserts(conn, csv_file: str):
    with open(csv_file, newline="") as fil:
        rows = list(csv.DictReader(fil))
    cmds = [f"INSERT INTO {TABLE} VALUES ({row2vals(r)});" for r in rows]
    with conn.cursor() as cur:
        print(f"Inserting {len(cmds):,} rows one-by-one …")
        start = time.perf_counter()
        for c in cmds:
            cur.execute(c)
        secs = time.perf_counter() - start
        print(f"Finished INSERT loop. Elapsed: {secs:0.4f} s")

def main():
    args = init_cli()
    conn = dbconnect()
    if args.createtable:
        with conn.cursor() as cur:
            cur.execute(DDL)
            print(f"Table {TABLE} created.")
    if args.method == "copy":
        load_with_copy(conn, args.datafile)
    else:
        load_with_inserts(conn, args.datafile)
    if args.createtable:
        with conn.cursor() as cur:
            cur.execute(CONSTRAINTS)
            print("Constraints & index added.")

if __name__ == "__main__":
    main()
