import pandas as pd
from datetime import datetime, timedelta

# ✅ Reload the dataset and exclude unwanted columns right away
df = pd.read_csv(
    'bc_trip259172515_230215.csv',
    usecols=lambda col: col not in ['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP']
)

# ✅ Corrected date parser: OPD_DATE format is '15FEB2023:00:00:00'
def create_timestamp(row):
    base_date = datetime.strptime(row['OPD_DATE'], "%d%b%Y:%H:%M:%S")  # <-- FIXED FORMAT
    time_delta = timedelta(seconds=int(row['ACT_TIME']))
    return base_date + time_delta

# ✅ Create TIMESTAMP
df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)

# ✅ Drop original columns
df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

# ✅ Compute time and distance differences
df['dMETERS'] = df['METERS'].diff()
df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()

# ✅ Calculate SPEED in m/s
df['SPEED'] = df.apply(
    lambda row: row['dMETERS'] / row['dTIMESTAMP'] if row['dTIMESTAMP'] and row['dTIMESTAMP'] > 0 else 0,
    axis=1
)

# ✅ Drop intermediate difference columns
df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

# ✅ Compute speed stats
min_speed = df['SPEED'].min()
max_speed = df['SPEED'].max()
avg_speed = df['SPEED'].mean()

print(f"Min Speed: {min_speed} m/s")
print(f"Max Speed: {max_speed} m/s")
print(f"Average Speed: {avg_speed:.2f} m/s")