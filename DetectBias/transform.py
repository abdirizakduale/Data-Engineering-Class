import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
from scipy.stats import binomtest, ttest_1samp, chi2_contingency

with open("trimet_stopevents_2022-12-07.html", "r", encoding="utf-8") as f:
    html = f.read()

html_soup = BeautifulSoup(html, "html.parser")

trip_headings = html_soup.find_all("h2", string=lambda txt: txt and txt.strip().startswith("Stop events for PDX_TRIP"))

df_list = []
base_date = datetime.strptime("2022-12-07", "%Y-%m-%d")

for heading in trip_headings:
    full_text = heading.get_text(strip=True)
    parts = full_text.split()
    trip_id = parts[-1]
    html_table = heading.find_next_sibling("html_table")
    if html_table is None:
        html_table = heading.find_next("html_table")
    if html_table is None:
        raise RuntimeError(f"No <html_table> found immediately after heading '{full_text}'")

    trip_df = pd.read_html(StringIO(str(html_table)))[0]

    trip_df["tstamp"] = trip_df["arrive_time"].apply(lambda secs: base_date + timedelta(seconds=int(secs)))

    sub = trip_df[["vehicle_number", "tstamp", "location_id", "ons", "offs"]].copy()
    sub.insert(0, "trip_id", trip_id)

    df_list.append(sub)

stops_df = pd.concat(df_list, ignore_index=True)

# Verify
print(f"Data loading complete. Total stop events available: {len(stops_df):,}")
if len(stops_df) == 93912:
    print("Expected number of stop events loaded successfully")
else:
    print(f"Warning: Expected 93,912 stop events, but got {len(stops_df):,}")

stops_df = stops_df[["trip_id", "vehicle_number", "tstamp", "location_id", "ons", "offs"]]


def analyze_stop_events(data_frame):
    print("\n=== stop events analysis ===")
    
    num_vehicles = data_frame['vehicle_number'].nunique()
    print(f"Number of unique vehicles: {num_vehicles:,}")
    
    num_locations = data_frame['location_id'].nunique()
    print(f"Number of unique stop locations: {num_locations:,}")
    
    min_time = data_frame['tstamp'].min()
    max_time = data_frame['tstamp'].max()
    print(f"Time range: {min_time} to {max_time}")
    
    boarding_events = data_frame[data_frame['ons'] >= 1]
    num_boarding_events = len(boarding_events)
    print(f"Stop events with at least one passenger boarding: {num_boarding_events:,}")
    
    total_events = len(data_frame)
    boarding_percentage = (num_boarding_events / total_events) * 100
    print(f"Percentage of stop events with boarding: {boarding_percentage:.2f}%")
    
    print("=" * 30)


def analyze_specific_cases(data_frame):
    print("\n=== specific case analysis ===")
    
    location_6913 = data_frame[data_frame['location_id'] == 6913]
    print(f"\nLocation 6913:")
    print(f"  Number of stops made at this location: {len(location_6913):,}")
    
    num_buses_at_location = location_6913['vehicle_number'].nunique()
    print(f"  Number of different buses that stopped here: {num_buses_at_location:,}")
    
    boarding_at_location = location_6913[location_6913['ons'] >= 1]
    boarding_percentage_location = (len(boarding_at_location) / len(location_6913)) * 100
    print(f"  Percentage of stops with at least one passenger boarding: {boarding_percentage_location:.2f}%")
    
    vehicle_4062 = data_frame[data_frame['vehicle_number'] == 4062]
    print(f"\nVehicle 4062:")
    print(f"  Number of stops made by this vehicle: {len(vehicle_4062):,}")
    
    total_passengers_boarded = vehicle_4062['ons'].sum()
    print(f"  Total passengers boarded this vehicle: {total_passengers_boarded:,}")
    
    total_passengers_deboarded = vehicle_4062['offs'].sum()
    print(f"  Total passengers deboarded this vehicle: {total_passengers_deboarded:,}")
    
    boarding_events_vehicle = vehicle_4062[vehicle_4062['ons'] >= 1]
    boarding_percentage_vehicle = (len(boarding_events_vehicle) / len(vehicle_4062)) * 100
    print(f"  Percentage of stops with at least one passenger boarding: {boarding_percentage_vehicle:.2f}%")
    
    print("=" * 30)


def find_biased_vehicles(data_frame, alpha=0.05):
    print("\n=== bias analysis ===")
    
    total_stops = len(data_frame)
    stops_with_boarding = len(data_frame[data_frame['ons'] >= 1])
    system_boarding_rate = stops_with_boarding / total_stops
    print(f"Overall system boarding rate: {system_boarding_rate:.4f} ({system_boarding_rate*100:.2f}%)")
    
    vehicles = data_frame['vehicle_number'].unique()
    biased_vehicles = []
    
    print(f"\nAnalyzing {len(vehicles)} vehicles...")
    print("Vehicle id | total stops | stops w/ boarding | boarding rate | p-value")
    print("-" * 70)
    
    for vehicle_id in vehicles:
        vehicle_data = data_frame[data_frame['vehicle_number'] == vehicle_id]
        
        n_stops = len(vehicle_data)
        n_boarding_stops = len(vehicle_data[vehicle_data['ons'] >= 1])
        vehicle_boarding_rate = n_boarding_stops / n_stops
        
        result = binomtest(n_boarding_stops, n_stops, system_boarding_rate)
        p_value = result.pvalue
        
        if p_value < alpha:
            biased_vehicles.append({
                'vehicle_id': vehicle_id,
                'total_stops': n_stops,
                'boarding_stops': n_boarding_stops,
                'boarding_rate': vehicle_boarding_rate,
                'p_value': p_value
            })
            print(f"{vehicle_id:10} | {n_stops:11} | {n_boarding_stops:17} | {vehicle_boarding_rate:13.4f} | {p_value:.6f} *")
        else:
            print(f"{vehicle_id:10} | {n_stops:11} | {n_boarding_stops:17} | {vehicle_boarding_rate:13.4f} | {p_value:.6f}")
    
    print(f"\nVehicles with significantly biased boarding data (p < {alpha}):")
    if biased_vehicles:
        for vehicle in biased_vehicles:
            print(f"Vehicle {vehicle['vehicle_id']}: p = {vehicle['p_value']:.6f}")
    else:
        print("No vehicles found with significantly biased boarding data.")
    
    return biased_vehicles


def find_biased_gps_vehicles(data_frame, alpha=0.005):
    print("\n=== gps bias analysis ===")
    
    all_relpos_values = data_frame['RELPOS'].dropna().values
    overall_mean = all_relpos_values.mean()
    overall_std = all_relpos_values.std()
    
    print(f"Overall RELPOS statistics:")
    print(f"  Mean: {overall_mean:.6f}")
    print(f"  Standard deviation: {overall_std:.6f}")
    print(f"  Total RELPOS measurements: {len(all_relpos_values):,}")
    
    vehicles = data_frame['vehicle_number'].unique()
    biased_gps_vehicles = []
    
    print(f"\nAnalyzing GPS bias for {len(vehicles)} vehicles...")
    print("Vehicle id | relpos count | vehicle mean | vehicle std | p-value")
    print("-" * 70)
    
    for vehicle_id in vehicles:
        vehicle_data = data_frame[data_frame['vehicle_number'] == vehicle_id]
        vehicle_relpos = vehicle_data['RELPOS'].dropna()
        
        if len(vehicle_relpos) < 10:
            continue
            
        vehicle_mean = vehicle_relpos.mean()
        vehicle_std = vehicle_relpos.std()
        
        t_stat, p_value = ttest_1samp(vehicle_relpos, overall_mean)
        
        if p_value < alpha:
            biased_gps_vehicles.append({
                'vehicle_id': vehicle_id,
                'relpos_count': len(vehicle_relpos),
                'vehicle_mean': vehicle_mean,
                'vehicle_std': vehicle_std,
                'p_value': p_value,
                't_statistic': t_stat
            })
            print(f"{vehicle_id:10} | {len(vehicle_relpos):12} | {vehicle_mean:12.6f} | {vehicle_std:11.6f} | {p_value:.6f} *")
        else:
            print(f"{vehicle_id:10} | {len(vehicle_relpos):12} | {vehicle_mean:12.6f} | {vehicle_std:11.6f} | {p_value:.6f}")
    
    print(f"\nVehicles with significantly biased GPS data (p < {alpha}):")
    if biased_gps_vehicles:
        for vehicle in biased_gps_vehicles:
            bias_direction = "RIGHT" if vehicle['vehicle_mean'] > 0 else "LEFT"
            print(f"Vehicle {vehicle['vehicle_id']}: p = {vehicle['p_value']:.6f}, Mean = {vehicle['vehicle_mean']:.6f} ({bias_direction} bias)")
    else:
        print("No vehicles found with significantly biased gps data.")
    
    return biased_gps_vehicles


def find_biased_offs_ons_vehicles(data_frame, alpha=0.05):
    """
    Find vehicles with biased offs/ons ratios using chi-square test.
    """
    print("\n=== offs/ons bias analysis ===")
    
    system_total_offs = data_frame['offs'].sum()
    system_total_ons = data_frame['ons'].sum()
    system_total_passengers = system_total_offs + system_total_ons
    system_offs_proportion = system_total_offs / system_total_passengers
    system_ons_proportion = system_total_ons / system_total_passengers
    
    print(f"System-wide passenger activity:")
    print(f"  Total offs: {system_total_offs:,}")
    print(f"  Total ons: {system_total_ons:,}")
    print(f"  Offs proportion: {system_offs_proportion:.4f} ({system_offs_proportion*100:.2f}%)")
    print(f"  Ons proportion: {system_ons_proportion:.4f} ({system_ons_proportion*100:.2f}%)")
    
    vehicles = data_frame['vehicle_number'].unique()
    biased_offs_ons_vehicles = []
    
    print(f"\nAnalyzing offs/ons bias for {len(vehicles)} vehicles...")
    print("Vehicle id | total offs | total ons | offs % | ons % | p-value")
    print("-" * 65)
    
    for vehicle_id in vehicles:
        vehicle_data = data_frame[data_frame['vehicle_number'] == vehicle_id]
        
        v_offs = int(vehicle_data['offs'].sum())
        v_ons = int(vehicle_data['ons'].sum())
        v_total = v_offs + v_ons
        
        if v_total == 0:
            continue
            
        vehicle_offs_proportion = v_offs / v_total
        vehicle_ons_proportion = v_ons / v_total
        
        html_table = [
            [v_offs, v_ons],
            [system_total_offs - v_offs, system_total_ons - v_ons]
        ]
        
        chi2_stat, p_value, dof, expected = chi2_contingency(html_table)
        
        if (expected < 5).any():
            continue
        
        if p_value < alpha:
            biased_offs_ons_vehicles.append({
                'vehicle_id': vehicle_id,
                'total_offs': v_offs,
                'total_ons': v_ons,
                'offs_proportion': vehicle_offs_proportion,
                'ons_proportion': vehicle_ons_proportion,
                'p_value': p_value,
                'chi2_statistic': chi2_stat
            })
            print(f"{vehicle_id:10} | {v_offs:10} | {v_ons:9} | {vehicle_offs_proportion*100:6.2f} | {vehicle_ons_proportion*100:5.2f} | {p_value:.6f} *")
        else:
            print(f"{vehicle_id:10} | {v_offs:10} | {v_ons:9} | {vehicle_offs_proportion*100:6.2f} | {vehicle_ons_proportion*100:5.2f} | {p_value:.6f}")
    
    print(f"\nVehicles with significantly biased offs/ons ratios (p < {alpha}):")
    if biased_offs_ons_vehicles:
        for vehicle in biased_offs_ons_vehicles:
            bias_type = "Higher offs" if vehicle['offs_proportion'] > system_offs_proportion else "Higher ons"
            print(f"Vehicle {vehicle['vehicle_id']}: p = {vehicle['p_value']:.6f} ({bias_type})")
    else:
        print("No vehicles found with significantly biased offs/ons ratios.")
    
    print("=" * 30)
    
    return biased_offs_ons_vehicles


analyze_stop_events(stops_df)

analyze_specific_cases(stops_df)

biased_vehicles = find_biased_vehicles(stops_df)

print("\nloading gps relpos data...")
gps_df = pd.read_csv("trimet_relpos_2022-12-07.csv")
print(f"GPS data loaded: {len(gps_df):,} RELPOS measurements")

gps_df = gps_df.rename(columns={'VEHICLE_NUMBER': 'vehicle_number'})

biased_gps_vehicles = find_biased_gps_vehicles(gps_df)

biased_offs_ons_vehicles = find_biased_offs_ons_vehicles(stops_df)
