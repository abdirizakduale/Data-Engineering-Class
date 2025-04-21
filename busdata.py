import requests
from datetime import datetime

# Get current date for filename
today = datetime.now().strftime("%Y-%m-%d")
filename = "bcsample.json" 

vehicle_ids = [
        2901, 2902, 2904, 2905, 2907, 2908, 2910, 2922, 2924, 2926, 2929, 2935, 2937, 3001, 3002, 3004, 3006,
    3008, 3009, 3010, 3017, 3020, 3029, 3036, 3042, 3046, 3052, 3054, 3059, 3105, 3108, 3110, 3115, 3117,
    3118, 3121, 3122, 3125, 3127, 3128, 3132, 3138, 3141, 3146, 3149, 3150, 3153, 3158, 3160, 3163, 3203,
    3204, 3206, 3208, 3213, 3214, 3219, 3220, 3223, 3226, 3227, 3231, 3233, 3234, 3235, 3237, 3240, 3241,
    ]

base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

with open(filename, "w", encoding="utf-8") as f:
    for vehicle_id in vehicle_ids:
        url = f"{base_url}{vehicle_id}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            f.write(f"--- Vehicle ID: {vehicle_id} ---\n")
            f.write(response.text + "\n\n")
        except requests.RequestException as e:
            f.write(f"--- Vehicle ID: {vehicle_id} ---\n")
            f.write(f"Error fetching data: {e}\n\n")
