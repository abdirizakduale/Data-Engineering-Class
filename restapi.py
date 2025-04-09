import requests
from datetime import datetime, timedelta

api_key = "API_KEY"
city = 'Portland,US'

current_weather_url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
current_response = requests.get(current_weather_url)
current_data = current_response.json()

if 'rain' in current_data:
    print("A. Yes, it is raining in Portland, OR.")
else:
    print("A. No, it is not raining in Portland, OR right now.")

forecast_url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric'
forecast_response = requests.get(forecast_url)
forecast_data = forecast_response.json()

rain_forecast = False
rain_times = []

now = datetime.now()
three_days_later = now + timedelta(days=3)

for entry in forecast_data['list']:
    forecast_time = datetime.strptime(entry['dt_txt'], "%Y-%m-%d %H:%M:%S")
    if now <= forecast_time <= three_days_later:
        if 'rain' in entry:
            rain_forecast = True
            rain_times.append(entry['dt_txt'])

if rain_forecast:
    print(f"B. Yes, rain.")
    print("Rain expected at:")
    for time in rain_times:
        print(f"  - {time}")
else:
    print("B. No, rain.")
