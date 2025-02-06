import os
import requests
import sqlite3

# Retrieve the Tomorrow.io API key from Codespaces secrets
api_key = os.getenv("TOMORROW_API_KEY")

# Define a basic request to Tomorrow.io's API for current weather data (replace with any valid endpoint for your test)
# This example fetches weather data for a specific location (latitude and longitude)
latitude = 42.3601   # Example: Latitude for Boston, MA
longitude = -71.0589 # Example: Longitude for Boston, MA

# Build the request URL
# url = f"https://api.tomorrow.io/v4/timelines?location={latitude},{longitude}&fields=temperature&timesteps=current&apikey={api_key}"
url = f"https://api.tomorrow.io/v4/timelines?location=42.3601,-71.0589&fields=solarGHI,windSpeed,cloudCover,temperature,humidity&timesteps=1h&units=metric&apikey={api_key}"

# Send the GET request to Tomorrow.io
response = requests.get(url)


# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('data_base.db')
cursor = conn.cursor()

# Create a table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT,
    cloud_cover REAL,
    humidity REAL,
    temperature REAL,
    wind_speed REAL
)
''')

# Extract data from the output
for timeline in response['data']['timelines']:
    for interval in timeline['intervals']:
        start_time = interval['startTime']
        cloud_cover = interval['values']['cloudCover']
        humidity = interval['values']['humidity']
        temperature = interval['values']['temperature']
        wind_speed = interval['values']['windSpeed']

        # Insert the data into the table
        cursor.execute('''
        INSERT INTO weather (start_time, cloud_cover, humidity, temperature, wind_speed)
        VALUES (?, ?, ?, ?, ?)
        ''', (start_time, cloud_cover, humidity, temperature, wind_speed))

# Commit the changes and close the connection

# Connect to the SQLite database
conn = sqlite3.connect('data_base.db')
cursor = conn.cursor()

# Query the weather table
cursor.execute('SELECT * FROM weather')

# Fetch all records
records = cursor.fetchall()

# Print the records
for record in records:
    print(record)

# Close the connection
conn.commit()
conn.close()

