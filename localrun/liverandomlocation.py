import streamlit as st
import pandas as pd
import folium
from folium.plugins import HeatMap
from datetime import datetime, timedelta
import random

# Function to generate sample data
def generate_movement_data_bangalore(num_records, center_lat, center_lon, radius=0.001, start_time=None):
    if start_time is None:
        start_time = datetime.now()
    data = []
    for _ in range(num_records):
        hashed_device_id = ''.join(random.choices('abcdef1234567890', k=32))
        lat_visit = round(center_lat + random.uniform(-radius, radius), 6)
        lon_visit = round(center_lon + random.uniform(-radius, radius), 6)
        timestamp = start_time + timedelta(minutes=random.randint(0, 60))
        time_stamp = int(timestamp.timestamp())  # Unix timestamp
        visit_date = timestamp.strftime('%Y-%m-%d')
        visit_time = timestamp.strftime('%H:%M:%S')
        data.append([hashed_device_id, lat_visit, lon_visit, time_stamp, visit_date, visit_time])
    return pd.DataFrame(data, columns=['hashed_device_id', 'lat_visit', 'lon_visit', 'time_stamp', 'date_visit', 'time_visit'])

# Define center coordinates for Bangalore Race Course
center_lat = 12.9821
center_lon = 77.5806
start_time = datetime.now() - timedelta(hours=1)
df_movement = generate_movement_data_bangalore(500, center_lat, center_lon, start_time=start_time)

# Prepare for timestamp-based simulation
df_movement = df_movement.sort_values(by='time_stamp')
unique_times = df_movement['time_stamp'].unique()

# Streamlit Layout
st.title("Live Movement Heatmap - Bangalore Race Course")

# Slider to select timestamp
selected_time = st.slider("Select Time", min_value=int(unique_times[0]), max_value=int(unique_times[-1]), value=int(unique_times[0]), format="YYYY-MM-DD HH:mm")

# Display relevant data for the selected timestamp
current_data = df_movement[df_movement['time_stamp'] <= selected_time]
st.write(f"Number of Visitors: {current_data.shape[0]}")
st.write(f"Time: {datetime.fromtimestamp(selected_time).strftime('%Y-%m-%d %H:%M:%S')}")

# Create the map and add the heatmap layer
bangalore_map = folium.Map(location=[center_lat, center_lon], zoom_start=14)
heat_data = [[row['lat_visit'], row['lon_visit']] for index, row in current_data.iterrows()]
HeatMap(heat_data, radius=15).add_to(bangalore_map)

# Render the map as an HTML object
map_html = bangalore_map._repr_html_()
st.components.v1.html(map_html, height=500)