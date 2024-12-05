import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
import h3
import numpy as np
import time

# Google Cloud BigQuery setup
PROJECT_ID = "team-plutus-iisc"
TABLE_ID = "location.visited_location_view"
client = bigquery.Client.from_service_account_json('path-to-service-account-json-key/team-plutus-iisc.json')

# Function to fetch data from BigQuery
def fetch_data(start_date, end_date):
    query = f"""
    SELECT lat_visit, lon_visit, date_visit, cluster_pred
    FROM `{TABLE_ID}`
    WHERE DATE(TIMESTAMP(date_visit)) BETWEEN '{start_date}' AND '{end_date}'
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    return df


# Function to convert lat/lon to H3 index (resolution 10)
def get_h3_index(lat, lon, resolution=10):
    return h3.geo_to_h3(lat, lon, resolution)


# Function to aggregate pings data by H3 grid, hour, and date
def aggregate_pings(df):
    # Convert date_visit to datetime
    df['date_visit'] = pd.to_datetime(df['date_visit'])
    df['hour'] = df['date_visit'].dt.hour
    df['date'] = df['date_visit'].dt.date

    # Generate H3 index for each lat/lon
    df['h3_index'] = df.apply(lambda row: get_h3_index(row['lat_visit'], row['lon_visit']), axis=1)

    # Aggregate pings by H3 index, hour, and date
    pings_count = df.groupby(['h3_index', 'hour', 'date']).size().reset_index(name='pings')

    # Calculate the average and std dev of pings for each H3 index and hour
    stats = pings_count.groupby(['h3_index', 'hour'])['pings'].agg(['mean', 'std']).reset_index()
    stats = pd.merge(pings_count, stats, on=['h3_index', 'hour'], how='left')

    # Flag outliers based on the 2 standard deviation rule
    stats['alert'] = stats['pings'] > (stats['mean'] + 2 * stats['std'])

    # Mark as "red wave" if more than 1500 pings in the same location and time
    stats['red_wave'] = stats['pings'] > 1500

    return stats


# Create the UK map using folium
def create_map(mean_lat, mean_lon, stats):
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6)

    # For each H3 index, add a marker to the map
    for _, row in stats.iterrows():
        h3_index = row['h3_index']
        # Use h3.h3_to_geo for getting the lat/lon of the center of the H3 cell
        lat, lon = h3.h3_to_geo(h3_index)  # Returns lat, lon of the H3 cell
        pings = row['pings']

        # If it's a red wave, color it differently
        color = "red" if row['red_wave'] else "blue"
        if row['red_wave']:
            popup_message = f"ALERT! Red Wave - {pings} pings at this location, Date: {row['date']}, Hour: {row['hour']}"
        else:
            popup_message = f"Ping Count: {pings}, Date: {row['date']}, Hour: {row['hour']}"

        folium.CircleMarker(
            location=(lat, lon),
            radius=7,
            color=color,
            fill=True,
            fill_opacity=0.7,
            popup=popup_message,
        ).add_to(m)

    return m


# Streamlit app setup
st.set_page_config(page_title="Population Heatmap with Alerts", layout="wide")
st.title("Live Population Heatmap with Alerts")

# Sidebar for user inputs
start_date = st.sidebar.date_input("Select Start Date", value=pd.Timestamp.now().date() - pd.Timedelta(days=7))
end_date = st.sidebar.date_input("Select End Date", value=pd.Timestamp.now().date())
refresh_interval = st.sidebar.number_input("Refresh Interval (seconds)", min_value=5, max_value=60, value=10, step=5)

# Analytics Section in Sidebar
st.sidebar.subheader("Analytics Insights")
data = fetch_data(start_date, end_date)

if not data.empty:
    # Aggregate pings data and calculate alerts
    stats = aggregate_pings(data)

    # Group by both date and hour to include both in the output
    pings_by_hour_date = data.groupby(
        [data['date_visit'].dt.date.rename('date'), data['date_visit'].dt.hour.rename('hour')]
    ).size().reset_index(name='pings')

    # Cluster distribution
    clusters_count = data['cluster_pred'].value_counts().reset_index(name='count').rename(columns={'index': 'cluster_pred'})

    # Display analytics in the sidebar
    st.sidebar.write(f"Total Data Points: {len(data)}")
    st.sidebar.write(f"Total Clusters: {clusters_count.shape[0]}")
    st.sidebar.write("Pings by Hour and Date:")
    st.sidebar.dataframe(pings_by_hour_date)
    st.sidebar.write("Cluster Distribution:")
    st.sidebar.dataframe(clusters_count)

    # Get mean lat/lon for centering the map
    mean_lat = data["lat_visit"].mean()
    mean_lon = data["lon_visit"].mean()

    # Create the UK map and add markers
    map_ = create_map(mean_lat, mean_lon, stats)

    # Display map in Streamlit
    st_folium(map_, width=700, height=500)

else:
    st.warning("No data found for the selected date range.")

# Trigger auto-refresh using a button
if st.button("Refresh Map"):
    st.rerun()

# Display refresh interval
st.write(f"Map will refresh in {refresh_interval} seconds.")
time.sleep(refresh_interval)
