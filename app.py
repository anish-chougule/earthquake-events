import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pydeck as pdk
from airflow.dags.utility import *



# Initialize the database
init_db()

# Create the Streamlit app
st.title("Earthquake Data Viewer")

# Initialize session state
if "data_fetched" not in st.session_state:
    st.session_state["data_fetched"] = False
if "earthquake_data" not in st.session_state:
    st.session_state["earthquake_data"] = None

# Create a form to input parameters
with st.form(key='earthquake_form'):
    start_date = st.date_input("Start date", value=datetime.today() - timedelta(days=30), min_value='2000-01-01')
    end_date = st.date_input("End date", value=datetime.today(), max_value=datetime.today())
    min_magnitude = st.slider("Minimum Magnitude", min_value=1.0, max_value=10.0, value=2.5, step=0.1)
    event_type = st.selectbox("Event Type", options=["earthquake", "explosion", "quarry blast", "landslide", "ice quake"])
    sort_by = st.selectbox("Sort by", options=["Magnitude", "Time", "Place"])
    submit_button = st.form_submit_button(label='Fetch Data')

# Fetch data and display results
if submit_button:

    if end_date < start_date:
        st.error("Please enter valid end date.")
    else:
        # Fetch data based on user input
        data = fetch_earthquake_data(start_date, end_date, min_magnitude, event_type)

        # Extract relevant data
        features = data["features"]
        earthquakes = [
            {
                "Place": feature["properties"]["place"],
                "Magnitude": feature["properties"]["mag"],
                "Time": datetime.fromtimestamp(feature["properties"]["time"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "latitude": feature["geometry"]["coordinates"][1],
                "longitude": feature["geometry"]["coordinates"][0],
                "Details": feature["properties"]["url"]
            }
            for feature in features
        ]

        # Convert to DataFrame for display
        df = pd.DataFrame(earthquakes)

        # Store the fetched data and state in session state
        st.session_state["data_fetched"] = True
        st.session_state["earthquake_data"] = df
        st.session_state["start_date"] = start_date
        st.session_state["end_date"] = end_date
        st.session_state["min_magnitude"] = min_magnitude
        st.session_state["event_type"] = event_type
    

# Display fetched data if available
if st.session_state["data_fetched"]:
    
    df = st.session_state["earthquake_data"]

    # Check if DataFrame is empty
    if df.empty:
        st.write("No events found")
    else:

        st.divider()

        st.subheader('Details')

        st.write(len(df), " events found between ", st.session_state["start_date"], " and ", st.session_state["end_date"])
        st.write(f"Event type: {st.session_state['event_type'].capitalize()}")
        st.write("Filtered by minimum of ", st.session_state["min_magnitude"], " magnitude on Richter scale.")
        st.download_button("Download Dataset", data=df.to_csv().encode("utf-8"), file_name="events.csv")

        
        # Sort the DataFrame by the selected column
        df = df.sort_values(by=sort_by, ascending=(sort_by != "Magnitude"))[:100]

        df.reset_index(drop=True, inplace=True)
        
        # Make the details column clickable
        df["Details"] = df["Details"].apply(lambda x: f'<a href="{x}" target="_blank">More Info</a>')

        st.divider()

        st.subheader('Top 10 Events')
        st.write(df[:10].to_html(escape=False), unsafe_allow_html=True)

        # Use pydeck for the map visualization
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["longitude", "latitude"],
            get_color=[200, 30, 0, 160],
            get_radius=50000,
            pickable=True,
        )

        view_state = pdk.ViewState(
            latitude=df["latitude"].mean(),
            longitude=df["longitude"].mean(),
            zoom=5,
            pitch=0,
        )

        tooltip = {
            "html": "<b>Place:</b> {Place} <br/> <b>Magnitude:</b> {Magnitude} <br/> <b>Time:</b> {Time}",
            "style": {"color": "white"}
        }

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
        )

        st.divider()

        st.subheader('First 100 Events Visualized (Sorted)')
        st.pydeck_chart(r)

    # Show the subscription form only after fetching data
    st.divider()
    st.subheader("Subscribe to Mailing List")
    with st.form(key='subscription_form', clear_on_submit=True):
        email = st.text_input("Enter your email address to subscribe to daily earthquake updates")
        subscribe_button = st.form_submit_button(label='Subscribe')

        if subscribe_button:
            if is_valid_email(email):
                add_subscription(
                    email,
                    st.session_state["min_magnitude"],
                    st.session_state["event_type"]
                )
            else:
                st.error("Please enter a valid email address.")