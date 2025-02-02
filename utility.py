import pandas as pd
from datetime import datetime
import pydeck as pdk
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import psycopg2
import streamlit as st
import re
from config import EMAIL_CONFIG, POSTGRES_CONFIG

# Database setup
def init_db():
    conn = psycopg2.connect(POSTGRES_CONFIG['uri'])  # Connect to PostgreSQL
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS subscriptions
                 (email TEXT PRIMARY KEY, min_magnitude REAL, event_type TEXT, sent BOOLEAN DEFAULT FALSE)"""
    )
    conn.commit()
    conn.close()

# Function to validate email
def is_valid_email(email):
    regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(regex, email) is not None

# Function to add subscription to the database
def add_subscription(email, min_magnitude, event_type):
    conn = psycopg2.connect(POSTGRES_CONFIG['uri'])  # Connect to PostgreSQL
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO subscriptions (email, min_magnitude, event_type, sent) VALUES (%s, %s, %s, %s)",
            (email, min_magnitude, event_type, False),
        )
        conn.commit()
        st.success("You have been successfully subscribed!")
    except psycopg2.IntegrityError:
        st.error("This email is already subscribed.")
    finally:
        conn.close()

# Function to generate the newsletter content
def generate_newsletter(data):
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

    # Convert to DataFrame
    df = pd.DataFrame(earthquakes)

    # Generate summary table (top 10 by magnitude)
    summary_table = df.sort_values(by="Magnitude", ascending=False).head(10)

    # Generate key statistics
    total_earthquakes = len(df)
    strongest_earthquake = df.loc[df["Magnitude"].idxmax()]
    most_affected_region = df["Place"].mode()[0]

    # Generate map visualization
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_color=[200, 30, 0, 160],
        get_radius=10000,
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=df["latitude"].mean(),
        longitude=df["longitude"].mean(),
        zoom=5,
        pitch=0,
    )

    map_html = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"html": "<b>Place:</b> {Place} <br/> <b>Magnitude:</b> {Magnitude} <br/> <b>Time:</b> {Time}"},
    ).to_html()

    # Create the newsletter content
    newsletter = f"""
    <h1>Daily Earthquake Newsletter</h1>
    <p>Here is your daily update on earthquake activity:</p>

    <h2>Summary Table (Top 10 by Magnitude)</h2>
    {summary_table.to_html(index=False)}

    <h2>Key Statistics</h2>
    <ul>
        <li><strong>Total Earthquakes:</strong> {total_earthquakes}</li>
        <li><strong>Strongest Earthquake:</strong> {strongest_earthquake['Magnitude']} magnitude at {strongest_earthquake['Place']} on {strongest_earthquake['Time']}</li>
        <li><strong>Most Affected Region:</strong> {most_affected_region}</li>
    </ul>

    <h2>Map Visualization</h2>
    {map_html}

    <h2>More Information</h2>
    <p>For more details, visit the <a href="https://earthquake.usgs.gov/earthquakes/map/">USGS Earthquake Map</a>.</p>
    """

    return newsletter

# Function to fetch earthquake data
def fetch_earthquake_data(start_date, end_date, min_magnitude, event_type):
    url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}"
        f"&endtime={end_date}&minmagnitude={min_magnitude}&eventtype={event_type}"
    )
    response = requests.get(url)
    return response.json()

# Function to reset sent flags
def reset_sent_flags():
    conn = psycopg2.connect(**POSTGRES_CONFIG)  # Connect to PostgreSQL
    c = conn.cursor()
    c.execute("UPDATE subscriptions SET sent = FALSE")
    conn.commit()
    conn.close()

# Function to send email
def send_email(email, content):
    conn = psycopg2.connect(**POSTGRES_CONFIG)  # Connect to PostgreSQL
    c = conn.cursor()

    # Check if the email has already been sent
    c.execute("SELECT sent FROM subscriptions WHERE email = %s", (email,))
    result = c.fetchone()

    if result and result[0]:  # Email already sent
        conn.close()
        return

    # Send the email
    msg = MIMEMultipart()
    msg["Subject"] = "Daily Earthquake Newsletter"
    msg["From"] = EMAIL_CONFIG["email"]
    msg["To"] = email
    msg.attach(MIMEText(content, "html"))

    try:
        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], port=EMAIL_CONFIG["smtp_port"]) as server:
            server.login(EMAIL_CONFIG["email"], EMAIL_CONFIG["password"])
            server.sendmail(EMAIL_CONFIG["email"], email, msg.as_string())

        # Mark the email as sent
        c.execute("UPDATE subscriptions SET sent = TRUE WHERE email = %s", (email,))
        conn.commit()
    except Exception as e:
        st.error(f"Failed to send email to {email}: {e}")
    finally:
        conn.close()