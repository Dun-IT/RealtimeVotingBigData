import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime
import time

# Database connection function
def get_connection():
    return psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")

# Fetch votes from the database
def fetch_votes():
    conn = get_connection()
    query = """
    SELECT v.voter_name, v.gender, c.candidate_name, c.party_affiliation, vote.voting_time
    FROM votes vote
    JOIN voters v ON vote.voter_id = v.voter_id
    JOIN candidates c ON vote.candidate_id = c.candidate_id
    ORDER BY vote.voting_time DESC
    """
    votes_df = pd.read_sql(query, conn)
    conn.close()
    return votes_df

# Fetch candidates from the database
def fetch_candidates():
    conn = get_connection()
    query = "SELECT candidate_name, party_affiliation FROM candidates"
    candidates_df = pd.read_sql(query, conn)
    conn.close()
    return candidates_df

# Streamlit application
st.title("Real-Time Voting Data Visualization")

# Auto-refresh interval in seconds
refresh_interval = 10

# Fetch data
votes_df = fetch_votes()
candidates_df = fetch_candidates()

st.header("Votes Table")
st.dataframe(votes_df)

st.header("Votes Count by Candidate")
votes_count_df = votes_df['candidate_name'].value_counts().reset_index()
votes_count_df.columns = ['Candidate Name', 'Votes']
st.bar_chart(votes_count_df.set_index('Candidate Name'))

st.header("Votes Over Time")
votes_over_time_df = votes_df.groupby('voting_time').size().reset_index(name='Votes')
votes_over_time_df['voting_time'] = pd.to_datetime(votes_over_time_df['voting_time'])
fig = px.line(votes_over_time_df, x='voting_time', y='Votes', title='Votes Over Time')
st.plotly_chart(fig)

st.header("Candidate Party Affiliation")
st.dataframe(candidates_df)

st.write("Last refreshed at: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Auto-refresh logic
time.sleep(refresh_interval)
st.experimental_rerun()
