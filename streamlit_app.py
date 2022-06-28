from matplotlib.style import use
import streamlit as st
from data_pipeline import connect_to_db
import pandas as pd
import altair as alt


@st.cache
def get_DB_data():

    host_name= st.secrets['host_name']
    dbname= st.secrets['dbname']
    port= st.secrets['port']
    username= st.secrets['username']
    password= st.secrets['password']

    #Establishing connection to the db
    conn = connect_to_db(host_name, dbname, username, password, port)
    curr = conn.cursor()

    curr.execute('''SELECT * FROM videos''')
    data = curr.fetchall()

    df = pd.DataFrame(data, columns=['video_id', 'video_title', 'video_date', 'view_count', 'like_count', 'comment_count'])
    df = df.set_index('video_date')
    df = df.drop('video_id', inplace = True, axis=1)

    return df

data = get_DB_data()

chart = (
    alt.Chart(data)
    .mark_area(opacity=0.3)
    .encode(
        x='video_date',
        y='view_count'
    )
)

st.altair_chart(chart, use_container_width=True)
