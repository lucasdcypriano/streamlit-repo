import pandas as pd
import time
import requests
import streamlit as st
import psycopg2 as ps

def get_video_stats(video_id):
    
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+st.secrets['API_KEY']
    response_stats = requests.get(url_video_stats).json()
    view_count, like_count, comment_count = '', '', ''
    view_count = response_stats['items'][0]['statistics']['viewCount']
    like_count = response_stats['items'][0]['statistics']['likeCount']
    try:
        comment_count = response_stats['items'][0]['statistics']['commentCount']
    except:
        comment_count = '0'
    
    return view_count, like_count, comment_count

def get_video(df):

    pageToken = ''
    while 1:

        url = "https://www.googleapis.com/youtube/v3/search?key="+st.secrets['API_KEY']+"&channelId="+st.secrets['CHANNEL_ID']+"&part=snippet,id&order=date&maxResults=1000000&"+pageToken
        response = requests.get(url).json()
        time.sleep(1)
        for video in response['items']:
            if video['id']['kind'] == 'youtube#video':
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                video_date = video['snippet']['publishTime']

                view_count, like_count, comment_count = get_video_stats(video_id)

                df = df.append({'video_id':video_id, 'video_title':video_title, 'video_date':video_date, 'view_count':view_count,
                                'like_count':like_count, 'comment_count':comment_count}, ignore_index=True)
        
        
        try:
            if response['nextPageToken'] != None: #if none, it means it reached the last page and break out of it
                pageToken = "pageToken=" + response['nextPageToken']
        
        except:
            break
            
    return df

df = pd.DataFrame(columns = ['video_id', 'video_title', 'video_date', 'view_count', 'like_count', 'comment_count'])
df = get_video(df)

#changing some DTypes
df.like_count.astype('int32')
df.comment_count.astype('int32')
df.video_date = pd.to_datetime(df.video_date)

#connect to db
def connect_to_db(host_name, dbname, username, password, port):
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)

    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    
    return conn

#create table
def create_table(curr):
    
    create_table_command = (''' CREATE TABLE IF NOT EXISTS videos (
                            video_id VARCHAR(255) PRIMARY KEY,
                            video_title TEXT NOT NULL,
                            video_date DATE NOT NULL DEFAULT CURRENT_DATE,
                            view_count INTEGER NOT NULL,
                            like_count INTEGER NOT NULL,
                            comment_count INTEGER NOT NULL
                            )''')
    
    curr.execute(create_table_command)


#insert videos that arent on the DB
def insert_into_table(curr, video_id, video_title, video_date, view_count, like_count, comment_count):    
    insert_into_videos = ('''INSERT INTO videos (video_id, video_title, video_date, view_count, like_count, comment_count)
        VALUES(%s,%s,%s,%s,%s,%s)''')
    row_to_insert = (video_id, video_title, video_date, view_count, like_count, comment_count)

    curr.execute(insert_into_videos, row_to_insert)
    

#update row if video exists
def update_row(curr, video_id, video_title, view_count, like_count, comment_count, bizarre_stuff):
    query = ('''UPDATE videos
                SET video_title = %s,
                    view_count = %s,
                    like_count = %s,
                    comment_count = %s
                WHERE video_id = %s;''')

    vars_to_update = (video_title, view_count, like_count, comment_count, video_id)
    curr.execute(query, vars_to_update)

#check if video exists
def check_if_video_exists(curr, video_id):
    query = ('''SELECT video_id FROM videos WHERE video_id = %s''')
    curr.execute(query, (video_id,))
    
    return curr.fetchone() is not None


def truncate_table(curr):
    truncate_table = ('''TRUNCATE TABLE videos''')
    
    curr.execute(truncate_table)
    
def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['video_id'], row['video_title'], row['video_date'], row['view_count'], row['like_count'], row['comment_count'])
        
def update_db(curr, df):
    tmp_df = pd.DataFrame(columns=['video_id', 'video_title', 'video_date', 'view_count', 'like_count', 'comment_count'])
    
    for i, row in df.iterrows():
        if check_if_video_exists(curr, row['video_id']):
            update_row(curr, row['video_id'], row['video_title'],row['view_count'],row['like_count'],row['comment_count'])
        else:
            tmp_df = tmp_df.append(row)
            
    return tmp_df


host_name= st.secrets['host_name']
dbname= st.secrets['dbname']
port= st.secrets['port']
username= st.secrets['username']
password= st.secrets['password']

#Establishing connection to the db
conn = connect_to_db(host_name, dbname, username, password, port)
curr = conn.cursor()

create_table(curr)

new_vid_df = update_db(curr,df)
conn.commit()

append_from_df_to_db(curr, new_vid_df)
conn.commit()

curr.close()