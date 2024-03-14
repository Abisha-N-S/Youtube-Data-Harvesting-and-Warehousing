from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_key='AIzaSyDVCazInf7gPtkm0r6Ya1Q5NKhUi5E9nHM'
api_service_name = "youtube"
api_version = "v3"

youtube = build(api_service_name, api_version,developerKey=api_key)

#Get Channel Details
def Channel_details(Channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=Channel_id
        )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_name=i['snippet']['title'],
                Channel_id=i['id'],
                Subscription_count=i['statistics']['subscriberCount'],
                Channel_view=i['statistics']['viewCount'],
                Channel_description=i['snippet']['description'],
                Total_videos=i['statistics']['videoCount'],
                Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#Get Video ids
def video_ids(Channel_id):
    response = youtube.channels().list(
            part="contentDetails",
            id=Channel_id
        ).execute()
    playlist=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    Video_id=[]
    next_page_token=None

    while True:
        playlist_response = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(playlist_response['items'])):
            Video_id.append(playlist_response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=playlist_response.get('nextPageToken')
        if next_page_token is None:
            break
    return Video_id
Video_ids=video_ids("UCZjRcM1ukeciMZ7_fvzsezQ")

#Get Video details using video id
def Video_info(Video_ids):
        Video_data=[]
        for Video_id in Video_ids:
                request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=Video_id
                )
                response = request.execute()

                for j in response['items']:
                        data=dict(Channel_id=j['snippet']['channelId'],
                                Channel_name=j['snippet']['channelTitle'],
                                Video_id=j['id'],
                                Video_name=j['snippet']['title'],
                                Video_description=j['snippet']['description'],
                                Tags=j['snippet'].get('tags'),
                                Published_date=j['snippet']['publishedAt'],
                                View_count=j['statistics']['viewCount'],
                                Like_count=j['statistics'].get('likeCount'),
                                Dislike_count=j['statistics'].get('dislikeCount'),
                                Favorite_count=j['statistics']['favoriteCount'],
                                Comment_count=j['statistics'].get('commentCount'),
                                Duration=j['contentDetails']['duration'],
                                Thumbnail=j['snippet']['thumbnails']['default']['url'],
                                Caption_status=j['contentDetails']['caption']
                        )
                        Video_data.append(data)
        return Video_data

#Get Comment details
def Comment_info(Video_ids):
    Comment=[]
    try:
        for video_id in Video_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
            response = request.execute()

            for C in response['items']:
                data=dict(Video_id=C['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_id=C['snippet']['topLevelComment']['id'],
                        Comment_text=C['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=C['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                        Comment_published_at=C['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment.append(data)
    except:
        pass
    return Comment

#Inserting channel,video and Comment details into Mongodb
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db=client["Youtube"]
col=db["Channel_detail"]

def Channel_detail(channel_id):
    C=Channel_details(channel_id)
    V=video_ids(channel_id)
    Vi=Video_info(Video_ids)
    Cmt=Comment_info(Video_ids)

    col.insert_one({"Channel_information":C,"Video_information":Vi,"Comment_information":Cmt})

    return "Uploaded successfully"

#Table Creation
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Abisha123,",
    database="youtube",
    port="5432"
)

cursor = mydb.cursor()

def Channel_table():
    try:
        drop_query=''' drop table if exists channels'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query='''create table if not exists channels(Channel_id varchar(255) primary key,
                                                            Channel_name varchar(255),
                                                            Subscription_count Bigint,
                                                            Channel_view Bigint,
                                                            Channel_description Text,
                                                            Total_videos int,
                                                            Playlist_id varchar(255))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels table is already created")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db=client["Youtube"]
    col=db["Channel_detail"]
    mongo_data =col.find()

    for data in mongo_data:
        channel_info = data['Channel_information']
        insert_query = '''INSERT INTO channels (Channel_id, Channel_name, Subscription_count, Channel_view, 
                            Channel_description,Total_videos, Playlist_id)VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        channel_values = (channel_info['Channel_id'],
                    channel_info['Channel_name'],
                    channel_info['Subscription_count'],
                    channel_info['Channel_view'],
                    channel_info['Channel_description'],
                    channel_info['Total_videos'],
                    channel_info['Playlist_id'])
        cursor.execute(insert_query, channel_values)
    mydb.commit()
Channel_table()

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Abisha123,",
    database="youtube",
    port="5432"
)

cursor = mydb.cursor()
    
def Video_table():
    try:
        drop_query=''' drop table if exists video'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query = '''create table if not exists video (Channel_id varchar(255),
                                                            Channel_name varchar(255),
                                                            Video_id VARCHAR(255) primary key,
                                                            Video_name VARCHAR(255),
                                                            Video_description TEXT,
                                                            Tags TEXT,
                                                            Published_date Timestamp,
                                                            View_count INT,
                                                            Like_count BIGINT,
                                                            Dislike_count BIGINT,
                                                            Favorite_count INT,
                                                            Comment_count INT,
                                                            Duration INTERVAL,
                                                            Thumbnail VARCHAR(255),
                                                            Caption_status VARCHAR(255)
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("video table is already created")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db=client["Youtube"]
    col=db["Channel_detail"]
    mongo_data =col.find()

    for data in mongo_data:
        video_information = data["Video_information"]
        for video_info in video_information:
            select_query = '''SELECT COUNT(*) FROM video WHERE Video_id = %s'''
            cursor.execute(select_query, (video_info['Video_id'],))
            result = cursor.fetchone()
            if result[0] == 0:
                insert_query = '''INSERT INTO Video (Channel_id, Channel_name, Video_id, Video_name, Video_description, Tags, Published_date,
                                    View_count, Like_count, Dislike_count, Favorite_count, Comment_count,
                                    Duration, Thumbnail, Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

                video_values = (
                    video_info['Channel_id'],
                    video_info['Channel_name'],
                    video_info['Video_id'],
                    video_info['Video_name'],
                    video_info['Video_description'],
                    video_info['Tags'],
                    video_info['Published_date'],
                    video_info['View_count'],
                    video_info['Like_count'],
                    video_info['Dislike_count'],
                    video_info['Favorite_count'],
                    video_info['Comment_count'],
                    video_info['Duration'],
                    video_info['Thumbnail'],
                    video_info['Caption_status']
                )
                cursor.execute(insert_query, video_values)
        mydb.commit()
Video_table()

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Abisha123,",
    database="youtube",
    port="5432"
)

cursor = mydb.cursor()
    
def Comment_table():
    try:
        drop_query=''' drop table if exists comment'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query = '''create table if not exists comment(Video_id Varchar(255),
                                Comment_id Varchar(255) primary key,
                                Comment_text text,
                                Comment_Author varchar(255),
                                Comment_published_at Timestamp
                        )'''

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Comment table is already created")

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db=client["Youtube"]
    col=db["Channel_detail"]
    mongo_data =col.find()

    for data in mongo_data:
        comment_information = data["Comment_information"]
        for comment_info in comment_information:
            select_query = '''SELECT COUNT(*) FROM comment WHERE Comment_id = %s'''
            cursor.execute(select_query, (comment_info['Comment_id'],))
            result = cursor.fetchone()
            if result[0] == 0:
                insert_comment_query = '''INSERT INTO Comment (
                                            Video_id, Comment_id, Comment_text, Comment_Author, Comment_published_at
                                        ) VALUES (%s, %s, %s, %s, %s)'''
                comment_values = (
                    comment_info['Video_id'],
                    comment_info['Comment_id'], 
                    comment_info['Comment_text'],
                    comment_info['Comment_Author'],
                    comment_info['Comment_published_at']
                )
                cursor.execute(insert_comment_query, comment_values)
        mydb.commit()
Comment_table()

with st.sidebar:
    st.sidebar.title(":red[YouTube Data Harvesting and Warehousing]")
    st.sidebar.header("Skill Takeaway")
    st.sidebar.caption("API Integration")
    st.sidebar.caption("Python Scripting")
    st.sidebar.caption("Data Collection")
    st.sidebar.caption("MongoDB")
    st.sidebar.caption("Data Management using MongoDB and SQL")

st.title(":red[Youtube Channel data Collection]")
st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/4/42/YouTube_icon_%282013-2017%29.png/640px-YouTube_icon_%282013-2017%29.png", width=50)

channel_id = st.text_input("Enter Channel ID")

def Channel_detail(channel_id):
    # Fetch channel, video, and comment details for the specified channel_id
    channel_info = Channel_details(channel_id)
    video_info = Video_info(video_ids(channel_id))
    comment_info = Comment_info(video_ids(channel_id))
    
    # Insert data into MongoDB
    col.insert_one({
        "Channel_information": channel_info,
        "Video_information": video_info,
        "Comment_information": comment_info
    })

    # Return a success message
    return "Uploaded successfully"
def mongodb(channel_id):
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db=client["Youtube"]
    col=db["Channel_detail"]
    mongo_data =col.find()
    existing_doc= col.find_one({"Channel_information.Channel_Id": channel_id})
    if existing_doc:
        st.warning("Channel details already exists")
    else:
        insert=Channel_detail(channel_id)
        st.success("Data Uploaded to MongoDB Successfully.")
def fetch_all_channel_ids():
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client["Youtube"]
    col = db["Channel_detail"]
    
    channel_ids = set()
    for doc in col.find():
        channel_info = doc.get("Channel_information")
        if channel_info:
            channel_id = channel_info.get("Channel_id")
            if channel_id:
                channel_ids.add(channel_id)
    
    return channel_ids
if st.button("Collect and store data"):
    if channel_id:
        mongodb(channel_id)
    else:
        st.warning("Please enter a YouTube Channel ID.")
        all_channel_ids = fetch_all_channel_ids()
        for channel_id in all_channel_ids:
            mongodb(channel_id)
def chan():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Abisha123,",
        database="youtube",
        port="5432"
    )
    cursor = mydb.cursor()

    cursor.execute("SELECT * FROM channels")
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    return df

def vid():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Abisha123,",
        database="youtube",
        port="5432"
    )
    cursor = mydb.cursor()

    cursor.execute("SELECT * FROM video")
    video_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(video_data, columns=column_names)
    return df

def comm():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Abisha123,",
        database="youtube",
        port="5432"
    )
    cursor = mydb.cursor()

    cursor.execute("SELECT * FROM comment")
    comment_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(comment_data, columns=column_names)
    return df

# Streamlit app
st.header(":red[YouTube Data Viewer]")

# Select the table to view
show_table = st.radio("Select the table for view", ("Channels", "Video", "Comments"))

# Display the selected table
if show_table == "Channels":
    st.subheader("Channels Table")
    df_channels = chan()
    st.dataframe(df_channels)
elif show_table == "Video":
    st.subheader("Video Table")
    df_videos = vid()
    st.dataframe(df_videos)
elif show_table == "Comments":
    st.subheader("Comments Table")
    df_comments = comm()
    st.dataframe(df_comments)

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Abisha123,",
    database="youtube",
    port="5432"
)

cursor = mydb.cursor()

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Abisha123,",
    database="youtube",
    port="5432"
)

cursor = mydb.cursor()

if question == "1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select Channel_Name as channelname,Video_name as videoname from Video'''
    cursor.execute(query1)
    mydb.commit()
    r1 = cursor.fetchall()
    df1 = pd.DataFrame(r1, columns=["Channel Name", "Video Name"])
    st.write(df1)

elif question =="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select Channel_name, count(Video_id) AS Video_Count from video group by Channel_name order by Video_Count DESC '''
    cursor.execute(query2)
    mydb.commit()
    r2 = cursor.fetchall()
    df2 = pd.DataFrame(r2, columns=["Channel Name", "Video Count"])
    st.write(df2)

elif question =="3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select Video_name, View_count, Channel_name from video order by View_count DESC limit 10'''
    cursor.execute(query3)
    mydb.commit()
    r3 = cursor.fetchall()
    df3 = pd.DataFrame(r3, columns=["Top 10 videos", "View Count", "Channel Name"])
    st.write(df3)

elif question =="4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select v.Video_name, COUNT(c.Comment_id) as Comment_Count from video v left join comment c on v.Video_id = c.Video_id group by v.Video_name'''
    cursor.execute(query4)
    mydb.commit()
    r4 = cursor.fetchall()
    df4 = pd.DataFrame(r4, columns=["Video Name","Comment Count"])
    st.write(df4)

elif question =="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''SELECT Video_name, Like_count, Channel_name from video order by Like_count desc limit 5'''
    cursor.execute(query5)
    mydb.commit()
    r5 = cursor.fetchall()
    df5 = pd.DataFrame(r5, columns=["Video Name", "Like Count","Channel Name"])
    st.write(df5)

elif question =="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select Video_name, sum(Like_count) as Total_likes, sum(Dislike_count) as Total_dislikes from video group by Video_name '''
    cursor.execute(query6)
    mydb.commit()
    r6 = cursor.fetchall()
    df6 = pd.DataFrame(r6, columns=["Video Name", "Total Likes", "Total Dislikes"])
    st.write(df6)

elif question =="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select c.Channel_name, sum(v.View_count) as Total_views from channels c 
                join video v on c.Channel_id = v.Channel_id group by c.Channel_name'''
    cursor.execute(query7)
    mydb.commit()
    r7 = cursor.fetchall()
    df7 = pd.DataFrame(r7, columns=["Channel Name", "Total Views"])
    st.write(df7)

elif question =="8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select distinct channels.Channel_name from channels join video on channels.Channel_id = video.Channel_id 
            where EXTRACT(YEAR from video.Published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    r8 = cursor.fetchall()
    df8 = pd.DataFrame(r8, columns=["Channel Name"])
    st.write(df8)

elif question =="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channels.Channel_name, AVG(EXTRACT(epoch from video.Duration)) as average_duration_seconds from channels 
            join video on channels.Channel_id = video.Channel_id group by channels.Channel_name'''
    cursor.execute(query9)
    mydb.commit()
    r9 = cursor.fetchall()
    df9 = pd.DataFrame(r9, columns=["Channel Name", "Average duration of Videos"])
    st.write(df9)

elif question =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query = '''select channels.Channel_name, video.Video_name, count(comment.Comment_id) as comment_count from channels join video on channels.Channel_id = video.Channel_id 
                left join comment on video.Video_id = comment.Video_id group by channels.Channel_name, video.Video_name order by comment_count desc limit 10'''
    cursor.execute(query)
    mydb.commit()
    r = cursor.fetchall()
    df = pd.DataFrame(r, columns=["Channel Name","Video Title","No of Comments"])
    st.write(df)