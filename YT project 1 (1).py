pip install google-api-python-client


from googleapiclient.discovery import build
import pandas as pd
import json

api_key='AIzaSyAPhY0wkL9VelO5UpWWYnDhcrOH_9fPgoU'


channel_id_1 ='UC2TvVGvdIBKB5FKLzhIsN6A'
channel_id_2='UCRdraUD7qnxi-XNqA0yVKmg'
channel_id_3='UCvyZS6W6zMJCZBVzF-Ei6sw'
channel_id_4='UCP7WmQ_U4GB3K51Od9QvM0w'
channel_id_5='UC4vgd34lbz4rMekxNfmxOIg'
channel_id_6='UCjvd2JmIWGsEWPmLifUS4PA'
channel_id_7='UCTxfYLmM82aMCovQexkRxFQ'
channel_id_8='UCcOBk2-PfulQhylDT3UvtjQ'
channel_id_9='UCqW8jxh4tH1Z1sWPbkGWL4g'
channel_id_10='UCMyi2FZAUNLFDUE8ZIf7YOQ'


youtube = build('youtube','v3',developerKey=api_key)


def get_channel_stats(youtube,channel_id_10):
    
    channel_details =[]
    
    request = youtube.channels().list(
                part = 'snippet,contentDetails,Statistics',
                id =channel_id_10)
    response = request.execute()
    
    data = dict(
                Channel_Id= response['items'][0]['id'],
                Channel_Name= response['items'][0]['snippet']['title'],
                Subscription_Count= int(response['items'][0]['statistics']['subscriberCount']),
                Channel_Views= int(response['items'][0]['statistics']['viewCount']),
                Channel_Description= response['items'][0]['snippet']['description'],
                #Playlist_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
               )
    channel_details.append(data)
    
    return pd.DataFrame(channel_details)

channel_data = get_channel_stats(youtube,channel_id_10)
display(channel_data)


def get_channel_stats1(youtube,channel_id_10):
    
    channel_details1 =[]
    
    request = youtube.channels().list(
                part = 'snippet,contentDetails,Statistics',
                id =channel_id_10)
    response = request.execute()
    
    data = dict(Playlist_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                Channel_Id= response['items'][0]['id']
               )
    channel_details1.append(data)
    
    return pd.DataFrame(channel_details1)

channel_data1 = get_channel_stats1(youtube,channel_id_10)
display(channel_data1)



def get_video_ids(youtube,playlist_id):
    
    request = youtube.playlistItems().list(
                part = 'contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    
    video_ids=[]
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    
    
    return video_ids
playlist_id = 'UUMyi2FZAUNLFDUE8ZIf7YOQ'
Video_Ids=get_video_ids(youtube,playlist_id)  
Video_Ids


def get_video_details(youtube, Video_Ids):
    all_video_details = []

    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=Video_Ids
    )
    response = request.execute()

    for video in response['items']:
        video_stats = dict(
            Title=video['snippet']['title'],
            Id=video['id'],
            Description=video['snippet']['description'],
            Tags=video['snippet'].get('tags', []),
            published_date=video['snippet']['publishedAt'],
            Views=video['statistics']['viewCount'],
            Likes=video['statistics'].get('likeCount', 0),
            Dislikes=video['statistics'].get('dislikeCount', 0),
            Favoritecount=video['statistics'].get('favoriteCount', 0),
            Comments=video['statistics'].get('commentCount', 0),
            Duration=video['contentDetails']['duration'],
            Thumbnail=video['snippet']['thumbnails']['default']['url'],
            Caption=video['contentDetails']['caption'],
            Playlist_Id=playlist_id
        )
        all_video_details.append(video_stats)

    return all_video_details
playlist_id = 'UUMyi2FZAUNLFDUE8ZIf7YOQ'

video_details = get_video_details(youtube, Video_Ids)
video_details


video_details1 = pd.DataFrame(video_details)
video_details1['Title'] = video_details1['Title'].str.replace("'", "''")
video_details1['Thumbnail'] = video_details1['Thumbnail'].str.replace("'", "''")
video_details1['Playlist_Id'] = video_details1['Playlist_Id'].str.replace("'", "''")
video_details1['Description'] = video_details1['Description'].str.replace("'", "''")
video_details1['published_date']=pd.to_datetime(video_details1['published_date']).dt.date
video_details1['Likes']=pd.to_numeric(video_details1['Likes'])
video_details1['Views']=pd.to_numeric(video_details1['Views'])
video_details1['Dislikes']=pd.to_numeric(video_details1['Dislikes'])
video_details1['Comments']=pd.to_numeric(video_details1['Comments'])
video_details1['Duration'] = pd.to_timedelta(video_details1['Duration'])
video_details1


def get_all_comments(youtube, Video_Ids):
    all_comment_detail = []

    for video_id in Video_Ids:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            textFormat='plainText',
            maxResults=100
        )
        response = request.execute()

        for item in response['items']:
            comment_details = dict(
                comment_id=item['id'],
                comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_published_at=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                video_id = video_id
            )
            all_comment_detail.append(comment_details)

    return pd.DataFrame(all_comment_detail)


comments = get_all_comments(youtube, Video_Ids)
all_Comments = pd.DataFrame(comments)
all_Comments['comment_published_at']=pd.to_datetime(all_Comments['comment_published_at']).dt.date
all_Comments



##Converting into dict to push the data into mongodb
channel_json=channel_data.to_dict(orient='records')
Video_josn = video_details1.to_dict(orient='records')
Comment_json = all_Comments.to_dict(orient='records')


pip install pymongo



import pymongo

##Establishing the connection
client = pymongo.MongoClient('mongodb+srv://Snekha:A9yXiG44VlPFmGn9@cluster0.m2b5rdr.mongodb.net/?retryWrites=true&w=majority')
database_name = client['Youtube_Data']
Channels =database_name['Channel_10']

#inserting the channels data
inserting_channel_data = Channels.insert_many(channel_json)

import datetime

#converting
for item in Video_josn:
    item['published_date'] = datetime.datetime(item['published_date'].year,
                                               item['published_date'].month,
                                               item['published_date'].day)

#inserting the videos data
inserting_videos_data = Channels.insert_many(Video_josn)
#inserting the comments 
inserting_comments = Channels.insert_many(Comment_json)
#closing the connection
client.close()



#Installing pymysql package
pip install pymysql

pip install mysql-connector-python


import numpy as np
import pandas as pd
import pymysql

#Establishing the connection
myconnection = pymysql.connect(host='127.0.0.1',user='root',password = 'My27$ql23')
cur = myconnection.cursor()

##creating the database
cur.execute('create database Youtube')

#Establishing the connection after the databse created
myconnection = pymysql.connect(host='127.0.0.1',user='root',password = 'My27$ql23',database='Youtube')
cur = myconnection.cursor()

##creating the Channel table
cur.execute('create table Channels(Channel_Id varchar(30) PRIMARY KEY,Channel_Name varchar(20),Subscription_Count int,Channel_Views int,Channel_Description text)')
##creating the Playlist table
cur.execute('create table Playlist(Playlist_id varchar(50) PRIMARY KEY,Channel_Id varchar(30),FOREIGN KEY (Channel_Id) REFERENCES Channels(Channel_Id))')
##creating the Video table
cur.execute('create table Video(Title varchar(100),Video_Id varchar(50) PRIMARY KEY,Description text,Tags varchar(500),Published_date datetime,View_count int,Like_count int,Dislike_count int,Favorite_count int,Comment_count int,Duration int,Thumbnail varchar(255),Caption_status varchar(255),Playlist_Id varchar(50),FOREIGN KEY (Playlist_Id) REFERENCES Playlist(Playlist_Id))')
##creating the Comment table
cur.execute('create table Comment(comment_id varchar(50) PRIMARY KEY,comment_text text,comment_author varchar(100),comment_published_date datetime,Video_Id VARCHAR(50),FOREIGN KEY (Video_Id) REFERENCES Video(Video_Id))')


##Inserting the values into colums

sql_channels= "insert into Channels(Channel_Id,Channel_Name,Subscription_Count,Channel_Views,Channel_Description) values (%s,%s,%s,%s,%s)"
for i in range(0,len(channel_data)):
    cur.execute(sql_channels,tuple(channel_data.iloc[i]))
    myconnection.commit()



sql_playlist = "insert into Playlist(Playlist_id,Channel_Id) values (%s,%s)"
for i in range(0,len(channel_data1)):
    cur.execute(sql_playlist,tuple(channel_data1.iloc[i]))
    myconnection.commit()


sql_Video = "INSERT INTO Video(Title,Video_Id,Description,Tags,Published_date,View_count,Like_count,Dislike_count,Favorite_count,Comment_count,Duration,Thumbnail,Caption_status,Playlist_Id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Title=VALUES(Title), Description=VALUES(Description), Tags=VALUES(Tags), Published_date=VALUES(Published_date), View_count=VALUES(View_count), Like_count=VALUES(Like_count), Dislike_count=VALUES(Dislike_count), Favorite_count=VALUES(Favorite_count), Comment_count=VALUES(Comment_count), Duration=VALUES(Duration), Thumbnail=VALUES(Thumbnail), Caption_status=VALUES(Caption_status), Playlist_Id=VALUES(Playlist_Id)"

def convert_tags_to_string(tags_list):
    return ','.join(tags_list)

for i in range(len(video_details1)):
    tags_str = convert_tags_to_string(video_details1.iloc[i]['Tags'])
    duration_seconds = int(video_details1.iloc[i]['Duration'].total_seconds())
    video_ch_row = (
        video_details1.iloc[i]['Title'],
        video_details1.iloc[i]['Id'],
        video_details1.iloc[i]['Description'],
        tags_str,
        video_details1.iloc[i]['published_date'],
        video_details1.iloc[i]['Views'],
        video_details1.iloc[i]['Likes'],
        video_details1.iloc[i]['Dislikes'],
        video_details1.iloc[i]['Favoritecount'],
        video_details1.iloc[i]['Comments'],
        duration_seconds,
        video_details1.iloc[i]['Thumbnail'],
        video_details1.iloc[i]['Caption'],
        video_details1.iloc[i]['Playlist_Id']
    )

    try:
        cur.execute(sql_Video, video_ch_row)
        myconnection.commit()
    except Exception as e:
        print("Error:", e)
        myconnection.rollback()


sql_comment = "INSERT INTO Comment(comment_id, comment_text, comment_author, comment_published_date, Video_Id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE comment_text=VALUES(comment_text), comment_author=VALUES(comment_author), comment_published_date=VALUES(comment_published_date), Video_Id=VALUES(Video_Id)"
for i in range(0,len(all_Comments)):
    cur.execute(sql_comment,tuple(all_Comments.iloc[i]))
    myconnection.commit()


myconnection.close()





