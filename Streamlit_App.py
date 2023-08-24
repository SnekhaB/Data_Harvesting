
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# Connect to MySQL Database
db_engine = create_engine("mysql+pymysql://root:My27$ql23@localhost/Youtube")


# Function to fetch and display data as a table
def display_query_results(query):
    data = pd.read_sql(query, con=db_engine)
    st.dataframe(data)

# Main Streamlit app
def main():
    st.title("MySQL Query Results")

    # Question 1
    st.header("1. Names of all videos and their corresponding channels")
    query1 = """SELECT Video.Title, Channels.Channel_Name 
    FROM Video 
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id 
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id;"""
    display_query_results(query1)

    # Question 2
    st.header("2. Channels with the most number of videos and their counts")
    query2 = """SELECT Channels.Channel_Name, COUNT(Video.Video_Id) AS Video_Count 
    FROM Channels 
    JOIN Playlist ON Channels.Channel_Id = Playlist.Channel_Id 
    JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id 
    GROUP BY Channels.Channel_Name 
    ORDER BY Video_Count 
    DESC LIMIT 1;"""
    display_query_results(query2)

    # Question 3
    st.header("3. Top 10 most viewed videos and their respective channels")
    query3 = """SELECT Video.Title, Channels.Channel_Name, Video.View_count
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    ORDER BY Video.View_count DESC
    LIMIT 10;"""
    display_query_results(query3)

    #Question 4
    st.header("4. Number of Comments and Corresponding Video Names")
    query4 = """SELECT Video.Title, COUNT(Comment.comment_id) AS Comment_Count
    FROM Video
    LEFT JOIN Comment ON Video.Video_Id = Comment.Video_Id
    GROUP BY Video.Video_Id, Video.Title;"""
    display_query_results(query4)

    #Question 5 
    st.header("5. Videos with Highest Number of Likes and Corresponding Channels")
    query5 = """SELECT Video.Title, Channels.Channel_Name, Video.Like_count
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    ORDER BY Video.Like_count DESC
    LIMIT 10;"""
    display_query_results(query5)

    #Question 6
    st.header("6. Total Likes and Dislikes for Each Video")
    query6 = """SELECT Video.Title, SUM(Video.Like_count) AS Total_Likes, SUM(Video.Dislike_count) AS Total_Dislikes
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    GROUP BY Video.Title;"""
    display_query_results(query6)

    #Question 7
    st.header("7. Total Views for Each Channel")
    query7 = """SELECT Channels.Channel_Name, SUM(Video.View_count) AS Total_Views
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    GROUP BY Channels.Channel_Name;"""
    display_query_results(query7)

    #Question 8
    st.header("8. Channels with Videos Published in 2022")
    query = """SELECT DISTINCT Channels.Channel_Name
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    WHERE YEAR(Video.Published_date) = 2022;"""
    display_query_results(query8)

    #Question 9
    st.header("9. Average Duration of Videos in Each Channel")
    query9 = """SELECT Channels.Channel_Name, AVG(Video.Duration) AS Average_Duration
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    GROUP BY Channels.Channel_Name;"""
    display_query_results(query9)

    #Question 10
    st.header("10. Videos with Highest Number of Comments and Corresponding Channels")
    query10 = """"SELECT Video.Title, Channels.Channel_Name, Video.Comment_count
    FROM Video
    JOIN Playlist ON Video.Playlist_Id = Playlist.Playlist_id
    JOIN Channels ON Playlist.Channel_Id = Channels.Channel_Id
    ORDER BY Video.Comment_count DESC
    LIMIT 10;"""
    display_query_results(query10)


if __name__ == "__main__":
    main()




