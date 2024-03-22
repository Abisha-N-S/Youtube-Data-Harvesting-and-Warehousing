#Youtube-Data-Harvesting-and-Warehousing
Introduction

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application. This project helps to extract the particular youtube channel data by using youtube API key, processes the data, stores it in the MongoDB database with the help of collect and store button and migrated the data into postgres SQL as a table form and displayed in the streamlit. 

Table of content

1.	Key technologies and skills
2.	Features
3.	YouTube API
4.	MongoDB
5.	Postgres
6.	Data Analysis

Key technologies and Skills

  •	pip install google-api-python-client
  
  •	pip install pymongo
  
  •	pip install psycopg2
  
  •	pip install pandas
  
  •	pip install streamlit

Feature

•Retrieving channel details from the YouTube using youtube API.

•Then store the whole data to MongoDB database as a data lake. 

•After that the stored data migrated to the SQL in a structured form. 

•Then displayed into streamlit.

YouTube API:
YouTube API helps to retreive channel, video and comment details. Here i used google api client to make request to the API.

MongoDB:
Once the data collected by API then it stored into MongoDB data lake.
MongoDB is a good choice for data lake because it handles unstructured and semi-structured data perfectly.

Postgres:
After collected the multiple data, it will migrate the whole data into SQL. Here I have used postgres SQL.
POstgres SQL is open-source, advanced and high scalable database management system(DBMS) known for its reliability and extensive features.
It provides the platform for storing and managing structured data.

Data Analysis:

This project provides the comprehensive data analysis capability using Streamlit

  •Channel Analysis: It includes insights on playlists, videos, subscribers, views, comments and durations. Gain a deep understanding of channels performance and audience engagement through detailed visualization and summaries.

  •Video Analysis: This analysis focus on views, likes, dislikes, comments, published date and duration. Leverage visual representations and metrics to extract valuable insights from individual video.

The Streamlit app provides an intuitive interface to interact with the charts and explore the data visually. Users can customize the visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis.
With the combined capabilities of Streamlit, the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.

Contact

📧 Email: abishans2012@gmail.com 

For any further inqueries,feel free to reach out.
