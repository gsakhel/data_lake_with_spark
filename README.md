# Data Lake on AWS
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
# Fact Table
    songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dimension Tables
    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

# Contents
    etl.py reads data from S3, processes that data using Spark, and writes them back to S3
    dl.cfgcontains your AWS credentials
    README.md provides discussion on process and decisions

# Purpose
The purpose of this project is to provide a structured database on a distributed cloud cluster. This will allow for analysis of large datasets.