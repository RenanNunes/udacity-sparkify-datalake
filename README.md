# Sparkify

## Context
Sparkify is a startup that has music streaming app and is interested in moving their data warehouse into a data lake. The data is already on S3 and, with this Spark ETL, the data is transformed and sent back to another s3 bucket. The ETL generate dimensional tables that will allow their analytics team to find new insights about their users.

# Database schema
The database schema is based on the Star Schema, with the `songplays` table as the fact table and the others as the dimensions (`users`, `songs`, `artists`, `time`). The information with each table stores is presented bellow:
- songplays: records in log data associated with song plays i.e. records with page NextSong
  - columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- users: users in the app
  - columns: user_id, first_name, last_name, gender, level
- songs: songs in music database
  - columns: song_id, title, artist_id, year, duration
- artists: artists in music database
  - columns: artist_id, name, location, latitude, longitude
- time: timestamps of records in songplays broken down into specific units
  - columns: start_time, hour, day, week, month, year, weekday

