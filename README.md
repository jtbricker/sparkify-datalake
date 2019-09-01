# Sparkify Datalake

## Purpose
The purpose of this project is to create a pipeline that is able to 
extract data from log files, transform the data to match required formats,
and load the transformed data into database tables to allow for
efficient data analysis by the music startup, Sparkify.

### Business Value
Sparkify expects to write queries against this database to gain insights
into the behavior and trends of its users in order to better serve them 
in the future.

## Database Schema
The database tables are laid our in a star schema, with central songplays fact table:

### songplays
`songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

and 4 supporting dimension tables, users, songs, artists, and time:

### users
`user_id, first_name, last_name, gender, level`

### songs
`song_id, title, artist_id, year, duration`

### artists
`artist_id, name, location, lattitude, longitude`

### time
`start_time, hour, day, week, month, year, weekday`

The star schema design allows for efficient querying of the songplay data across many different dimensons
(see [Example Queries](#Example\ Queries))

## Example Queries

Question: How many users are listening to the Backstreet Boys on Thursdays?

```SQL
SELECT COUNT(*) 
FROM songplays sp
JOIN artists a on sp.artist_id = a.artist_id
JOIN time t on sp.start_time = t.start_time
WHERE a.name = 'Backstreet Boys' AND  t.weekday = 3
```

Question: Who are the top 5 users with the most listening time?

```SQL
SELECT TOP 5 u.*, SUM(s.duration) as total_time
FROM songplays sp
JOIN artists a on sp.artist_id = a.artist_id
JOIN users u on sp.user_id = u.user_id
GROUP BY u.user_id
ORDER BY total_time DESC
```

## Setup
1. This project requires spark to be setup on your machine.  Rather than doing this, I opted to use the publicly available `pyspark-notebook` docker image.  You can start this image by running the following command:
    ```bash
        docker run -it -v "C:\<path>\<to>\<this>\<repo>:/home/jovyan/work/sparkify-datalake" -p 8888:8888 jupyter/pyspark-notebook
    ```
    this starts the docker image and maps `C:\<path>\<to>\<this>\<repo>` on your local machine to `/home/jovyan/work/sparkify-datalake` on the docker container.
    
1. Create a `dl.cfg` file with the entries for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (under the `AWS` config category) taken from your AWS account (do not check these values into source control).
    1. You can also run this project locally by passing "LOCAL" to the main method of `etl.py`
