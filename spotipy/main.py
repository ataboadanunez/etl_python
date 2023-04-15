# simple python code to donwload data from Spotify API
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
# load configuration settings from a test Spotify App
# to create a custom App: https://developer.spotify.com/dashboard
from config import CLIENT_ID, CLIENT_SECRET, SPOTIPY_REDIRECT_URI

from IPython import embed


# constants definition
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
scope = "user-read-recently-played"

# access to Spotify using spotify authentication manager
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,
																							 client_secret=CLIENT_SECRET,
																							 redirect_uri=SPOTIPY_REDIRECT_URI,
																							 scope=scope))

def extract_data(dt, limit=10):
	"""
		Extracts data of recently played tracks using Spotipy
		Args:
			- dt (datetime): date to query  
			- limit (int): number of elements to query
	"""

	# round date converted to timestamp (sec) and convert to miliseconds
	date2ms = int(dt.timestamp()) * 1000
	r = sp.current_user_recently_played(limit=limit, after=date2ms)

	return r

def validate_data(df : pd.DataFrame) -> bool:
	"""
		Perform checks on data before continue processing
	
	"""

	# Check if dataframe contains data
	if df.empty:
		print("No songs were downloaded!")
		return False

	# Primary Key check (duplicates)
	if pd.Series(df['played_at']).is_unique:
		pass
	else:
		raise Exception("Primary Key is violated. Found duplicated data!")

	# Check for nulls
	if df.isnull().values.any():
		raise Exception("DataFrame contains null data!")

	return True

if __name__ == "__main__":

		
		# Convert time to Unix timestamp in milliseconds      
		today = datetime.datetime.now()
		yesterday = today - datetime.timedelta(days=1)
		yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
		
		data = extract_data(yesterday, 50)


		song_names = []
		artist_names = []
		played_at_list = []
		timestamps = []

		# Extracting only the relevant bits of data from the dictionary object      
		for song in data["items"]:
				song_names.append(song["track"]["name"])
				artist_names.append(song["track"]["album"]["artists"][0]["name"])
				played_at_list.append(song["played_at"])
				timestamps.append(song["played_at"][0:10])
				
		# Prepare a dictionary in order to turn it into a pandas dataframe below       
		song_dict = {
				"song_name" : song_names,
				"artist_name": artist_names,
				"played_at" : played_at_list,
				"timestamp" : timestamps
		}

		df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

		# do the checks on current data frame
		if validate_data(df):
			print("List of recently played songs: ")
			print(df.to_markdown())

		# Load data into a database 
		# first, create an engine using ORM SQLAlchemy
		engine = sqlalchemy.create_engine(DATABASE_LOCATION)
		# initiate a connection to the database
		conn = sqlite3.connect('my_played_tracks.sqlite')
		# now, create a cursor (pointer that allows us to refer to specific rows in a DB)
		cursor = conn.cursor()

		# create a new table for our data
		sql_query = """
		CREATE TABLE IF NOT EXISTS my_played_tracks(
				song_name VARCHAR(200),
				artist_name VARCHAR(200),
				played_at VARCHAR(200),
				timestamp VARCHAR(200),
				CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
		)
		"""

		cursor.execute(sql_query)
		print("DataBase opened successfully!")

		try:
			df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
		except:
			print("Data already exists in the database")

		conn.close()
		print("Close DataBase successfully")

		embed()
