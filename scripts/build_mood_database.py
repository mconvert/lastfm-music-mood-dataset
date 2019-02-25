import os
import sys
import sqlite3
import time

"""	DESCRIPTION:
	Derive a dataset with mood labels on the Million Song Dataset 
	using social tags provided by Lastfm.
	Store it in a new database file, with multiclass labels for each track.
"""


def initiate_db_connection(db_file):
	""" Returns either a db connection object or None.
		Takes a database filename as argument.
	"""
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Exception as e:
		print(e) 
 
	return None




def attach_tracks_info_to_connection(conn, database):
	""" Attaches the given database to the connection object.
	"""

	sql = """ ATTACH "%s" AS attached; """ % database
	try:
		conn.execute(sql)
	except Exception as e:
		print("%s could not be attached!")




def show_db_preview(conn):
	""" Shows a preview of the database connection object.
		Used only for testing purposes.
	"""

	# Show the columns and first 3 entries of the table "tag"
	sql = "SELECT * FROM tags;"
	res = conn.execute(sql)
	data = res.fetchall()
	for i in range(3):
		print data[i]

	print "-----------"

	# Show the columns and first 3 entries of the table "tid"
	sql = "SELECT * FROM tids;"
	res = conn.execute(sql)
	data = res.fetchall()
	for i in range(3):
		print data[i]

	print "-----------"

	# Show the columns and first 3 entries of the table "tid_tag"
	sql = "SELECT * FROM tid_tag;"
	res = conn.execute(sql)
	data = res.fetchall()
	for i in range(3):
		print data[i]

	print "-----------"

	# Show the columns and first 3 entries of the attached table "attached.tracks"
	sql = "SELECT * FROM attached.tracks;"
	res = conn.execute(sql)
	data = res.fetchall()
	for i in range(3):
		print data[i]
		
	


def get_tracks_with_at_least_one_tag(conn):
	""" Returns a list of tid for all tracks with at least one tag.
	"""

	sql = "SELECT tid FROM tids"
	res = conn.execute(sql)
	data = res.fetchall()
	tagged_tracks = map(lambda x: x[0], data)
	
	return tagged_tracks



def get_keywords_for_mood_group(moodgroup):
	""" Returns the list of keywords of the specified mood group, or None. 
	"""

	keywords = {
			'g1': ['excitement', 'exciting', 'exhilarating', 'thrill', 'ardor', 'stimulating', 'thrilling', 'titillating'],
			'g2': ['upbeat', 'gleeful', 'high spirits', 'zest', 'enthusiastic', 'buoyancy', 'elation', 'mood: upbeat'],
			'g5': ['happy', 'happiness', 'happy songs', 'happy music', 'glad', 'mood: happy'],
			'g6': ['cheerful', 'cheer up', 'festive', 'jolly', 'jovial', 'merry', 'cheer', 'cheering', 'cheery', 'get happy', 'rejoice', 'songs that are cheerful', 'sunny'],
			
			'g8': ['brooding', 'contemplative', 'meditative', 'reflective', 'broody', 'pensive', 'pondering', 'wistful'],
			'g12': ['calm', 'comfort', 'quiet', 'serene', 'mellow', 'chill out', 'calm down', 'calming', 'chillout', 'comforting', 'content', 'cool down', 'mellow music', 'mellow rock', 'peace of mind', 'quietness', 'relaxation', 'serenity', 'solace', 'soothe', 'soothing', 'still', 'tranquil', 'tranquility'],
			
			'g15': ['sad', 'sadness', 'unhappy', 'melancholic', 'melancholy', 'feeling sad', 'mood: sad - slightly', 'sad song'],
			'g16': ['depressed', 'blue', 'dark', 'depressive', 'dreary', 'gloom', 'darkness', 'depress', 'depression', 'depressing', 'gloomy'],
			'g17': ['grief', 'heartbreak', 'mournful', 'sorrow', 'sorry', 'doleful', 'heartache', 'heartbreaking', 'heartsick', 'lachrymose', 'mourning', 'plaintive', 'regret', 'sorrowful'],

			'g25': ['angst', 'anxiety', 'anxious', 'jumpy', 'nervous', 'angsty'],
			'g28': ['anger', 'angry', 'choleric', 'fury', 'outraged', 'rage', 'angry music'],
			'g29': ['aggression', 'aggressive'],

			'g32': ['romantic', 'romantic music'],
			'g14': ['dreamy'],
			'g9': ['confident', 'encouraging', 'encouragement', 'optimism', 'optimistic'],
			'g7': ['desire', 'hope', 'hopeful', 'mood: hopeful'],
			'g11': ['earnest', 'heartfelt'],
			'g31': ['pessimism', 'cynical', 'pessimistic', 'weltschmerz', 'cynical/sarcastic']	
		}

	try:
		result = keywords[moodgroup]
		return result
	except:
		print("Invalid mood group!")

	return None



def get_mood_groups():
	return ['g1', 'g2', 'g5', 'g6', 'g7', 'g8', 'g9', 'g11', 'g12', 'g14', 'g15', 'g16', 'g17', 'g25', 'g28', 'g29', 'g31', 'g32']




def create_mood_labels_table(conn):
	""" Creates the mood_labels table in the given db-connection (tracks_mood.db).
	"""

	sql = """ CREATE TABLE IF NOT EXISTS mood_labels (
				tid TEXT PRIMARY KEY,
				title TEXT,
				artist TEXT,
				g1 integer,
				g2 integer,
				g5 integer,
				g6 integer,
				g7 integer,
				g8 integer,
				g9 integer,
				g11 integer,
				g12 integer,
				g14 integer,
				g15 integer,
				g16 integer,
				g17 integer,
				g25 integer,
				g28 integer,
				g29 integer,
				g31 integer,
				g32 integer
			); """

	try:
		c = conn.cursor()
		c.execute(sql)
	except Exception as e:
		print(e)




def insert_row_in_database(conn, row):
	""" Inserts the given already formatted row into the database (tracks_mood.db).	
	"""

	sql = """ INSERT INTO mood_labels(tid,title,artist,g1,g2,g5,g6,g7,g8,g9,g11,g12,g14,g15,g16,g17,g25,g28,g29,g31,g32) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); """

	try:
		c = conn.cursor()
		c.execute(sql, (row))
		conn.commit()
	except Exception as e:
		print(e)



def keyword_is_in_title_or_artist(conn, trackid, keyword):
	""" Compares the given keyword to the "artist" and "title" of the given track_id.
		Returns True or False.
	"""

	try:
		sql = """ SELECT title,artist FROM attached.tracks WHERE track_id=?; """
		res = conn.execute(sql, (trackid,))
		data = res.fetchall()
		title, artist = data[0][0], data[0][1]
	except Exception as e:
		print("Could not access attached.tracks table to check for keyword presence in title/artist!")


	if (keyword in title.lower()) or (keyword in artist.lower()):
		return True
	else:
		return False 





def check_if_tagged_same_word_twice(conn, trackid, tags, moodtags):
	""" Checks whether the track satisfies the condition on the given moodgroup by checking its tags in the db.
		Returns True or False, whether it belongs to the group or not.
	"""		

	condition = False
	if not (len(tags)==0):	# if track has no tag, return False
		minval = min(tags.itervalues())
		for kw in moodtags:
			if (kw in tags) and not keyword_is_in_title_or_artist(conn, trackid, kw): # if the keyword is in the title/artist or the keyword does not appear in tags, skip to the next keyword
				normalized_count = tags[kw]
				if normalized_count > minval: 	# if the keyword is used twice or more, the track fits in the moodgroup, so break the loop
					condition = True
					break
	
	return condition




def check_if_tagged_two_keywords(conn, trackid, tags, moodtags):
	""" Checks whether the track satisfies the condition on the given moodgroup by checking its tags in the db. 
		Returns True or False, whether it belongs to the group or not.
	"""

	# Remove the keywords that also appear in title/artist from the mood keyword list 
	for kw in moodtags:
		if keyword_is_in_title_or_artist(conn, trackid, kw):
			moodtags.remove(kw)

	# Check if the set of mood keywords and the set of tags intersect with more than two elements
	condition = False
	if (len(set(tags.keys()).intersection(set(moodtags))) >= 2):
		condition = True


	return condition





def check_if_conditions_satisfied(conn, trackid, moodgroup):
	""" Gives the final judgment on the two conditions.
		Returns True or False.
	"""

	# Load mood keywords for given group
	mood_tags = get_keywords_for_mood_group(moodgroup)

	# Get tags and normalized count for the given track
	sql = "SELECT tags.tag, tid_tag.val FROM tid_tag, tids, tags WHERE tags.ROWID=tid_tag.tag AND tid_tag.tid=tids.ROWID and tids.tid=?"
	res = conn.execute(sql, (trackid,))
	data = res.fetchall()
	
	tags = dict(map(lambda x: [x[0], x[1]], data))

	# Check the two conditions described in MIREX2009
	conditions_hold = False
	if check_if_tagged_same_word_twice(conn, trackid, tags, mood_tags) or check_if_tagged_two_keywords(conn,trackid, tags, mood_tags):
		conditions_hold = True
	

	return conditions_hold	




def check_if_mood_row_all_zero(row):
	""" Checks if the given mood row is all zero, in which case the data example serves no purpose in our study.
		Returns True or False
	"""
	if sum(row) == 0:
		return True
	else:
		return False



def fetch_title_artist(conn, trackid):
	""" Return athe title and artist of the given track ID.
	"""

	try:
		sql = """ SELECT title,artist FROM attached.tracks WHERE track_id=?; """
		res = conn.execute(sql, (trackid,))
		data = res.fetchall()
		title, artist = data[0][0], data[0][1]
	except Exception as e:
		print("Could not access attached.tracks table to check for keyword presence in title/artist!")


	return title, artist





def test__get_mood_db_data(conn):
	sql = "SELECT tid FROM mood_labels"
	res = conn.execute(sql)
	data = res.fetchall()
	tracks = map(lambda x: x[0], data)
	
	return tracks






if __name__ == "__main__":


	# Specify db files
	dbroot = "../db/"
	tracks_info = dbroot + "tracks.db"
	tags_db = dbroot + "lastfm_tags.db"
	mood_db = dbroot + "tracks_mood.db"


	# Initialize connection to databases
	tags_conn = initiate_db_connection(tags_db)
	mood_conn = initiate_db_connection(mood_db)


	# Attach the track_info db to the tags db
	attach_tracks_info_to_connection(tags_conn, tracks_info)


	# List of tracks with at least one tag
	tagged_tracks = get_tracks_with_at_least_one_tag(tags_conn)
	

	# ------------------------------------------------------------------------------
	# Creating the database

	# Create the mood_labels table in the tracks_mood.db database
	create_mood_labels_table(mood_conn)


	# Insert rows to the mood database
		# For each track
		# Construct row
			# For each mood group
				# Check the conditions for a given track ID
					# Check if at least two keywords
						# Check that keyword not in title or artist
					# Check if same keyword twice
						# Check that keyword not in title or artist
				# If conditions are met, the current mood column gets value 1
		# Check if columns all 0 -> discard
		# Otherwise insert row

	start = time.time()

	mood_groups = get_mood_groups()
	
	for track in tagged_tracks:
		row = []
		for mood_group in mood_groups:
			if check_if_conditions_satisfied(tags_conn, track, mood_group):
				row.append(1)
			else:
				row.append(0)

		if not check_if_mood_row_all_zero(row):
			title, artist = fetch_title_artist(tags_conn, track)
			row.insert(0, artist)
			row.insert(0, title)
			row.insert(0, track)
			insert_row_in_database(mood_conn, row)
	
	end = time.time()
	print(end-start)


	# Close the database connections
	tags_conn.close()
	mood_conn.close()