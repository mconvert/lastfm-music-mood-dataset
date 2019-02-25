import sqlite3
import csv 

dbfile = 'tracks_mood.db'

conn = sqlite3.connect(dbfile)
conn.text_factory = str # ...
cur = conn.cursor()
data = cur.execute("SELECT * FROM mood_labels")

with open('../csv/tracks_mood.csv', 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(['tid', 'title', 'artist', 'g1', 'g2', 'g5', 'g6', 'g7', 'g8', 'g9', 'g11', 'g12', 'g14', 'g15', 'g16', 'g17', 'g25', 'g28', 'g29', 'g31', 'g32'])
    writer.writerows(data)