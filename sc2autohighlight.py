import psycopg2
import sys
import operator
import os


# Usage python highlights.py <start time> <filename>


# 03:58:43.91
durration = 238 # minutes


# Connect to an existing database
start_time = int(sys.argv[1]) * 1000
file_name = sys.argv[2]
print "Start time {0}".format(start_time)
conn = psycopg2.connect("dbname=twitch user=twitch")

# Open a cursor to perform database operations
cur = conn.cursor()

current_time = start_time
minutes = {}

for minute in xrange(durration):
   current_time_next = current_time + 60000
   cur.execute("SELECT message FROM chat_log where channel = '#gsl' and date > {0} and date < {1}".format(current_time, current_time_next))
   chat_lines = cur.fetchall()
   minutes[minute] = len(chat_lines)
   current_time = current_time_next
  
  

sorted_minutes = sorted(minutes.iteritems(), key = operator.itemgetter(1))
print sorted_minutes[-1:-10:-1]

for k, v in sorted_minutes[-1:-10:-1]:
   print "Minute: {0}, Chats {1}".format(k, v)

for k, v in sorted_minutes[-1:-10:-1]:
   minute = k
   basename = os.path.basename(file_name)[:-4] # remove extension
   
   print "avconv -i {0} -ss 00:{1}:00 -t 00:01:00 -strict experimental -async 1 /tmp/{2}-{3}.mp4".format(file_name,minute,basename,minute)




#ffmpeg -i movie.mp4 -ss 00:00:03 -t 00:00:08 -async 1 cut.mp4

#
#cur.execute("SELECT date FROM chat_log where channel = '#gsl' and date > {0}".format(start_time))
#seconds = cur.fetchall()

#print seconds



#>>> cur.fetchone()
#(1, 100, "abc'def")

# Execute a command: this creates a new table
#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
#cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
#...      (100, "abc'def"))

# Query the database and obtain data as Python objects

# Make the changes to the database persistent
#>>> conn.commit()

# Close communication with the database
cur.close()
conn.close()

