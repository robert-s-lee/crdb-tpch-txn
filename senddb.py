# Import the driver
import psycopg2
import sys
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--host", dest="host", default="127.1")
parser.add_option("--port", dest="port", default="26257")
parser.add_option("--username", dest="username", default="root")
parser.add_option("--password", dest="password", default="")
parser.add_option("--dbname", dest="dbname", default="tpch")
(options, args) = parser.parse_args()

# Connect to the database
conn = psycopg2.connect(database=options.dbname, user=options.username, host=options.host, port=options.port, password=options.password)

# Make each statement commit immediately.
conn.set_session(autocommit=True)

# Open a cursor to perform database operations.
cur = conn.cursor()

# send each line as a statement
for line in sys.stdin:
  cur.execute(line)

# Close the database connection.
cur.close()
conn.close()
