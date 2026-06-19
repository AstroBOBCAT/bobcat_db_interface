import os

db_info_file = "{0}/{1}".format(os.getenv("HOME"),".bobcat/db_info.txt")

# This maps to Dominic's docker set-up
db_info = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "pass": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}
#db_info = {line.split(":", 1)[0].strip(): line.split(":", 1)[1].strip() for line in open(db_info_file) if ':' in line}
