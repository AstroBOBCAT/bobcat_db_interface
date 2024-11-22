import os
from . import db_comms


# Read in personal keys and info from user-based file.
# This info file lives in ~/.bobcat/db_info.txt
db_info_file = "{0}/{1}".format(os.getenv("HOME"),".bobcat/db_info.txt")
db_info = {line.split(":", 1)[0].strip(): line.split(":", 1)[1].strip() for line in open(db_info_file) if ':' in line}
