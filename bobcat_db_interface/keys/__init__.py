import os

try:
    db_info_file = "{0}/{1}".format(os.getenv("HOME"),".bobcat/db_info.txt")
except Exception as err:
    print("Error reading your DB info. You might need to fix your .bobcat/db_info.txt file. Returned error:",err)
    exit()

try:
    db_info = {line.split(":", 1)[0].strip(): line.split(":", 1)[1].strip() for line in open(db_info_file) if ':' in line}
except Exception as err:
    print("It seems I could read your .bobcat/db_info.txt file but it has an unacceptable format. Returned error:",err)
    exit()
