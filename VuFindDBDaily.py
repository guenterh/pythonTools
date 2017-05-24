import os,re
from datetime import timedelta, datetime

#os.chdir("/usr/local/swissbib/meni/mapreduce")
os.chdir("/root/scripts")

CONFIG='/root/scripts/VuFindDB.conf'
LOGFILE= open('VuFindDB_daily.log','a')

def cleanup_scratch(DB_USER, DB_PASS, DB_NAME):
    prunedate = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    LOGFILE.write('prune ' + DB_NAME + '.search  - ' + str(prunedate) + os.linesep )
    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="Delete from search where created < \'',str(prunedate),'\' and user_id=0"'])
    os.system(execute)

    LOGFILE.write('prune ' + DB_NAME + '.session  - ' + str(prunedate) + os.linesep )
    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="Delete from session where created < \'',str(prunedate),'\'"'])
    os.system(execute)
    #update user set language = 'en' where language = 'en';


#this is actually because we have a bug in VuFind with language settings
def setLanguageForUserTable(DB_USER, DB_PASS, DB_NAME):
    userUpdateDate = (datetime.now()).strftime("%Y-%m-%d")
    LOGFILE.write('update user table ' + DB_NAME + '.user  - ' + str(userUpdateDate) + os.linesep )

    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="update user set language = \'en\' where language = \'en\'"'])
    os.system(execute)

    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="update user set language = \'de\' where language = \'de\'"'])
    os.system(execute)

    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="update user set language = \'it\' where language = \'it\'"'])
    os.system(execute)

    execute = "".join(['mysql --user=', DB_USER,' --password=\'',DB_PASS,'\' --database=',DB_NAME,
               ' --execute="update user set language = \'fr\' where language = \'fr\'"'])
    os.system(execute)



pattern = re.compile('^#')
with open(CONFIG, "r") as configFile:
    for line in configFile:
        #keine Kommentarzeilen und Leerzeilen
        if not  pattern.search(line) and not re.match(r'^\s*$', line) :
            #remove endline
            arguments = line.rstrip(os.linesep).split(',')
            cleanup_scratch(arguments[0],arguments[1],arguments[2])
            setLanguageForUserTable(arguments[0],arguments[1],arguments[2])

LOGFILE.close()
