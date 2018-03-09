# between two versions, tagging files according to commmit tag
# not yet add into main program
import subprocess
import csv
from orm.commit import *
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Session = sessionmaker()
engine = sqlalchemy.create_engine("postgresql"+ '+' +
                                  "pypostgresql" + '://' +
                                  "dbuser" + ':' +
                                  "12345678" + '@' +
                                  "localhost" + ':' +
                                  "5432" + '/' +
                                  "cas_vlis", pool_size=100, max_overflow=0) # the value of pool_size has to be less than the max_connections to postgres.
Session.configure(bind=engine)
Base = declarative_base()
session = Session()


TAG_CMD = 'git tag --sort=taggerdate --no-merge'
FILE_CMD = 'git log --pretty=format: --name-status | cut -f2- | sort -u'
LOG_CMD1 = 'git log {!s} --pretty=format:"%H"'
LOG_CMD2 = 'git log  {!s}..{!s} --pretty=format:"%H"'

repo_dir = '/home/wenfeng/vlis/cas_vlis/ingester/CASRepos/git/ant1803'
list_ext_dir = "/home/wenfeng/vlis/cas_vlis/analyzer/code_file_extentions.txt"

header = ['filename','classification']


# get the java file list
def is_included(file):
    file_exts_to_include = open(list_ext_dir).read().splitlines()
      # weed out bad files/binary files/etc
    if file != "'" and file != "":
        file_info = file.split(".")

        # get extentions
        if len(file_info) > 1:
            file_ext = (file_info[1]).lower()

          # ensure these source code file endings
            if file_ext.upper() in file_exts_to_include:
                return True
            else:
                return False
        else:
            return False
    else:
        return False

# list all tags
tags = (subprocess.check_output(TAG_CMD,shell=True,cwd=repo_dir)).decode('utf-8','replace').strip().split('\n')

# list all included files
files_tmp = (subprocess.check_output(FILE_CMD,shell=True,cwd=repo_dir)).decode('utf-8','replace').strip().split('\n')
files = {}
for file in files_tmp:
    if is_included(file):
        files[file] = ''

def fun(commits, tag1, tag2):
    for commit in commits:
        corrective_commits = (session.query(Commit).filter(Commit.commit_hash == commit).all())
        if corrective_commits:
            fileschanged_tmp = corrective_commits[0].fileschanged.split(",CAS_DELIMITER")
            fileschanged = []
            for file in fileschanged_tmp:
                if file == '':
                    continue
                elif is_included(file):
                    fileschanged.append(file.lstrip(","))
                    new_classification = corrective_commits[0].classification
                    file_name = file.lstrip(",")
                    old_classification = files[file_name].split("CAS_DELIMITER")
                    if new_classification not in old_classification:
                        files[file_name] += new_classification + "CAS_DELIMITER"
    f_list = list(files.items())
    result_file = './ant/' + 'ant_' + tag1.replace('/','') + '_'+ tag2.replace('/','') + '.csv'
    with open(result_file, 'w') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(header)
        f_csv.writerows(f_list)

#first tag
commits = (subprocess.check_output((LOG_CMD1.format(tags[0])),shell=True,cwd=repo_dir)).decode('utf-8','replace').split('\n')
fun(commits,'None',tags[0])

for i in range(1, len(tags)-1):
    tag1 = tags[i-1]
    tag2 = tags[i]
    # get commits list
    commits = (subprocess.check_output(LOG_CMD2.format(tag1,tag2),shell=True,cwd=repo_dir)).decode('utf-8','replace').split('\n')
    fun(commits,tag1,tag2)


