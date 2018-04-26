import os
import subprocess
import re
import json
import logging
import math                               # Required for the math.log function
from orm.commit import *
from ingester.commitFile import *         # Represents a file
from classifier.classifier import *       # Used for classifying each commit
import time
import csv

"""
file: repository.py
authors: Ben Grawi <bjg1568@rit.edu>, Christoffer Rosen <cbr4830@rit.edu>
date: October 2013
description: Holds the repository git abstraction class
"""

class Git():
    """
    Git():
    pre-conditions: git is in the current PATH
                    self.path is set in a parent class
    description: a very basic abstraction for using git in python.
    """
    # Two backslashes to allow one backslash to be passed in the command.
    # This is given as a command line option to git for formatting output.

    # A commit mesasge in git is done such that first line is treated as the subject,
    # and the rest is treated as the message. We combine them under field commit_message

    # We want the log in ascending order, so we call --reverse
    # Numstat is used to get statistics for each commit
    LOG_FORMAT = '--pretty=format:\" CAS_READER_STARTPRETTY\
    \\"parent_hashes\\"CAS_READER_PROP_DELIMITER: \\"%P\\",CAS_READER_PROP_DELIMITER2\
    \\"commit_hash\\"CAS_READER_PROP_DELIMITER: \\"%H\\",CAS_READER_PROP_DELIMITER2\
    \\"author_name\\"CAS_READER_PROP_DELIMITER: \\"%an\\",CAS_READER_PROP_DELIMITER2\
    \\"author_email\\"CAS_READER_PROP_DELIMITER: \\"%ae\\",CAS_READER_PROP_DELIMITER2\
    \\"author_date\\"CAS_READER_PROP_DELIMITER: \\"%ad\\",CAS_READER_PROP_DELIMITER2\
    \\"author_date_unix_timestamp\\"CAS_READER_PROP_DELIMITER: \\"%at\\",CAS_READER_PROP_DELIMITER2\
    \\"commit_message\\"CAS_READER_PROP_DELIMITER: \\"%s%b\\"\
    CAS_READER_STOPPRETTY \" --numstat --reverse  --before=\"2017-10-1\"'

    CLONE_CMD = 'git clone {!s} {!s}'     # git clone command w/o downloading src code
    PULL_CMD = 'git pull'      # git pull command
    RESET_CMD = 'git reset --hard FETCH_HEAD'
    CLEAN_CMD = 'git clean -df' # f for force clean, d for untracked directories
    DIFF_CMD = "git diff {0}^ {1} "
    DIFF_CMD_NAME = "git diff {0}^ {1} --name-only"
    DIFF_CMD_INIT = "git diff {0} "

    REPO_DIRECTORY = "/CASRepos/git/"        # directory in which to store repositories
    DIFF_DIRECTORY = "/CASRepos/diff/"       # directory in which to store diff information
    LEAST_CHARACTER = 10
    MAX_LINE = 10000                         # if modified line of one commit is more then MAX_LINE, then ommit this commit 


    def getCommitStatsProperties( stats, commitFiles, devExperience, author, unixTimeStamp ):
        """
        getCommitStatsProperties
        Helper method for log. Caclulates statistics for each change/commit and
        returns them as a comma seperated string. Log will add these to the commit object
        properties

        @param stats            These are the stats given by --numstat as an array
        @param commitFiles      These are all tracked commit files
        @param devExperience    These are all tracked developer experiences
        @param author           The author of the commit
        @param unixTimeStamp    Time of the commit
        """

        statProperties = ""

        # Data structures to keep track of info needed for stats
        subsystemsSeen = []                         # List of system names seen
        directoriesSeen = []                        # List of directory names seen
        locModifiedPerFile = []                     # List of modified loc in each file seen
        authors = []                                # List of all unique authors seen for each file
        fileAges = []                               # List of the ages for each file in a commit

        # Stats variables
        la = 0                                      # lines added
        ld = 0                                      # lines deleted
        nf = 0                                      # Number of modified files
        ns = 0                                      # Number of modified subsystems
        nd = 0                                      # number of modified directories
        entrophy = 0                                # entrophy: distriubtion of modified code across each file
        lt = 0                                      # lines of code in each file (sum) before the commit
        ndev = 0                                    # the number of developers that modifed the files in a commit
        age = 0                                     # the average time interval between the last and current change
        exp = 0                                     # number of changes made by author previously
        rexp = 0                                    # experience weighted by age of files ( 1 / (n + 1))
        sexp = 0                                    # changes made previous by author in same subsystem
        totalLOCModified = 0                        # Total modified LOC across all files
        nuc = 0                                     # number of unique changes to the files
        filesSeen = ""                              # files seen in change/commit

        for stat in stats:

            if( stat == ' ' or stat == '' ):
                continue

            fileStat = stat.split("\\t")

             # Check that we are only looking at file stat (i.e., remove extra newlines)
            if( len(fileStat) < 2):
                continue
            # catch the git "-" line changes
            try:
                fileLa = int(fileStat[0])
                fileLd = int(fileStat[1])
            except:
                fileLa = 0
                fileLd = 0

            # Remove oddities in filename so we can process it
            fileName = (fileStat[2].replace("'",'').replace('"','').replace("\\",""))

            totalModified = fileLa + fileLd

            # have we seen this file already?
            if(fileName in commitFiles):
                prevFileChanged = commitFiles[fileName]
                prevLOC = getattr(prevFileChanged, 'loc')
                prevAuthors = getattr(prevFileChanged, 'authors')
                prevChanged = getattr(prevFileChanged, 'lastchanged')
                file_nuc = getattr(prevFileChanged, 'nuc')
                nuc += file_nuc
                lt += prevLOC

                for prevAuthor in prevAuthors:
                    if prevAuthor not in authors:
                        authors.append(prevAuthor)

                # Convert age to days instead of seconds
                age += ( (int(unixTimeStamp) - int(prevChanged)) / 86400 )
                fileAges.append(prevChanged)

                # Update the file info

                file_nuc += 1 # file was modified in this commit
                setattr(prevFileChanged, 'loc', prevLOC + fileLa - fileLd)
                setattr(prevFileChanged, 'authors', authors)
                setattr(prevFileChanged, 'lastchanged', unixTimeStamp)
                setattr(prevFileChanged, 'nuc', file_nuc)

            else:

                # new file we haven't seen b4, add it to file commit files dict
                if(author not in authors):
                    authors.append(author)

                if(unixTimeStamp not in fileAges):
                    fileAges.append(unixTimeStamp)

                fileObject = CommitFile(fileName, fileLa - fileLd, authors, unixTimeStamp)
                commitFiles[fileName] = fileObject

            # end of stats loop

            locModifiedPerFile.append(totalModified) # Required for entrophy
            totalLOCModified += totalModified
            fileDirs = fileName.split("/")

            if( len(fileDirs) == 1 ):
                subsystem = "root"
                directory = "root"
            else:
                subsystem = fileDirs[0]
                directory = "/".join(fileDirs[0:-1])

            if( subsystem not in subsystemsSeen ):
                subsystemsSeen.append( subsystem )

            if( author in devExperience ):
                experiences = devExperience[author]
                exp += sum(experiences.values())

                if( subsystem in experiences ):
                    sexp = experiences[subsystem]
                    experiences[subsystem] += 1
                else:
                    experiences[subsystem] = 1

                try:
                    rexp += (1 / (age) + 1)
                except:
                    rexp += 0

            else:
                devExperience[author] = {subsystem: 1}

            if( directory not in directoriesSeen ):
                directoriesSeen.append( directory )

            # Update file-level metrics
            la += fileLa
            ld += fileLd
            nf += 1
            filesSeen += fileName + ",CAS_DELIMITER,"

        # End stats loop

        if( nf < 1):
            return ""

        # Update commit-level metrics
        ns = len(subsystemsSeen)
        nd = len(directoriesSeen)
        ndev = len(authors)
        lt = lt / nf
        age = age / nf
        exp = exp / nf
        rexp = rexp / nf

        # Update entrophy
        for fileLocMod in locModifiedPerFile:
            if (fileLocMod != 0 ):
                avg = fileLocMod/totalLOCModified
                entrophy -= ( avg * math.log( avg,2 ) )

        # Add stat properties to the commit object
        statProperties += ',"la":"' + str( la ) + '\"'
        statProperties += ',"ld":"' + str( ld ) + '\"'
        statProperties += ',"fileschanged":"' + filesSeen[0:-1] + '\"'
        statProperties += ',"nf":"' + str( nf ) + '\"'
        statProperties += ',"ns":"' + str( ns ) + '\"'
        statProperties += ',"nd":"' + str( nd ) + '\"'
        statProperties += ',"entrophy":"' + str(  entrophy ) + '\"'
        statProperties += ',"ndev":"' + str( ndev ) + '\"'
        statProperties += ',"lt":"' + str( lt ) + '\"'
        statProperties += ',"nuc":"' + str( nuc ) + '\"'
        statProperties += ',"age":"' + str( age ) + '\"'
        statProperties += ',"exp":"' + str( exp ) + '\"'
        statProperties += ',"rexp":"' + str( rexp ) + '\"'
        statProperties += ',"sexp":"' + str( sexp ) + '\"'

        return statProperties
    # End stats

    def isComment(self, line):
        """
        isComment():helper method for parsingDiff(), to decide whether a line is a comment or not
        :param line: a string
        :return: boolean
        """
        if line.startswith('//') or line.startswith('/**') or line.startswith('*') or line.startswith(
                '/*') or line.endswith('*/'):
            return True
        else:
            return False

    def getBuggyLines(self,commit):
        bug = {}
        if commit.buggy_lines == 'NULL':
            return bug
        buggy_files = commit.buggy_lines.split('FILE_START:')[1:]

        for buggy_file in buggy_files:
            info = buggy_file.split(',')
            file_name = info[0]
            lines = info[1:]
            bug[file_name] = lines
        return bug

    def getBugLabel(self, file, line_num,buggy_lines):
        lines = buggy_lines.get(file,[])
        if lines:
            if str(line_num) in lines:
                return True
            else:
                return False
        else:
            return False


    def isOneLine(self,line):
        # line.find("class "): a line to define a class
        # line.find("throws “）： a line to define exception
        if line.endswith("{") or line.endswith("}") or line.endswith(";") or line.startswith("@") or\
                line.endswith(")") or line.find("class ") != -1 or line.find("throws ") != -1:
            return True
        else:
            return False

    def parsingDiff(self, diff_info, commit):
        if len(diff_info.split('\n')) > self.MAX_LINE:
            return []
        region_diff = {}
        # only link code source files as any type of README, etc typically have HUGE changes and reduces
        # the performance to unacceptable levels. it's very hard to blacklist everything; much easier just to whitelist
        # code source files endings.

        list_ext_dir = os.path.dirname(__file__)+  "/../analyzer/code_file_extentions.txt"
        with open(list_ext_dir,'r') as file:
            file_exts_to_include = file.read().splitlines()

        regions = diff_info.split('diff --git ')
        if len(regions) < 2:
            return [] # ignore commits without diff information like merge commit

        add_results = []
        del_results = []
        addresuluts_header = ['commit_hash', 'content', 'file_pre', 'file_new', 'line_num', 'author', 'time', 'bug_introducing','commit_label']
        delresuluts_header = ['commit_hash', 'content', 'file_pre', 'file_new', 'line_num', 'author', 'time', 'fix']
        # file to store results
        add_file = os.path.dirname(
            __file__) + self.DIFF_DIRECTORY + commit.repository_id + '/' + commit.repository_id + '_add.csv'
        del_file = os.path.dirname(
            __file__) + self.DIFF_DIRECTORY + commit.repository_id + '/' + commit.repository_id + '_del.csv'
        buggy_lines = self.getBuggyLines(commit)

        for region in regions[1:]:
            chunks = region.split('@@ -')
            # get the previous file name and new file name
            file_pre = re.search('\-{3} (a/)?(.*)', chunks[0])
            if hasattr(file_pre, 'group'):
                file_pre = file_pre.group(2)
            else:
                continue
            file_new = re.search('\+{3} (b)/?(.*)', chunks[0])
            if hasattr(file_new, 'group'):
                file_new = file_new.group(2)
            else:
                continue

            # only focus on ".java" file
            file_info = file_new.split(".")
            if len(file_info) > 1: # get extentions
                file_ext = (file_info[1]).lower()
                if file_ext.upper() not in file_exts_to_include:# ensure these source code file endings
                    continue
            else:
                continue
            line_am = '' # variable to process added multiple lines
            line_dm = '' # variable to process deleted multiple lines
            first_segm = True # helper variable to process multiple lines condition
            num_m = 0
            for chunk in chunks[1:]:
                lines = chunk.split('\n')
                # get the line number of each change
                nums = re.match(r'^(\d+),*\d* \+(\d+),*\d* @@', lines[0])
                if hasattr(nums, 'group'):
                    pre_current = int(nums.group(1))
                    new_current = int(nums.group(2))
                else:
                    continue
                bug_introducing = False
                fix = False
                for line in lines[1:]:
                    is_add = line.startswith('+')  # this line add some code(missing in previous file but added to new file)
                    is_del = line.startswith('-')  # this line delete some code(appears in previous file but removed in new file)
                    if is_add:
                        line = line.lstrip('+').strip().strip('\t').strip('\r')
                        # this line is a comment or not
                        comment = self.isComment(line)
                        if not comment:
                            if len(line) < self.LEAST_CHARACTER:
                                continue  # escape those line without enought information
                            bug_flag = self.getBugLabel(file_new, new_current, buggy_lines)
                            if bug_flag:
                                bug_introducing = True
                            if self.isOneLine(line):
                                line_am += ' ' + line
                                if first_segm:
                                    num_m = new_current

                                result = (commit.commit_hash, line_am, file_pre, file_new, num_m, commit.author_name,
                                          commit.author_date, bug_introducing, commit.contains_bug)
                                # bug all contain_bug became False
                                add_results.append(result)
                                line_am = ''  # reset
                                first_segm = True  # reset
                                bug_introducing = False # reset
                            else:
                                if first_segm:
                                    num_m = new_current
                                line_am += line
                                first_segm = False # set for the next segment, if exist.
                            new_current += 1
                        else:
                            new_current += 1
                            continue
                    elif is_del:
                        line = line.lstrip('-').strip().strip('\t').strip('\r')  # remove some useless characters
                        comment = self.isComment(line)
                        if not comment:
                            if len(line) < self.LEAST_CHARACTER:
                                continue  # ignore blank lines
                            fix_flag = commit.fix
                            if fix_flag=='True':
                                fix = True
                            if self.isOneLine(line):
                                line_dm += ' ' + line
                                if first_segm:
                                    num_m = pre_current
                                result = (commit.commit_hash, line_dm, file_pre, file_new, num_m, commit.author_name,
                                          commit.author_date, fix)
                                del_results.append(result)
                                line_dm = ''
                                first_segm = True
                                fix = False # reset
                            else:
                                if first_segm:
                                    num_m = pre_current
                                line_dm += line
                                first_segm = False
                            pre_current += 1
                        else:
                            pre_current += 1
                            continue
                    else:
                        pre_current += 1
                        new_current += 1
                        continue
        add_exist = os.path.isfile(add_file)  # avoid write file header towice
        with open(add_file, 'a') as file:
            f_csv = csv.writer(file)
            if not add_exist:
                f_csv.writerow(addresuluts_header)
            f_csv.writerows(add_results)
        del_exist = os.path.isfile(del_file)
        with open(del_file, 'a') as file:
            f_csv = csv.writer(file)
            if not del_exist:
                f_csv.writerow(delresuluts_header)
            f_csv.writerows(del_results)


    def diff(self,repoId):
        repo_dir = os.chdir(os.path.dirname(__file__) + self.REPO_DIRECTORY + repoId)
        diff_dir = os.path.dirname(__file__)+ self.DIFF_DIRECTORY + repoId

        # check the directory exist or not
        if not os.path.isdir(diff_dir):
            os.mkdir(diff_dir)
        else:
            pass

        # get commit hash
        session = Session()
        commits = (session.query(Commit).filter((Commit.repository_id==repoId)&(Commit.diffed==False))
                   .order_by( Commit.author_date_unix_timestamp.desc()).all())

        # diff
        logging.info('Starting get/parsing diff information.')
        for commit in commits:
            try:
                diff_info = (subprocess.check_output(self.DIFF_CMD.format(commit.commit_hash, commit.commit_hash),\
                                                 shell=True, cwd=repo_dir)).decode('utf-8','replace')

                self.parsingDiff(diff_info,  commit)
                commit.diffed = True
            except:
                try:
                    diff_info = (subprocess.check_output(self.DIFF_CMD_INIT.format(commit.commit_hash), \
                                                         shell=True, cwd=repo_dir)).decode('utf-8', 'replace')

                    self.parsingDiff(diff_info, commit)
                    commit.diffed = True
                    #session.commit()  # update diffed
                except Exception as e:
                    logging.info(e)
                    continue
        # the initial commit
        session.commit()
        session.close()
        logging.info('Done getting/parsing diff informations.')

    def log(self, repo, firstSync):
        """
        log(): Repository, Boolean -> Dictionary
        arguments: repo Repository: the repository to clone
                   firstSync Boolean: whether to sync all commits or after the
            ingestion date
        description: a very basic abstraction for using git in python.
        """
        repo_dir = os.chdir(os.path.dirname(__file__) + self.REPO_DIRECTORY + repo.id)
        logging.info('Getting/parsing git commits: '+ str(repo) )
        # Spawn a git process and convert the output to a string
        if not firstSync and repo.ingestion_date is not None:
            cmd = 'git log --after="' + repo.ingestion_date + '" '
        else:
            cmd = 'git log '

        log = str( subprocess.check_output(cmd + self.LOG_FORMAT, shell=True, cwd = repo_dir ) )
        log = log[2:-1]   # Remove head/end clutter

        # List of json objects
        json_list = []

        # Make sure there are commits to parse
        if len(log) == 0:
            return []

        commitFiles = {}            # keep track of ALL file changes
        devExperience = {}          # Keep track of ALL developer experience
        classifier = Classifier()   # classifier for classifying commits (i.e., corrective, feature addition, etc)

        commitList = log.split("CAS_READER_STARTPRETTY")

        for commit in commitList:
            author = ""                                 # author of commit
            unixTimeStamp = 0                           # timestamp of commit
            fix = False                                 # whether or not the change is a defect fix
            classification = None                       # classification of the commit (i.e., corrective, feature addition, etc)
            isMerge = False                             # whether or not the change is a merge

            commit = commit.replace('\\x', '\\u00')   # Remove invalid json escape characters
            splitCommitStat = commit.split("CAS_READER_STOPPRETTY")  # split the commit info and its stats

            # The first split will contain an empty list
            if(len(splitCommitStat) < 2):
                continue

            prettyCommit = splitCommitStat[0]
            statCommit = splitCommitStat[1]
            commitObject = ""

            # Start with the commit info (i.e., commit hash, author, date, subject, etc)
            prettyInfo = prettyCommit.split(',CAS_READER_PROP_DELIMITER2    "')
            for propValue in prettyInfo:
                props = propValue.split('"CAS_READER_PROP_DELIMITER: "')
                propStr = ''
                for prop in props:
                    prop = prop.replace('\\','').replace("\\n", '')  # avoid escapes & newlines for JSON formatting
                    propStr = propStr + '"' + prop.replace('"','') + '":'

                values = propStr[0:-1].split(":")

                if(values[0] == '"    parent_hashes"'):
                    # Check to see if this is a merge change. Fix for Issue #26. 
                    # Detects merges by counting the # of parent commits
                    
                    parents = values[1].split(' ')
                    if len(parents) == 2:
                        isMerge = True

                if(values[0] == '"author_name"'):
                    author = values[1].replace('"', '')

                if(values[0] == '"author_date_unix_timestamp"'):
                    unixTimeStamp = values[1].replace('"','')

                # Classify the commit
                if(values[0] == '"commit_message"'):

                    if (isMerge):
                        classification = "Merge"
                    else:
                        classification = classifier.categorize(values[1].lower())

                    # If it is a corrective commit, we induce it fixes a bug somewhere in the system
                    if classification == "Corrective":
                        fix = True


                commitObject += "," + propStr[0:-1]
                # End property loop
            # End pretty info loop

            # Get the stat properties
            stats = statCommit.split("\\n")
            commitObject += self.getCommitStatsProperties(stats, commitFiles, devExperience, author, unixTimeStamp)

            # Update the classification of the commit
            commitObject += ',"classification":"' + str( classification ) + '\"'

             # Update whether commit was a fix or not
            commitObject += ',"fix":"' + str( fix ) + '\"'

            # Remove first comma and extra space
            commitObject = commitObject[1:].replace('    ','')
            # Add commit object to json_list
            json_list.append(json.loads('{' + commitObject + '}'))
        # End commit loop

        logging.info('Done getting/parsing git commits.')

        return json_list

    def clone(self, repo):
        """
        clone(repo): Repository -> String
        description:Takes the current repo and clones it into the
            `clone_directory/the_repo_id`
        arguments: repo Repository: the repository to clone
        pre-conditions: The repo has not been already created
        """
        repo_dir = os.chdir(os.path.dirname(__file__) + self.REPO_DIRECTORY)

        # Run the clone command and return the results

        logging.info('Git cloning repo: '+ str(repo) )
        cloneResult = str(subprocess.check_output(
                  self.CLONE_CMD.format(repo.url, './' + repo.id),
                  shell= True,
                  cwd = repo_dir ) )
        logging.info('Done cloning.')
        #logging.debug("Git clone result:\n" + cloneResult)

        # Reset path for next repo

        # TODO: only return true on success, else return false
        return True

    def pull(self, repo):
        """
        fetch(repo): Repository -> String
        description:Takes the current repo and pulls the latest changes.
        arguments: repo Repository: the repository to pull
        pre-conditions: The repo has already been created
        """

        repo_dir = os.path.dirname(__file__) + self.REPO_DIRECTORY + repo.id

        # Weird sceneario where something in repo gets modified - reset all changes before pulling
        subprocess.call(self.RESET_CMD, shell=True, cwd= repo_dir)
        subprocess.call(self.CLEAN_CMD, shell=True, cwd= repo_dir)

        # Run the pull command and return the results
        logging.info('Pulling latest changes from repo: '+ str(repo) )
        fetchResult = str(subprocess.check_output(
                  self.RESET_CMD + "\n" + self.PULL_CMD ,
                  shell=True,
                  cwd=  repo_dir  ) )
        logging.info('Done fetching.')
        #logging.debug("Git pull result:\n" + cloneResult)

        # TODO: only return true on success, else return false
        return True
