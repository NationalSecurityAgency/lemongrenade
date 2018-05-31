
'''

Provides JSON data of LEMONGRENADE statistics. Can be ran from cron once a day
and output to html file /opt/lemongrenade/webapp/static/metrics.json, which the api.

Sample Cron Entry: (run at 2:10AM)
----------------------------------
10 2 * * * python /opt/lemongrenade/bin/lgstats.py   /opt/lemongrenade/webapp/static/data/metrics.json

the metrics.json is viewable in raw form at http://localhost:9999/data/metrics.json

Or it's used by the lemongrande api gui at http://localhost:9999


'''
from operator import itemgetter
import json
import os
import json
import calendar
import time
import datetime
import requests
from array import array
from datetime import datetime
import sys
from time import gmtime, strftime
from collections import OrderedDict

class LemonGrenadeStats(object):

    def __init__(self, server, ofile):
        self.numberOfDays = 30
        self.maxadapters  = 50
        self.server = server
        self.jobs = []
        self.adapters = {}
        self.dayJobTotal= [0]*self.numberOfDays
        self.dayErrorTotal= [0]*self.numberOfDays
        self.dayTaskTotal= [0]*self.numberOfDays
        self.dayAvgSeedSize = [0]*self.numberOfDays

        self.graphActivityDayMin= [0]*self.numberOfDays
        self.graphActivityDayMax= [0]*self.numberOfDays
        self.graphActivityDayCount= [0]*self.numberOfDays  # keeps running count for avg calc
        self.failedTaskDayTotal = [0]*self.numberOfDays
        self.successTaskDayTotal= [0]*self.numberOfDays

        self.usersByJobsSubmitted = {}
        self.usersBySeedSize     = {}
        self.usersByGraphSize    = {}

        self.adapterErrorsPerDay = {}
        self.adapterTasksPerDay  = {}
        self.adapterRunTimeTotalPerDay = {}
        self.adapterRunTimeAvgPerDay = {}
        self.adapterTasksSpawnedPerDayAvg  = {}
        self.adapterTasksSpawnedPerDayMin  = {}
        self.adapterTasksSpawnedPerDayMax  = {}
        self.adapterGraphChangesPerDayAvg  = {}
        self.adapterGraphChangesPerDayMin  = {}
        self.adapterGraphChangesPerDayMax  = {}

        ## Top Level
        self.jobsSubmittedByHour = [0]*24
        self.adapterNames    = []
        self.totalJobCount   = 0
        self.totalErrorCount = 0
        self.totalTaskCount  = 0
        self.totalJobAvgRunTimeCount = 0

        self.jobAvgRuntime  = 0
        self.adaptersErrors = {}
        self.adaptersTasks  = {}
        self.adaptersAvgRunTime = {}
        self.currentTime= strftime("%b %d,%Y %H:%M:%S")
        self.sortOn = 'createDate'
        self.sortOrder= True
        self.load()
        print "Writing output file "+ofile
        self.writeStats(ofile)
        print "Complete."

    def sortOrderChange(self):
        self.sortOrder = not self.sortOrder

    def sortBy(self,field):
        self.sortOn = field
        self.jobs = sorted(self.jobs, key=itemgetter(field),reverse=self.sortOrder)

    def load(self):
        print "Querying adapter data..."
        url = self.server + "/api/adapter"
        print url
        ar   = requests.get(url)
        adaptersFull = json.loads(ar.text)

        print "Querying jobs ran in last "+str(self.numberOfDays)+ " days..."
        # Get last self.numberOfDays days
        url = self.server + "/api/jobs/days/full/1/"+str(self.numberOfDays)
        print url
        r   = requests.get(url)
        rawjobs = json.loads(r.text)
        self.totalJobCount = len(rawjobs)

        print "Processing adapter data"
        for a in adaptersFull:
            self.adapters[str(adaptersFull[a]['type'])] = adaptersFull[a]
            self.adapterNames.append(adaptersFull[a]['name']+"-"+adaptersFull[a]['id'])
            ## Initialize counters for adapters
            for i in range(self.numberOfDays):
                self.adapterErrorsPerDay[a]          = [0]*self.numberOfDays
                self.adapterTasksPerDay[a]           = [0]*self.numberOfDays
                self.adapterRunTimeTotalPerDay[a]    = [0]*self.numberOfDays
                self.adapterRunTimeAvgPerDay[a]      = [0]*self.numberOfDays
                self.adapterTasksSpawnedPerDayMin[a] = [0]*self.numberOfDays
                self.adapterTasksSpawnedPerDayMax[a] = [0]*self.numberOfDays
                self.adapterTasksSpawnedPerDayAvg[a] = [0]*self.numberOfDays
                self.adapterGraphChangesPerDayMin[a] = [0]*self.numberOfDays
                self.adapterGraphChangesPerDayMax[a] = [0]*self.numberOfDays
                self.adapterGraphChangesPerDayAvg[a] = [0]*self.numberOfDays


        ## Preprocess job info
        print "Processing job data"
        for j in rawjobs:
            job = {}
            job['job_id'] = str(rawjobs[j]['job']['job_id'])
            job['status'] = rawjobs[j]['job']['status']
            job['starttime'] = rawjobs[j]['job']['starttime']
            job['endtime'] = rawjobs[j]['job']['endtime']
            job['graph_activity'] = rawjobs[j]['job']['graph_activity']
            job['task_count'] = rawjobs[j]['job']['task_count']
            job['active_task_count'] = rawjobs[j]['job']['active_task_count']
            job['error_count'] = rawjobs[j]['job']['error_count']
            job['job_config'] = rawjobs[j]['job']['job_config']


            # We don't store the seed data in the job data, in the future, if we do - (for replay)
            # we'll have to readdress this
            #if 'seed' in rawjobs[j]['job']:
            #    job['seed'] = rawjobs[j]['job']['seed']
            #    print "FOUND SEED"+job['seed']


            job['user'] = "unknown"
            if 'roles' in  job['job_config']:
                roles = job['job_config']['roles']
                for r in roles:
                    if roles[r]['owner']:
                        job['user'] = r
            if r in self.usersByJobsSubmitted:
                self.usersByJobsSubmitted[r] += 1
            else:
                self.usersByJobsSubmitted[r] = 1


            self.totalErrorCount += job['error_count']
            self.totalTaskCount  += job['task_count']


            # Calc run time
            utime = datetime.strptime(job['starttime']+"","%b %d,%Y %H:%M:%S")
            epoch_start = int(time.mktime(utime.timetuple()))
            endtime = datetime.strptime(job['endtime']+"","%b %d,%Y %H:%M:%S")
            epoch_end = int(time.mktime(endtime.timetuple()))
            if epoch_end <= 0:
                runtime = int(time.time() - epoch_start)
            else:
                runtime = int(epoch_end - epoch_start)
            job['runtime'] = runtime

            self.totalJobAvgRunTimeCount += runtime

            ## Store for jobsByHour table
            self.jobsSubmittedByHour[utime.hour] += 1

            # user
            job['user'] = "unknown"
            if hasattr(job['job_config'],'roles'):
                roles = job['job_config']['roles']
                for r in roles:
                    if roles[r]['owner']:
                        job['user'] = roles[r]

            # Which day (1 .. number of days) is this job
            timediff = int(time.time() - epoch_start) / 86400
            job['day'] = int(timediff)
            self.dayJobTotal[int(timediff)] += 1
            self.dayErrorTotal[int(timediff)] += job['error_count']
            self.dayTaskTotal[int(timediff)] += job['task_count']

            # Avg Seed size
            seedSize = 0
            if 'seed' in job:
                print job['seed']
                seedSize = sys.getsizeof(job['seed'])
            self.dayAvgSeedSize[int(timediff)] += seedSize


            # Job graph_activity, min, max , count
            ga = job['graph_activity']
            self.graphActivityDayCount[int(timediff)] += ga
            if (self.graphActivityDayMin[int(timediff)] == 0):
                self.graphActivityDayMin[int(timediff)] = ga
            elif (self.graphActivityDayMin[int(timediff)] > ga):
                self.graphActivityDayMin[int(timediff)] = ga
            if (self.graphActivityDayMax[int(timediff)] < ga):
                self.graphActivityDayMax[int(timediff)] = ga

            # Story history by taskID for fast lookups
            # we use graphchagnes, tasks
            jobhistory = {}
            for history in rawjobs[j]['history']:
                if 'task_id' in history:
                    jobhistory[history['task_id']] = history

            ## Loop through tasks
            for task in rawjobs[j]['tasks']:
                # Extract info for history if we can find it for this task
                graphChanges = 0
                tasksSpawned = 0
                if task['task_id'] in jobhistory:
                    h = jobhistory[task['task_id']]
                    #print "----> "+str(h)
                    if 'number_of_new_tasks_generated' in h:
                        tasksSpawned = h['number_of_new_tasks_generated']
                    if 'graph_changes' in h:
                        graphChanges = h['graph_changes']

                a = task['adapter_name']+"-"+task['adapter_id']
                if a in self.adapterTasksPerDay:
                    self.adapterTasksPerDay[a][timediff]  += 1;

                if a in self.adapterTasksSpawnedPerDayAvg:
                    self.adapterTasksSpawnedPerDayAvg[a][timediff]  += tasksSpawned;
                    if tasksSpawned < self.adapterTasksSpawnedPerDayMin:
                        self.adapterTasksSpawnedPerDayMin[a][timediff]  = tasksSpawned;
                    if tasksSpawned > self.adapterTasksSpawnedPerDayMax[a][timediff]:
                        self.adapterTasksSpawnedPerDayMax[a][timediff]  = tasksSpawned;

                if task['status'] != "complete":
                    if a in self.adapterErrorsPerDay:
                        self.adapterErrorsPerDay[a][int(timediff)] += 1
                task_utime = datetime.strptime(task['start_time']+"","%b %d,%Y %H:%M:%S")
                task_epoch_start = int(time.mktime(task_utime.timetuple()))
                task_endtime = datetime.strptime(task['end_time']+"","%b %d,%Y %H:%M:%S")
                task_epoch_end = int(time.mktime(task_endtime.timetuple()))
                if task_epoch_end <= 0:
                    task_runtime = int(time.time() - task_epoch_start)
                else:
                    task_runtime = int(task_epoch_end - task_epoch_start)
                if a in self.adapterRunTimeTotalPerDay:
                    self.adapterRunTimeTotalPerDay[a][timediff] += task_runtime
                    adapterAvgRunTime = int(self.adapterRunTimeTotalPerDay[a][timediff] /  self.adapterTasksPerDay[a][timediff])
                    self.adapterRunTimeAvgPerDay[a][timediff] = adapterAvgRunTime


            # Store jobs
            self.jobs.append(job)

    def writeStats(self,ofile):
        startJob = 1
        count = len(self.jobs)
        endJob = count
        totalAvgRunTime = 0

        self.currentTime= strftime("%b %d,%Y %H:%M:%S")

        ## Calculate top 10 users
        topTenUsers= OrderedDict(sorted(self.usersByJobsSubmitted.items(), key=lambda x: x[1], reverse=True)[:10])

        o = open(ofile, 'w')

        # Header
        o.write( "{\n");

        o.write( " \"top_users_by_job_submitted\": {\n")
        first = True
        for u in topTenUsers:
            print str(u)
            o.write("     ")
            if not first:
                o.write(",")
            first = False
            o.write( "\""+u+"\" :"+str(self.usersByJobsSubmitted[u]))
            o.write("\n")
        o.write("  },\n")


        o.write(" \"adapters\": [");
        first = True
        for a in self.adapterNames:
            if not first:
                o.write(",")
            first = False
            o.write("\""+a+"\"")
        o.write("],\n")

        # misc stats
        o.write(" \"total_job_count\": "+str(self.totalJobCount)+",\n");
        o.write(" \"total_error_count\": "+str(self.totalErrorCount)+",\n");
        o.write(" \"total_task_count\":  "+str(self.totalTaskCount)+",\n");
        if self.totalJobCount >= 1:
            totalAvgRunTime = int((self.totalJobAvgRunTimeCount / self.totalJobCount)/1000)
        o.write(" \"avg_job_time\": "+str(totalAvgRunTime)+",\n");

        # Hourly
        o.write(" \"job_count_by_hour\": [ ");
        first = True
        for i in range(24):
            if not first:
                o.write(",")
            first = False
            o.write(str(self.jobsSubmittedByHour[i]))
        o.write(" ],\n")


        # ---- Graph activity ----
        min = [0]*self.numberOfDays
        max = [0]*self.numberOfDays
        avg = [0]*self.numberOfDays
        for i in range(self.numberOfDays):
            min[i] = self.graphActivityDayMin[i]
            max[i] = self.graphActivityDayMax[i]
            avg[i] = max[i]
            if self.dayJobTotal[i] > 0:
                avg[i] = int(self.graphActivityDayCount[i] / self.dayJobTotal[i])
        # Write to seperate lines, makes graphing easier
        o.write( " \"graph_activity_min\": [")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(min[i]))
        o.write( "],\n")
        o.write( " \"graph_activity_max\": [ ")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(max[i]))
        o.write( "],\n")
        o.write( " \"graph_activity_avg\": [ ")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(avg[i]))
        o.write( "],\n")


        # ---- Adapter tasks -----
        o.write( " \"adapter_tasks_per_day\": [ \n")
        firstAdapter = True
        for a in self.adapterTasksPerDay:
            if not firstAdapter:
                o.write(",\n")
            firstAdapter = False
            o.write("     {\n")
            o.write("        \"adapter\":\""+a+"\",\n")
            o.write("        \"data\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                o.write(" "+str(self.adapterTasksPerDay[a][i]))
            o.write("]\n")
            o.write("     }")
        o.write("\n ],\n")


        # --- Adapter tasksSpawned (min/max/avg) self.adapterTasksSpawnedPerDayAvg[a][timediff]
        o.write( " \"adapter_tasks_spawned_per_day\": [ \n")
        firstAdapter = True
        for a in self.adapterTasksSpawnedPerDayAvg:
            if not firstAdapter:
                o.write(",\n")
            firstAdapter = False
            o.write("     {\n")
            o.write("        \"adapter\":\""+a+"\",\n")
            ## Avg
            o.write("        \"avg\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                if a in self.adapterTasksSpawnedPerDayAvg:
                    avg = 0
                    if self.adapterTasksPerDay[a][i] > 0:
                        avg = int(self.adapterTasksSpawnedPerDayAvg[a][i]/self.adapterTasksPerDay[a][i])
                    o.write(" "+str(avg))
                else :
                    o.write (" 0")
            o.write("],\n")

            ## Min
            o.write("        \"min\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                if a in self.adapterTasksSpawnedPerDayMin:
                    o.write(" "+str(self.adapterTasksSpawnedPerDayMin[a][i]))
                else :
                    o.write(" 0")

            o.write("],\n")
            ## Max
            o.write("        \"max\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                if a in self.adapterTasksSpawnedPerDayMax:
                    o.write(" "+str(self.adapterTasksSpawnedPerDayMax[a][i]))
                else :
                    o.write(" 0")

            o.write("]\n")
            o.write("     }")
        o.write("\n ],\n")



        # ---- Errors Per Adapter Day -----
        o.write( " \"adapter_errors_per_day\": [ \n")
        firstAdapter = True
        for a in self.adapterErrorsPerDay:
            if not firstAdapter:
                o.write(",\n")
            firstAdapter = False
            o.write("     {\n")
            o.write("        \"adapter\":\""+a+"\",\n")
            o.write("        \"data\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                o.write(" "+str(self.adapterErrorsPerDay[a][i]))
            o.write("]\n")
            o.write("     }")
        o.write("\n ],\n")

        # ---- Adapter Avg Runtime Day ----
        o.write( " \"adapter_avg_runtime_day\": [ \n")
        firstAdapter = True
        for a in self.adapterRunTimeAvgPerDay:
            if not firstAdapter:
                o.write(",\n")
            firstAdapter = False
            o.write("     {\n")
            o.write("        \"adapter\":\""+a+"\",\n")
            o.write("        \"data\": [")
            first = True
            for i in range(self.numberOfDays):
                if not first:
                    o.write(",")
                first = False
                o.write(" "+str(self.adapterRunTimeAvgPerDay[a][i]))
            o.write("]\n")
            o.write("     }")
        o.write("\n ],\n")


        # ---- Job List -----
        o.write( " \"jobs_per_day\": [")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(self.dayJobTotal[i]))
        o.write( "],\n")

        # ---- Errors lists -----
        o.write(" \"errors_per_day\": [ ")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(self.dayErrorTotal[i]))
        o.write( "],\n")

        # ---- Tasks per day ----
        o.write( " \"task_total_per_day\": [ ")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            o.write( " "+str(self.dayTaskTotal[i]))
        o.write( "],\n")

        # self.dayAvgSeedSize
        o.write(" \"job_avg_seed_size_per_day\": [ ")
        first = True
        for i in range(self.numberOfDays):
            if not first:
                o.write(",")
            first = False
            avg = 0
            if self.dayJobTotal[i] > 0:
                avg = int(self.dayAvgSeedSize[i] /self.dayJobTotal[i])
            o.write( " "+str(avg))
        o.write( "],\n")

        # Write the last polliing time
        o.write(" \"last_run_time\": \""+self.currentTime+"\" \n")

        # Note if you add something here, put comma above

        # End
        o.write( "}\n")



##
## Main Processing
##
if __name__ == '__main__':

    if len(sys.argv) <= 1:
        print "Error: missing output filename."
        print ""
        print "    Please provide an output file to write json metric data."
        print "    python lgstats.py /tmp/lgmetrics.json"
        print ""
        sys.exit()

    script, outputFile = sys.argv
    print "Writing metric data to "+outputFile
    LemonGrenadeStats('http://localhost:9999',outputFile)


