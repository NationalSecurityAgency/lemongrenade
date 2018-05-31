#!/usr/bin/env python
'''
Provids a simple TOP like viewer into LEMONGRENADE

You need to install curses to get this to run   'yum install python-curses'


'''
import curses
from operator import itemgetter
import json
import os
import json
import calendar
import time
import datetime
import requests 
from datetime import datetime

from time import gmtime, strftime



class LemonGrenadeTop(object):

    def __init__(self, server):
        self.server = server
        self.screen = curses.initscr()
        self.curr_topline = 0
        self.jobs = []
        self.sortOn = 'starttime'
        self.sortOrder= True
        self.padding = 2
        self.currentTime= strftime("%b %d,%Y %H:%M:%S")

    def top(self):
        self.curr_topline = 0
        self.draw()

    def sortOrderChange(self):
        self.sortOrder = not self.sortOrder

    def sortBy(self,field):
        self.sortOn = field
        sortfield = field
        if field == 'starttime':
            sortfield='utime_starttime';
        self.jobs = sorted(self.jobs, key=itemgetter(sortfield),reverse=self.sortOrder)

    def load(self):
        self.height,self.width = self.screen.getmaxyx()
        maxload = self.height - self.padding
        self.jobs = []
        url = self.server + "/api/jobs/last/"+str(maxload)
        r   = requests.get(url);
        rawjobs = json.loads(r.text)
        for j in rawjobs:
            job = {}
            job['job_id'] = str(rawjobs[j]['job_id'])
            job['status'] = rawjobs[j]['status']
            job['starttime'] = rawjobs[j]['starttime']
            job['utime_starttime'] = datetime.strptime(job['starttime']+"","%b %d,%Y %H:%M:%S")
            job['endtime'] = rawjobs[j]['endtime']
            job['graph_activity'] = rawjobs[j]['graph_activity']
            job['task_count'] = rawjobs[j]['task_count']
            job['active_task_count'] = rawjobs[j]['active_task_count']
            job['error_count'] = rawjobs[j]['error_count']
            job['job_config'] = rawjobs[j]['job_config']
            job['user'] = "unknown"

            job_config = rawjobs[j]['job_config']
            if 'roles' in job['job_config']:
                roles = job['job_config']['roles']
                for r in roles:
                    if roles[r]['owner']:
                        user = r[3:20]
                        job['user'] = user
            self.jobs.append(job)

    def draw(self):
        self.currentTime= strftime("%b %d,%Y %H:%M:%S")
        self.height,self.width = self.screen.getmaxyx()
        self.max_display_jobs = self.height - self.padding
        self.screen.clear()
        startJob = 1
        count = len(self.jobs)
        if (count < self.max_display_jobs):
            endJob = count
        else:
            endJob = startJob + self.max_display_jobs
        self.sortBy(self.sortOn)

        # ---- Header -----
        direction = "Descending"
        if not self.sortOrder:
            direction = "Ascending"
        self.screen.addstr(0,0,"LEMONGRENADE                                          [Displaying "+str(endJob)+" jobs] [sort:"+self.sortOn+" "+direction+"]  Time: "+self.currentTime)
        self.screen.addstr(1,0,"              Job ID                    ", curses.A_REVERSE)
        self.screen.addstr(1,40," Status       ", curses.A_REVERSE)
        self.screen.addstr(1,54," Graph       ", curses.A_REVERSE)
        self.screen.addstr(1,62," Tasks         ", curses.A_REVERSE)
        self.screen.addstr(1,72," Errors      ", curses.A_REVERSE)
        self.screen.addstr(1,78," RunTime     ", curses.A_REVERSE)
        self.screen.addstr(1,90," User                 ", curses.A_REVERSE)
        self.screen.addstr(1,110,"  Start Time        ", curses.A_REVERSE)
 
        # ---- Job List -----
        line = 2
        count = 1
        for job in self.jobs:
            count += 1
            if count < endJob:
                utime = datetime.strptime(job['starttime']+"","%b %d,%Y %H:%M:%S")
                epoch_start = int(time.mktime(utime.timetuple()))
                endtime = datetime.strptime(job['endtime']+"","%b %d,%Y %H:%M:%S") 
                epoch_end = int(time.mktime(endtime.timetuple()))
                if epoch_end <= 0:
                    runtime = int(time.time() - epoch_start)
                else:
                    runtime = int(epoch_end - epoch_start)
                self.screen.addstr(line, 0, job['job_id'] )
                if (job['status'] == 'PROCESSING'):
                    self.screen.addstr(line, 40, str(job['status']), curses.A_REVERSE)
                elif (job['status'] == 'FINISHED_WITH_ERRORS'):
                    self.screen.addstr(line, 40, 'FINISHED_ERR', curses.A_REVERSE)
                else:
                    self.screen.addstr(line, 40, str(job['status']))
                self.screen.addstr(line, 55, str(job['graph_activity']))
                self.screen.addstr(line, 65, str(job['active_task_count'])+"/"+str(job['task_count']))
                self.screen.addstr(line, 75, str(job['error_count']))
                self.screen.addstr(line, 82, str(runtime))
                self.screen.addstr(line, 90, job['user'])
                self.screen.addstr(line, 110, str(job['starttime']))

                line += 1
        self.screen.addstr(self.height-1,0, "                   (q)uit  (Space)Refresh  Sort [(s)tatus (g)raphsize (c)reate (J)ob (-)Order ]                                   ", curses.A_REVERSE)
        self.screen.refresh()


    def main_loop(self, stdscr):
        stdscr.clear()
        self.screen.nodelay(True)
        self.load()
        self.draw()
        lastpoll=0
        while True:
            c=self.screen.getch()
            try:
                c = chr(c)
                if c == ' ':
                    self.load()
                    self.draw()
                if c == 'g':
                    self.sortBy('graph_activity')
                    self.draw()
                if c == 'c':
                    self.sortBy('starttime')
                    self.draw()
                if c == 's':
                    self.sortBy('status')
                    self.draw()
                if c == 'j':
                    self.sortBy('job_id')
                    self.draw()
                if c == '-':
                    self.sortOrderChange()
                    self.draw()
                if c == 'q':
                    break;
            except ValueError:
                donothin = 1

            time.sleep(.5)
            lastpoll += 1
            if (lastpoll > 10):
                self.load()
                self.draw()
                self.screen.refresh()
                lastpoll = 0

if __name__ == '__main__':
    c = LemonGrenadeTop('http://localhost:9999')
    curses.wrapper(c.main_loop)

