#!/usr/bin/env python
'''
Adapter version of LGTOP, needs help


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
        self.adapters = []
        self.sortOn = 'name'
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
        self.adapters = sorted(self.adapters, key=itemgetter(sortfield),reverse=self.sortOrder)

    def load(self):
        self.height,self.width = self.screen.getmaxyx()
        maxload = self.height - self.padding
        self.adapters = []
        url = self.server + "/api/adapters"
        r   = requests.get(url);
        rawadapters = json.loads(r.text)
        print rawadapters
        for j in rawadapters:
            adapter = {}
            adapter['id'] = j
            adapter['status'] = rawadapters[j]['status']
            adapter['uptime'] = rawadapters[j]['uptime']
            adapter['name']   = rawadapters[j]['name']
            adapter['last_hb']= rawadapters[j]['last_hb']
            self.adapters.append(adapter)

    def draw(self):
        self.currentTime= strftime("%b %d,%Y %H:%M:%S")
        self.height,self.width = self.screen.getmaxyx()
        self.max_display_adapters = self.height - self.padding
        self.screen.clear()
        startAdapter = 1
        count = len(self.adapters)
        if (count < self.max_display_adapters):
            endAdapter = count
        else:
            endAdapter = startAdapter + self.max_display_adapters
        self.sortBy(self.sortOn)

        # ---- Header -----
        direction = "Descending"
        if not self.sortOrder:
            direction = "Ascending"
        self.screen.addstr(0,0,"LEMONGRENADE                                          [Displaying "+str(endAdapter)+" adapters] [sort:"+self.sortOn+" "+direction+"]  Time: "+self.currentTime)
        self.screen.addstr(1,0,"              Adapter                   ", curses.A_REVERSE)
        self.screen.addstr(1,30," Status                  ", curses.A_REVERSE)
        self.screen.addstr(1,40," HeartBeat                  ", curses.A_REVERSE)
        self.screen.addstr(1,62," Runtime        ", curses.A_REVERSE)
        self.screen.addstr(1,78," FullName                                                                    ", curses.A_REVERSE)

        # ---- Adapter List -----
        line = 2
        count = 0
        for adapter in self.adapters:
            count += 1
            if count <= endAdapter:
                self.screen.addstr(line, 0, str(adapter['name']))

                if (adapter['status'] == 'ONLINE'):
                    self.screen.addstr(line, 30, str(adapter['status']), curses.A_REVERSE)
                else:
                    self.screen.addstr(line, 30, str(adapter['status']))
                self.screen.addstr(line, 46, str(adapter['last_hb']))
                self.screen.addstr(line, 60, str(adapter['uptime']))
                self.screen.addstr(line, 80, adapter['id'] )

                line += 1
        self.screen.addstr(self.height-1,0, "                   (q)uit  (Space)Refresh  Sort [(s)tatus                             (-)Order ]                                   ", curses.A_REVERSE)
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
                #if c == 'g':
                #    self.sortBy('graph_activity')
                #    self.draw()
                #if c == 'c':
                #    self.sortBy('starttime')
                #    self.draw()
                if c == 's':
                    self.sortBy('status')
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

