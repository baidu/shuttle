#!/bin/env python
import os
import sys
import time
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

if len(sys.argv) < 3:
	print "python plot_job_chart.py [jobid] [imgdir]"
	sys.exit(-1)

jobid = sys.argv[1]
img_dir = sys.argv[2]

map_stat = {}
reduce_stat = {}
order_by_time = []

cmd='''./shuttle status -a %s | grep -P 'map-|reduce-' | grep -v Cancel |  grep -v Kill | grep -v Fail | awk -v OFS="\t" '{print $1,$5" "$6, $7" "$8}'
''' % (jobid,)

for line in os.popen(cmd):
	task,start_time,end_time = line.rstrip().split("\t")
	order_by_time.append((start_time, "start", task))
	if end_time != "-":
		order_by_time.append((end_time, "end", task))
	#print task,start_time,end_time

order_by_time.sort()

x_time = []
map_running = []
reduce_running = []
map_done = set()
reduce_done = set()
map_done_count = []
reduce_done_count = []

last_time = ""
for event in order_by_time: 
	the_time, action, task = event
	if task.startswith("map"):
		stat = map_stat
		done = map_done
	elif task.startswith("reduce"):
		stat = reduce_stat
		done = reduce_done
	if not task in stat:
		stat[task] = 0
	if action == "start":
		stat[task] += 1
	elif action == "end":
		stat[task] -= 1

	if stat[task] <= 0:
		del stat[task]
		done.add(task)

	#print "%s\t%d\t%d" % (the_time, len(map_stat), len(reduce_stat))
	if last_time != the_time:
		last_time = the_time
		the_time = datetime.strptime(the_time, "%Y-%m-%d %H:%M:%S")
		x_time.append(the_time)
		map_running.append(len(map_stat))
		reduce_running.append(len(reduce_stat))
		map_done_count.append(len(map_done))
		reduce_done_count.append(len(reduce_done))


fig, ax_ary = plt.subplots(2, sharex=True, figsize=(8,4))
ax_ary[0].plot(x_time, map_running, 'r-', lw=2)
ax_ary[0].set_ylabel('Running', color='r')
ax_ary[0].set_ylim([min(map_running),max(map_running) * 1.1 ])

tmp = ax_ary[0].twinx()
tmp.plot(x_time, map_done_count, 'g-', label='Map Done#', lw=2)
tmp.set_ylabel('Done', color='g')

ax_ary[1].plot(x_time, reduce_running, 'b-', label="Reduce Running#", lw=2)
ax_ary[1].set_ylabel('Running', color='b')
ax_ary[1].set_ylim([min(reduce_running), max(reduce_running) * 1.1])
tmp = ax_ary[1].twinx()
tmp.plot(x_time, reduce_done_count, 'g-', label="Reduce Done#",lw=2)
tmp.set_ylabel('Done', color='g')


ax_ary[0].set_title("Map Running Now#: %d, Done: %d" % (map_running[-1], map_done_count[-1]), fontsize=14)
ax_ary[1].set_title("Reduce Running Now#: %d, Done: %d" % (reduce_running[-1], reduce_done_count[-1]), fontsize=14)
#ax1.legend(('Map Running#', 'Reduce Running#'), 'upper left')
ax_ary[0].grid()
ax_ary[1].grid()

from matplotlib.dates import DateFormatter
formatter = DateFormatter('%H:%M')
plt.gcf().axes[0].xaxis.set_major_formatter(formatter)  
plt.gcf().axes[1].xaxis.set_major_formatter(formatter)  


#fig.autofmt_xdate()
fig.tight_layout()

fig.savefig("%s/%s.png" % (img_dir, jobid))

