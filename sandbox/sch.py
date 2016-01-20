#!/bin/env python
import os
import datetime
import shelve

MAP_MAX = 1300
REDUCE_MAX = 1000
SLOT_TOTAL = 4500

def GetCPURatio():
	result = os.popen("./galaxy status | grep cpu -a2 | tail -1 | awk '{print $3/$1}'").read()
	return float(result)

def GetMemRatio():
	result = os.popen("./galaxy status | grep mem -a2 | tail -1 | awk '{print $5/$1}'").read()
	x = float(result)
	if x > 1:
		x = x / 1000.0
	return x

def GetAllJobIds():
	jobs = [] 
	for line in os.popen('./shuttle list | grep job_'):
		tup = line.split()	
		jobs.append(tup[0])
	return jobs

def GetCapacity(jobid):
	lines = os.popen("./shuttle status " + jobid + " | grep Capacity" ).readlines()
	map_c = 0 
	reduce_c = 0
	for line in lines:
		if line.find('Map Capacity:') != -1:
			map_c = int(line.split('Map Capacity: ')[1])
		if line.find('Reduce Capacity:') != -1:
			reduce_c = int(line.split('Reduce Capacity: ')[1])
	return (map_c, reduce_c)

def SetMapCapacity(jobid, cap):
	if cap < 0:
		return
	print "update", jobid, "map", cap
	os.system("./shuttle update %s -D mapred.job.map.capacity=%d" % (jobid, cap))

def SetReduceCapacity(jobid, cap):
	if cap < 0:
		return
	print "update", jobid, "reduce", cap
	os.system("./shuttle update %s -D mapred.job.reduce.capacity=%d" % (jobid, cap))

def GetJobRunningSlot():
	r_slots = {}
	for line in os.popen('''./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $1, A[1]+B[1]}' '''):
		job_id, running = line.split()
		r_slots[job_id] = int(running)
	return r_slots

def GetTotalTasks():
	map_total = {}
	reduce_total = {}
	for line in os.popen('''./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $1, A[1]+A[2]+A[3], B[1]+B[2]+B[3]}' '''):
		job_id, map_t, reduce_t = line.split()
		map_total[job_id] = map_t
		reduce_total[job_id] = reduce_t
	return map_total, reduce_total

def GetJobNames():
	job_names = {}
	for line in os.popen('''./shuttle list | grep job_ | awk '{print $1,$2}' '''):
		job_id, name = line.split()
		job_names[job_id] = name
	return job_names

def GetCompleteAmount():
	complet_amounts = {}
	for line in os.popen('''./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $1, A[3]+B[3]}' '''):
		job_id, complet_amount = line.split()
		complet_amounts[job_id] = int(complet_amount)
	return complet_amounts

def InWhiteList(job_name):
	white_list = ('tom', 'jerry', 'frog')
	for w in white_list:
		if job_name.startswith(w):
			return True
	return False

complet_db = shelve.open('complete.data', 'c')

if __name__ == "__main__":
	jobs = []
	map_c_all = []
	reduce_c_all = []
	for jobid in GetAllJobIds():
		map_c, reduce_c = GetCapacity(jobid)
		jobs.append(jobid)
		map_c_all.append(map_c)
		reduce_c_all.append(reduce_c)
	map_c_sum = sum(map_c_all)
	reduce_c_sum = sum(reduce_c_all)
	i = 0
	cpu_ratio = GetCPURatio()
	mem_ratio = GetMemRatio()
	running_slots = GetJobRunningSlot()
	map_total, reduce_total = GetTotalTasks()
	job_names = GetJobNames()
	#print running_slots
	#print map_total, reduce_total
	running_total = sum(running_slots.values()) 
	
	complet_amounts = GetCompleteAmount()

	print "================================================================="

	print "cpu:", cpu_ratio, "mem:", mem_ratio, "running-total:", running_total
	for jobid in jobs:
		map_c = map_c_all[i]
		reduce_c = reduce_c_all[i]
		i+=1

		job_name = job_names[jobid]
		print "-----------------------------------------------------------"
		print datetime.datetime.now(), jobid, job_name, map_c, reduce_c

		complet_cur = complet_amounts[jobid]
		complet_old = complet_db.get(jobid, 0)
		if complet_cur == complet_old:
			key = 'PEND#' + jobid
			complet_db[key] = complet_db.get(key,0) + 1
			print "PEND:", complet_db[key]
			if complet_db[key] > 30:
				print "freeze long no complet job"
				SetMapCapacity(jobid, 0)
				SetReduceCapacity(jobid, 0)
				continue
		print "comp", job_name, jobid, complet_old, complet_cur 
		complet_db[jobid] = complet_cur

		print "check job:", job_name, InWhiteList(job_name)
		if cpu_ratio > 0.35 and (not InWhiteList(job_name)) :
			print "scale down, not in whitelist"
			SetMapCapacity(jobid, min(map_c, 100))
			SetReduceCapacity(jobid, min(reduce_c,60))
			continue

		if map_c > map_total[jobid] * 2:
			print "scale down abuse"
			SetMapCapacity(jobid, int(map_c * 0.8))
		if reduce_c > reduce_total[jobid] * 2:
			print "scale down abuse"
			SetReduceCapacity(jobid, int(reduce_c * 0.8))

		if running_total > SLOT_TOTAL or cpu_ratio > 0.83 or mem_ratio > 0.92:
			print "scale down"
			if map_c > MAP_MAX:
				SetMapCapacity(jobid, int(map_c * 0.8))
			if reduce_c > REDUCE_MAX:
				SetReduceCapacity(jobid, int(reduce_c * 0.8))

	complet_db.close()


