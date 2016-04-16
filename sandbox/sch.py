#!/bin/env python
import os
import re
import datetime
import shelve

SLOT_TOTAL = 6000
LOWEST_SLOT = 80
SLOT_NOT_IN_WHITE_LIST = 200
SCALE_DOWN_RATIO = 0.85
VIP_SCALE_DOWN_RATIO = 0.7

USER_LIST = {
        'online': 2500,
        'offline': 1600,
        'besteffor': 1000
        'nobody': 300
}

VIP_LIST = ['bigbrother']

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

def Kill(jobid):
    print "kill",jobid
    os.system('./shuttle kill %s' % (jobid,))

def SetMapCapacity(jobid, cap):
    cap = int(cap)
    if cap < 50:
        return
    print "update", jobid, "map", cap
    os.system("./shuttle update %s -D mapred.job.map.capacity=%d" % (jobid, cap))

def SetReduceCapacity(jobid, cap):
    cap = int(cap)
    if cap < 50:
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
        map_total[job_id] = int(map_t)
        reduce_total[job_id] = int(reduce_t)
    return map_total, reduce_total

def GetPendingTasks():
    map_pending = {}
    reduce_pending = {}
    for line in os.popen('''./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $1, A[2], B[2]}' '''):
        job_id, map_t, reduce_t = line.split()
        map_pending[job_id] = int(map_t)
        reduce_pending[job_id] = int(reduce_t)
    return map_pending, reduce_pending

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
    for w in USER_LIST.keys():
        if job_name.startswith(w):
            return True
    return False


def GetUserNameFromJobName(job_name):
    tup = re.split(r'[-_ ]', job_name)
    user_name = tup[0]
    if user_name not in USER_LIST:
        return "nobody"
    return user_name

def GetUserUsage():
    users_usage = {}
    for line in os.popen(''' ./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $2, A[1]+int(B[1]*1.5)}' '''):
        if line.strip()=="":
            continue
        job_name, slots = line.split()
        user_name = GetUserNameFromJobName(job_name)
        users_usage[user_name] = users_usage.get(user_name, 0) + int(slots)
    return users_usage

def GetUserGalaxyUsage():
    users_usage = {}
    for line in os.popen(''' ./galaxy jobs | grep kJobNormal | awk '{print $3, $6}' '''):
        if line.strip()=="":
            continue
        job_name, slots = line.split()
        user_name = GetUserNameFromJobName(job_name)
        users_usage[user_name] = users_usage.get(user_name, 0) + int(slots)
    return users_usage

def GetUserGalaxyPending():
    users_pending = {}
    for line in os.popen(''' ./galaxy jobs | grep kJobNormal | awk '{split($5,A,"/");print $3, A[2]}' '''):
        if line.strip()=="":
            continue
        job_name, slots = line.split()
        user_name = GetUserNameFromJobName(job_name)
        users_pending[user_name] = users_pending.get(user_name, 0) + int(slots)
    return users_pending


def VIPIsHungry(galaxy_pending):
    for vip in VIP_LIST:
        if vip not in galaxy_pending:
            continue
        if galaxy_pending[vip] > 2:
            return True   
    return False

complet_db = shelve.open('complete.data', 'c')
scale_db = shelve.open("scale.data", 'c')

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
    map_pending, reduce_pending = GetPendingTasks()
    job_names = GetJobNames()
    #print running_slots
    #print map_total, reduce_total
    running_total = sum(running_slots.values()) 
    
    complet_amounts = GetCompleteAmount()

    users_usage = GetUserUsage()
    galaxy_usage = GetUserGalaxyUsage()
    galaxy_pending = GetUserGalaxyPending()

    print "================================================================="

    print "cpu:", cpu_ratio, "mem:", mem_ratio, "running-total:", running_total
    print "shuttle usage:", users_usage
    print "galaxy usage:", galaxy_usage
    print "galaxy pending:", galaxy_pending

    for jobid in jobs:
        map_c = map_c_all[i]
        reduce_c = reduce_c_all[i]
        i+=1

        job_name = job_names[jobid]
        user_name = GetUserNameFromJobName(job_name)
        print "-----------------------------------------------------------"
        print datetime.datetime.now(), jobid, job_name, user_name, map_c, reduce_c

        complet_cur = complet_amounts[jobid]
        complet_old = complet_db.get(jobid, 0)
        key = 'PEND#' + jobid

        if complet_cur == complet_old:
            complet_db[key] = complet_db.get(key,0) + 1
            pend_times = complet_db[key]
            print "PEND:", pend_times
            if pend_times > 70:
                print "freeze long no complet job"
                Kill(jobid)
                continue
        else:
            complet_db[key] = 0 

        print "comp", job_name, jobid, complet_old, complet_cur 
        complet_db[jobid] = complet_cur

        print "check job:", job_name, InWhiteList(job_name)

        if cpu_ratio > 0.35 and map_c > USER_LIST[user_name]:
            print "scale down abuse", map_c
            SetMapCapacity(jobid, USER_LIST[user_name])
        
        if cpu_ratio > 0.35 and reduce_c > USER_LIST[user_name]:
            print "scale down abuse", reduce_c
            SetReduceCapacity(jobid, USER_LIST[user_name])

        avg_slot = SLOT_TOTAL / len(jobs)
        print "avg slot", avg_slot
           
        vip_is_hungry = VIPIsHungry(galaxy_pending)

        if running_total > SLOT_TOTAL or cpu_ratio > 0.75 or mem_ratio > 0.8 or vip_is_hungry:
            adjust_flag = False
            beyond_quota = True
            if user_name in USER_LIST and user_name in users_usage:
                the_quota = USER_LIST[user_name]
                the_use = users_usage[user_name]
                the_galaxy_use = galaxy_usage[user_name]
                if the_use > the_quota or the_galaxy_use > the_quota:
                    beyond_quota = True
                    print user_name, "beyond quota", the_use, the_galaxy_use, the_quota
                else:
                    beyond_quota = False
            else:
                USER_LIST[user_name] = SLOT_NOT_IN_WHITE_LIST

            if vip_is_hungry:
                print "vip is hungry"
                if user_name not in VIP_LIST:
                    beyond_quota = True

            if not beyond_quota:
                continue

            print "scale down"
           
            scale_down_ratio = VIP_SCALE_DOWN_RATIO if vip_is_hungry else SCALE_DOWN_RATIO
            if map_pending[jobid] >= 0:
                if map_c > USER_LIST[user_name]:
                    map_c = USER_LIST[user_name]
                SetMapCapacity(jobid, int(map_c * scale_down_ratio))
                adjust_flag = True
            if reduce_pending[jobid] >= 0:
                if reduce_c > USER_LIST[user_name]:
                    reduce_c = USER_LIST[user_name]
                SetReduceCapacity(jobid, int(reduce_c * scale_down_ratio))
                adjust_flag = True
            if (not jobid in scale_db) and adjust_flag:
                scale_db[jobid] = (map_c, reduce_c)
        else:
            if jobid in scale_db:
                old_map_c, old_reduce_c = scale_db[jobid]
                print "scale up"
                if map_c < avg_slot and map_c < old_map_c and map_pending[jobid]>0:
                    del scale_db[jobid]
                    SetMapCapacity(jobid, old_map_c)
                if reduce_c < avg_slot and reduce_c < old_reduce_c and reduce_pending[jobid]>0:
                    del scale_db[jobid]
                    SetReduceCapacity(jobid, old_reduce_c)

    complet_db.close()
    scale_db.close()


