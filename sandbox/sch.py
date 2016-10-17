#!/bin/env python
import os
import re
import datetime
import shelve

SLOT_TOTAL = 3200
LOWEST_SLOT = 80
SLOT_NOT_IN_WHITE_LIST = 200
SCALE_DOWN_RATIO = 0.85
VIP_SCALE_DOWN_RATIO = 0.7

USER_LIST = {
        'online': 3000,
        'offline': 700,
        'nobody': 3000,
}

VIP_LIST = ['robin']

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
    if cap < 10:
        cap = 10
    print "update", jobid, "map", cap
    os.system("./shuttle update %s -D mapred.job.map.capacity=%d" % (jobid, cap))

def SetReduceCapacity(jobid, cap):
    cap = int(cap)
    if cap < 10:
        cap = 10 
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

#running + pendings
def GetTodoTasks():
    map_todo = {}
    reduce_todo = {}
    for line in os.popen('''./shuttle list | grep job_ | awk '{split($(NF-1),A,"/"); split($(NF),B,"/");  print $1, A[1]+A[2], B[1]+B[2]}' '''):
        job_id, map_t, reduce_t = line.split()
        map_todo[job_id] = int(map_t) + LOWEST_SLOT
        reduce_todo[job_id] = int(reduce_t) + LOWEST_SLOT
    return map_todo, reduce_todo

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
        if galaxy_pending[vip] > 3:
            return True   
    return False

complet_db = shelve.open('complete.data', 'c')
scale_db = shelve.open("scale.data", 'c')
vip_down_db = shelve.open('vip_down.data', 'c')

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
    map_todo, reduce_todo = GetTodoTasks()
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
            if pend_times > 80:
                print "freeze long no complet job"
                Kill(jobid)
                continue
        else:
            complet_db[key] = 0 

        print "comp", job_name, jobid, complet_old, complet_cur 
        complet_db[jobid] = complet_cur

        print "check job:", job_name, InWhiteList(job_name)

        map_is_abuse = False
        reduce_is_abuse = False
        if cpu_ratio > 0.35 and map_c > map_total and map_c > LOWEST_SLOT:
            map_c = map_total
            map_is_abuse = True
        
        if cpu_ratio > 0.35 and reduce_c > reduce_total and reduce_c > LOWEST_SLOT:
            reduce_c = reduce_total
            reduce_is_abuse = True

        if cpu_ratio > 0.35 and map_c > USER_LIST[user_name]:
            map_c = min(USER_LIST[user_name], map_c)
            map_is_abuse = True
        
        if cpu_ratio > 0.35 and reduce_c > USER_LIST[user_name]:
            reduce_c = min(USER_LIST[user_name], reduce_c)
            reduce_is_abuse = True

        if map_is_abuse:
            print "scale down abuse:", jobid, map_c
            SetMapCapacity(jobid, min(map_c, map_todo[jobid]))
        if reduce_is_abuse:
            print "scale down abuse:", jobid, reduce_c
            SetReduceCapacity(jobid, min(reduce_c, reduce_todo[jobid]))

        user_count = sum([1 for user,res in galaxy_usage.items() if res > 0])
        avg_slot = SLOT_TOTAL / user_count
        avg_slot_by_job = SLOT_TOTAL / len(jobs)
        print "avg slot", avg_slot, avg_slot_by_job
           
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
                if (user_name not in VIP_LIST) and (galaxy_usage[user_name] > avg_slot) and vip_down_db.get(jobid,0) < 2:
                    beyond_quota = True
                    vip_down_db[jobid] = vip_down_db.get(jobid,0) + 1
                    print "give some res to vip"

            if not beyond_quota:
                continue

            print "scale down"
           
            scale_down_ratio = VIP_SCALE_DOWN_RATIO if vip_is_hungry else SCALE_DOWN_RATIO

            if map_c > USER_LIST[user_name]:
                map_c = USER_LIST[user_name]
            if map_c > avg_slot_by_job:
                SetMapCapacity(jobid, int( min(map_c * scale_down_ratio, map_todo[jobid]) ) )
            
            if reduce_c > USER_LIST[user_name]:
                reduce_c = USER_LIST[user_name]
            if reduce_c > avg_slot_by_job:
                SetReduceCapacity(jobid, int( min(reduce_c * scale_down_ratio, reduce_todo[jobid]) ) )

            if (not jobid in scale_db):
                scale_db[jobid] = (map_c, reduce_c)
        else:
            if jobid in scale_db:
                old_map_c, old_reduce_c = scale_db[jobid]
                print "scale up"
                if map_c < avg_slot and map_c < old_map_c:
                    del scale_db[jobid]
                    SetMapCapacity(jobid, max(old_map_c, USER_LIST[user_name]))
                    break

    complet_db.close()
    scale_db.close()
    vip_down_db.close()


