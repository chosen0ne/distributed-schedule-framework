import apscheduler.events
import datetime
import time
import pymongo
import sys

sys.path.append('../src/')

import mtxscheduler

sched = mtxscheduler.MutexScheduler()

mongo = pymongo.Connection(host = '127.0.0.1', port = 27017)
lock_store = mongo['lockstore']['locks']

def lock():
	print 'lock()'
	now = datetime.datetime.now() - datetime.timedelta(seconds = 3)
	lck = lock_store.find_one({'name': 't'})
	return lck

def hb(ip, now, **attrs):
	print 'heartbeat()'
	attrs['active_ip'] = ip
	attrs['update_time'] = now
	lock_store.update({'name': 't'}, {'$set': attrs}, upsert = True)

def le(lock_rec):
	if lock_rec:
		print 'active ip', lock_rec['active_ip']
	else:
		 print 'lock else'

i = 0

@sched.mutex(lock = lock, heartbeat = hb, lock_else = le)
@sched.cron_schedule(second = '*')
def job(**attr):
	global i
	i += 1
	print i

def err_listener(ev):
	if ev.exception:
		print sys.exc_info()
			
sched.add_listener(err_listener, apscheduler.events.EVENT_JOB_ERROR)

sched.start()
time.sleep(10)
