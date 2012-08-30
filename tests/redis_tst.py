import apscheduler.events
import redis
import datetime
import time
import sys

sys.path.append('../src/')
import mtxscheduler

sched = mtxscheduler.MutexScheduler()

pool = redis.ConnectionPool(host = '127.0.0.1', port = 6379)
lock_store = redis.Redis(connection_pool = pool)

def lock():
	print 'lock()'
	lck = lock_store.hgetall('lockstore')
	if lck and lck['update_time']:
		lck['update_time'] = datetime.datetime.strptime(lck['update_time'][:19], '%Y-%m-%d %H:%M:%S')

	return lck

def hb(ip, now, **attrs):
	print 'heartbeat()'
	attrs['active_ip'] = ip
	attrs['update_time'] = now

	lock_store.hmset('lockstore', attrs)


def le(lock_rec):
	if lock_rec:
		print 'active ip', lock_rec['active_ip']
	else:
		print 'no lock rec'


def err_listener(ev):
	if ev.exception:
		print sys.exc_info()


i = 0

@sched.mutex(lock = lock, heartbeat = hb, lock_else = le)
@sched.cron_schedule(second = '*')
def job(**attr):
	global i
	i += 1
	print i

sched.add_listener(err_listener, apscheduler.events.EVENT_JOB_ERROR)
sched.start()

time.sleep(10)
