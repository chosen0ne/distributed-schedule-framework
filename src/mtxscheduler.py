from apscheduler.scheduler import Scheduler
import datetime
import time
import socket
import struct
import fcntl

def get_ip(ifname):
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	return socket.inet_ntoa(fcntl.ioctl(
		s.fileno(),
		0x8915, # SIOCGIFADDR
		struct.pack('256s', ifname[:15])
	)[20:24])
	

class MutexScheduler(Scheduler):
	def __init__(self, gconfig={}, **options):
		Scheduler.__init__(self, gconfig, **options)
		self.ip = get_ip('eth0')

	def mutex(self, lock = None, heartbeat = None, lock_else = None, 
			unactive_interval = datetime.timedelta(seconds = 30)):
		
		def mutex_func_gen(func):
			def mtx_func():

				if lock:
					lock_rec = lock()
					now = datetime.datetime.now()

					# execute mutex job when the server is active, or the other server is timeout.
					if not lock_rec or lock_rec['active_ip'] == self.ip or (lock_rec['update_time'] and now - lock_rec['update_time'] >= unactive_interval):  
						if lock_rec:
							del lock_rec['active_ip']
							del lock_rec['update_time']

						if not lock_rec:
							lock_rec = {}

						lock_attrs = func(**lock_rec)
						if not lock_attrs:
							lock_attrs = {}

						# send heart beat
						heartbeat(self.ip, now, **lock_attrs)
					else: 
						lock_else(lock_rec)
				else:
					func()

			return mtx_func

		self.mtx_func_gen = mutex_func_gen

		def inner(func):
			return func

		return inner

	def cron_schedule(self, **options):
		def inner(func):
			if hasattr(self, 'mtx_func_gen'):
				func = self.mtx_func_gen(func)

			func.job = self.add_cron_job(func, **options)
			return func
		return inner

