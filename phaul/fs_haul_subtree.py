#
# FS haul driver, that copies the subtree from
# one node to another using rsync. It's used in
# legacy OpenVZ configurations.
#

import subprocess as sp
import os
import logging
import time			# to count time in __runsync()
#import images
import tarfile
import threading
import shutil
import util
from images import opendir

rsync_log_file = "rsync.log"
tar_sync_log_file = "tar-sync.log"


class untar_thread(threading.Thread):

	def __init__(self, sk, tdir="/dir-name-not-given"):
		threading.Thread.__init__(self)
		self.__sk = sk
		self.__dir = tdir

	def run(self):
		try:
			tf_fileobj = util.tarfile_fileobj_wrap(self.__sk)

			tf = tarfile.open(mode="r|gz", fileobj=tf_fileobj)

			logging.info("lele: extract to dir %s", self.__dir)
			if len(tf.getmembers()) < 1:
				logging.exception("Nothing in package")
			for file_ in tf.getmembers():
				logging.info("file: ", file_.name)

			tf.extractall(self.__dir)
			tf.close()
			logging.info("untar_thread: done extract.")
			tf_fileobj.discard_unread_input()
		except:
			logging.exception("Exception in untar_thread")



class fs_tar:
	def __init__(self, sk, dirname):
		tf_fileobj = util.tarfile_fileobj_wrap(sk)
		self.__tf = tarfile.open(mode="w|gz", fileobj=tf_fileobj)
		self.__dir = dirname

	def add(self, path):
		if not path:
			path = os.path.join(self.__dir)

		self.__tf.add(path)

	def pack(self):
		if not self.__dir:
			logging.exception("Exception in fs_tar when packing")
			return
		logging.info("lele: now pack dir: %s", self.__dir)
		self.__tf.add(self.__dir)

	def close(self):
		self.__tf.close()

# lele: added according to service.py, image.py, and p_haul_docker.py
# act as a file sys receiver on the target node
# it will receive the tar stream from source node and unpack it on the target node
class fs_receiver:
	
	def __init__(self):	
		self.current_iter = 0
		self.sync_time = 0.0
		self._keep_on_close = True
		self._current_dir = None

	def close(self):
		
		if self._current_dir:
			self._current_dir.close()

		if not self._keep_on_close:
			logging.info("Removing dirs")
			#shutil.rmtree(self._wdir.name())
			for dir_name in self.__roots:
				shutil.rmtree(dir_name)
				logging.info("removed: %s ", dir_name)
		else:
			for dir_name in self.__roots:
				logging.info("dirs are kept in %s ", dir_name)
		pass

	
	def create_dir(self, dirname):

		if os.path.isdir(dirname):
			logging.exception("Exception in fs_receiver:create_dir: %s already exists", 
					dirname)
		
		self.current_iter += 1
		logging.info("\tlele: create_dir(target node): Making directory %s", dirname)
		os.mkdir(dirname)
		self._current_dir = opendir(dirname)

	def start_accept_dirs(self, dir_name , sk):

		if not os.path.isdir(dir_name):
		     self.create_dir(dir_name)

		self.__acc_tar = untar_thread(sk, dir_name)
		self.__acc_tar.start()
		logging.info("Started directories receiving server")

	def stop_accept_dirs(self):
		logging.info("Waiting for directories to unpack")
		self.__acc_tar.join(360)
		if self.__acc_tar.isAlive():
			logging.exception("Exception: untar timeout")
		logging.info("unpack done")


class p_haul_fs:

	def __init__(self, subtree_paths, compression = True):
		self.__compression=compression
		self.__roots = []
		for path in subtree_paths:
			logging.info("Initialized subtree FS hauler (%s)", path)
			self.__roots.append(path)

		self.__thost = None
		self._keep_on_close = False	# lele: added refer to images.py
		self.sync_time = 0.0

	def set_options(self, opts):
		self.__thost = opts["to"]

	def set_work_dir(self, wdir):
		self.__wdir = wdir
	
	
	def __run_rsync(self):
		logf = open(os.path.join(self.__wdir, rsync_log_file), "a+")
		logging.info("lele: start __run_rsync(), logging file: %s ", 
					os.path.join(self.__wdir, rsync_log_file))
		for dir_name in self.__roots:

			dst = "%s:%s" % (self.__thost, os.path.dirname(dir_name))

			# First rsync might be very long. Wait for it not
			# to produce big pause between the 1st pre-dump and
			# .stop_migration

			#ret = sp.call(["rsync", "-va", "-e", "'ssh -p2342'", dir_name, dst],
			#	stdout = logf, stderr = logf)
			#logging.info("lele: run command: rsync -v -a -e 'ssh -p 2342' %s %s.\n", 
			#		dir_name, str(dst))
			cmdstring = "rsync -a -v -e 'ssh -p22' --timeout=7200 " + dir_name + " " + dst
			logging.info("lele: cmd: %s", cmdstring)

			#exit()
			ret = sp.call(cmdstring,
				stdout = logf, stderr = logf, shell=True)
			
			if ret != 0:
				raise Exception("Rsync failed")
			#__pack_and_sync(dir_name, dst)
		
		
	def start_migration(self):
		logging.info("Starting FS migration")
		logging.info("lele: start timing1")
		startFS=time.time()
		if(self.__compression):
			#self.__run_sync3()
			self.__run_sync_layers()
		else:
			self.__run_sync()
		endFS=time.time()
		logging.info("lele: end timing1 (FS mig): %s", str(endFS - startFS))
		return None

	def next_iteration(self):
		return None

	def stop_migration(self):
		logging.info("Doing final FS sync")
		logging.info("lele: start timing2")
		startFS=time.time()
		self.__run_rsync()
		endFS=time.time()
		logging.info("lele: end timing2(final FS mig): %s", str(endFS - startFS))
		return None

	def __run_sync3(self):
		logf = open(os.path.join(self.__wdir, rsync_log_file), "a+")
		logging.info("lele: start __run_sync3(), use bash tar command over ssh logging file: %s ", 
					os.path.join(self.__wdir, rsync_log_file))
		for dir_name in self.__roots:

			#dst = "%s:%s" % (self.__thost, os.path.dirname(dir_name))
			dst = self.__thost
			# http://meinit.nl/using-tar-and-ssh-to-efficiently-copy-files-preserving-permissions
			
			cmdstring = "tar jcpf - " + dir_name + " | ssh " + dst + " -p 22 'tar jxpf - -C /'"

			logging.info("lele: cmd: %s", cmdstring)

			#exit()
			ret = sp.call(cmdstring,
				stdout = logf, stderr = logf, shell=True)
			
			if ret != 0:
				raise Exception("tar over ssh failed")
		
	def __run_sync_layers(self):
		logf = open(os.path.join(self.__wdir, rsync_log_file), "a+")
		logging.info("lele: start __run_sync3(), use bash tar command over ssh logging file: %s ", 
					os.path.join(self.__wdir, rsync_log_file))
		for dir_name in self.__roots:

			#dst = "%s:%s" % (self.__thost, os.path.dirname(dir_name))
			dst = self.__thost
			# http://meinit.nl/using-tar-and-ssh-to-efficiently-copy-files-preserving-permissions
			
			cmdstring = "tar jcpf - " + dir_name + " | ssh " + dst + " -p 22 'tar jxpf - -C /'"

			logging.info("lele: cmd: %s", cmdstring)

			#exit()
			ret = sp.call(cmdstring,
				stdout = logf, stderr = logf, shell=True)
			
			if ret != 0:
				raise Exception("tar over ssh failed")
		

	# lele: pack root_dirs and send to target and unpack
	# changed based on images.py
	def __run_rsync2(self, target_host, htype, sk):
		"""Via RPC, First pack image and send to target, and ask target to unpack;
		similar to img sending in images.py:sync_imgs_to_target
		"""
		logging.info("Sending fs directories to target")

		logging.info("lele: start __run_rsync2()")
		
		for dir_name in self.__roots:

			logging.info("Packing and sending fs directory: %s", dir_name)

			logging.info("lele: start counting time1")

			start = time.time()

			target_host.start_accept_dirs(dir_name)
			tf = fs_tar(sk, dir_name)
			tf.pack()
			logging.info("Finished packing.")
			tf.close()
			target_host.stop_accept_dirs()

			endTime = time.time()
			self.sync_time = endTime - start
			logging.info ("\nlele: time for pack and send dir to target: %s", str(self.sync_time))
			

	def start_migration2(self, target_host, htype, sk):
		logging.info("Starting FS migration (pack and rpc sending)")
		logging.info("lele: start timing1")
		startFS=time.time()
		self.__run_rsync2(target_host,htype,sk)
		endFS=time.time()
		logging.info("lele: end timing1 (FS pack and send): %s", str(endFS - startFS))
		return None

	def stop_migration2(self, target_host, htype, sk):
		logging.info("Doing final FS pack and send")
		logging.info("lele: start timing2")
		startFS=time.time()
		self.__run_rsync2(target_host, htype, sk)
		endFS=time.time()
		logging.info("lele: end timing2(final FS pack and send): %s", str(endFS - startFS))
		return None

	# When rsync-ing FS inodes number will change
	def persistent_inodes(self):
		return False
