#
# Docker container hauler
#

import os
import logging
import time
import signal
import fs_haul_subtree
import fs_haul_docker
import docker_container as dc
import json
import subprocess as sp
from subprocess import PIPE

# TODO use docker-py
# import docker


# Some constants for docker
docker_bin = "/usr/bin/docker"
docker_dir = "/var/lib/docker/0.0/"
docker_run_meta_dir = "/var/run/docker/execdriver/native"

restore_log_name = "docker_restore.log"


class p_haul_type:
	def __init__(self, ctid):

		self.backup_dir_target = None
		self.backup_dir_source = None
		# TODO ctid must > 3 digit; with docker-py, we can also resolve
		#  container name
		if len(ctid) < 3:
			raise Exception("Docker container ID must be > 3 digits")

		self._ctid = ctid
		self._ct_rootfs = ""
		self._nocompression = False

		self._fs = None

		self._layer_map = None
		self.load_layer_map()

		
		# on target node
		self._target_rootfs_id = ""
		self.down_time = 0.0

	def init_src(self):

		self._on_source = True

		# self.full_ctid = self.get_full_ctid()

		# on source node get rootfs_id and dc object
		if self._nocompression:
			self.full_ctid = self.get_full_ctid()
		else:
			self.layerIDs_stack = self._get_layerIDs_stack()
			self.full_ctid = self._dockerCon._full_cid

		# on src node, load dc objects and add sync directories to a list self._sync_dirs
		self.__load_sync_dirs()


	def __load_sync_dirs(self):

		# on source node, maintain all the dirs need to sync
		self._sync_dirs = []
		
		self.__load_ct_config()
		self._ct_rootfs = self.__load_ct_rootfs()

		# on source node, add diff dir to sync
		if(not self._nocompression):
			self._ct_diff_layer_dirs = self.__load_ct_diff_layer_dirs()

		
	def init_dst(self):
		self._on_source = False

	def adjust_criu_req(self, req):
		"""Add module-specific options to criu request"""
		pass

	def root_task_pid(self):
		# Do we need this for Docker?
		return self.full_ctid

	# Each docker container has 3 directories that need to be
	# migrated: 
	#	- (1) root filesystem layer (usually empty after stopped), 
	#	- (2) the thin container layer ( after stopped, store in /aufs/diff/rootfs_id/)
	#	- (3) image layer stack IDs, with the original IDs
	#	- (4) container configuration,
	# 	- (5) runtime meta state. We have to do this in two steps on
	# restore, so it is separated into two +1  methods.

	# only for source node.
	# on target node, use get_ct_rootfs(rootfsID)
	def __load_ct_rootfs(self):
		with open(os.path.join(self._ct_run_meta_dir, "state.json")) as data_file:
			data = json.load(data_file)
			ct_rootfs = data["config"]["rootfs"]
		logging.info("Container rootfs: %s", ct_rootfs)
		return ct_rootfs

	# for target node:
	# given rootfs_id, return the path /aufs/mnt/rootfs_id
	def get_ct_rootfs(self, rootfs_id):

		self._ct_rootfs = os.path.join(docker_dir, "aufs", "mnt", rootfs_id)
		# logging.info("Container rootfs: %s", self._ct_rootfs)
		return self._ct_rootfs

	# only in source node
	def __load_ct_diff_layer_dirs(self):

		ct_diff_layer_dir = self._layer_map.get_diff_layer_dir(self._dockerCon._rootfs_id)
		ct_diff_layer_dir_init = ct_diff_layer_dir + "-init"
		
		logging.info("Container layer diff dir: %s", ct_diff_layer_dir)
		# logging.info("Container layer diff init dir: %s", ct_diff_layer_dir_init)

		return [ct_diff_layer_dir, ct_diff_layer_dir_init]

	# used both on target and source node.
	def __load_ct_config(self):
		self._ct_config_dir = os.path.join(docker_dir, "containers", self.full_ctid)
		self._ct_run_meta_dir = os.path.join(docker_run_meta_dir, self.full_ctid)
		self._ct_image_db = os.path.join(docker_dir, "image", "aufs", "layerdb", "mounts", self.full_ctid)
		# logging.info("Container config: %s", self._ct_config_dir)
		# logging.info("Container meta: %s", self._ct_run_meta_dir)
		# logging.info("Container image db: %s", self._ct_image_db)


	def load_layer_map(self):
		self._layer_map = dc.docker_layer_map()
		logging.info("p_haul_docker.py: load_layer_map() done.")
		# logging.info("get layerdb sha256 dir: " + self._layer_map._layerdb_sha256_dir)
		# logging.info("get layerdb layer-id : cache-id ")
		# for key, value in self._layer_map._cache_ids.items():
		# 	logging.info("\t" + key + " : " + value)
		# logging.info("get layerdb cache-id : layer-ids ")
		# for key, value in self._layer_map._layer_ids.items():
		# 	logging.info("\t" + key + " : " + value)


	# used at source node: 
	def _get_layerIDs_stack(self):

		conName = self._ctid

		self._dockerCon = dc.docker_container(conName)

		# logging.info("get ID: " + self._dockerCon._full_cid)
		# logging.info("is running: " + str(self._dockerCon._is_running))
		# logging.info("get config dir: " + self._dockerCon._config_dir)
		# logging.info("get rootfs id: " + self._dockerCon._rootfs_id)
		# logging.info("get rootfs path: " + self._dockerCon._rootfs_path)
		# logging.info("get run meta dir: " + self._dockerCon._run_meta_dir)
		# logging.info("get imagedb content/sha256 dir: " + self._dockerCon._imagedb_content_dir)
		# logging.info("get layerdb mount dir: " + self._dockerCon._layerdb_mnt_dir)
		# logging.info("get layer stack: ")
		# for layer_stack in self._dockerCon._layer_stack:
		# 	logging.info("\t" + layer_stack)
		# logging.info("get layer stack init: ")
		# for layer_stack in self._dockerCon._layer_stack_init:
		# 	logging.info("\t" + layer_stack)

		layerID_stack_init = self._layer_map.convert_cacheID_stack_to_layerIDs(self._dockerCon._layer_stack_init)

		return_list = self._layer_map.get_layerID_stack_list(self._dockerCon._rootfs_id,layerID_stack_init)
		# logging.info("get layerIDs_stack list to return: ")
		# for layer_stack in return_list:
		# 	logging.info("\t" + layer_stack)

		return return_list

	# used at target node: convert layerID_stack to cache_ID stack 
	# and then store it in /aufs/layers/rootfs_id[-init] file.
	def convert_and_write_layer_stacks(self, layerID_stack_got):

		#full_cid=
		rootfs_id_read = self._layer_map.read_rootfs_id_from_stack(layerID_stack_got)
		self._target_rootfs_id = rootfs_id_read
		logging.info("server: received layerIDs stack:")
		for layer_stack in layerID_stack_got:
			logging.info("\t" + layer_stack)
		logging.info("also rootfs_id:\t" + rootfs_id_read)

		#cacheID_stack_file = "target_stack_cacheIDs_" + full_cid + ".txt"
		cacheID_stack_file_sys = self._layer_map.get_layer_stack_file_from_rootfs_id(rootfs_id_read)
		logging.info("cacheID_stack_file_sys: " + cacheID_stack_file_sys)

		logging.info("convert layerID to cacheID")
		self._cacheID_stack = self._layer_map.convert_layerID_stack_to_cacheIDs(layerID_stack_got)
		logging.info("print layerIDs stack to file: " + cacheID_stack_file_sys)
		self._layer_map.write_cacheID_stack_to_file(rootfs_id_read,self._cacheID_stack, cacheID_stack_file_sys)
		self._layer_map.write_cacheID_stack_to_init_file(self._cacheID_stack, cacheID_stack_file_sys+"-init")


	# used in both target and source node
	def set_options(self, opts):
		self._nocompression = opts ["nocompression"]
		logging.info("Lele: set nocompression option in P_haul_docker.py: %s", str(self._nocompression))
		self._pre_dump_docker = opts ["pre_dump_docker"]

		self.backup_dir_target="/root/logs-myphaul-target/" + opts["bandwidth"]
		self.backup_dir_source="/root/logs-myphaul-source/" + opts["bandwidth"]

		# if self._nocompression:
		# 	logging.info("Lele: set nocompression option in P_haul_docker.py: %s", str(self._nocompression))
		# else:
		# 	logging.info("Lele: use default nocompression option in P_haul_docker.py: %s", str(self._nocompression))

		
	# only used for target node
	# on target node, full_ctid is the same as _ctid, which is passed over rpc in __init__.
	def setup_docker_layers(self, layer_stacks):
		if(self._nocompression):
			logging.info("legacy mode: no layer stacks processed")
		else:
			logging.info("on target node, set self.full_ctid the same as _ctid")
			self.full_ctid=self._ctid

			logging.info("Lele: setting up docker layers in P_haul_docker.py for %s", self.full_ctid)
			self.convert_and_write_layer_stacks(layer_stacks)
			logging.info("Lele: setting up config dirs, rootfs dirs, (necessary?)")
			self.__load_ct_config()
			self.get_ct_rootfs(self._target_rootfs_id)

	# Remove any specific FS setup
	def umount(self):
		pass

	def start(self):
		pass

	def stop(self, umount):
		pass

	# on source node: 
	# Lele: prepare dirs to sync:
	# - rootfs. aufs/mnt/rootfs_id
	# - config_dir. /containers/full_ctid
	# - image_db. 
	# - 
	def get_fs(self, fdfs=None):
		# use rsync for rootfs and configuration directories
		#return fs_haul_subtree.p_haul_fs([self._ct_rootfs, self._ct_config_dir, self._ct_image_db], self._nocompression)

		self._sync_dirs = [self._ct_rootfs, self._ct_config_dir, self._ct_image_db]

		if(self._nocompression):
			logging.info("nocompression mode")
			return fs_haul_docker.p_haul_fs(self._sync_dirs)

		logging.info("Lele: compression mode, sync diff layers")
		for diff_dir in self._ct_diff_layer_dirs:
			if os.path.isdir(diff_dir):
				self._sync_dirs.append(diff_dir)
			else:
				raise Exception("Lele: ERROR on diff_dir: " + diff_dir + " does not exists")
		
		if (not os.path.isdir(self._ct_run_meta_dir)):
			raise Exception("LELE: ERROR: no run meta dir %s.", self._ct_run_meta_dir)
		else:
			self._sync_dirs.append(self._ct_run_meta_dir)
		
		self._fs = fs_haul_docker.p_haul_fs(self._sync_dirs)
		return self._fs

	def get_fs_receiver(self, fdfs=None):
		#return fs_haul_subtree.fs_receiver()
		return None

	# used only on source node
	def get_full_ctid(self):
		dir_name_list = os.listdir(os.path.join(docker_dir, "containers"))

		full_id = ""
		for name in dir_name_list:
			name = name.rsplit("/")
			if (name[0].find(self._ctid) == 0):
				full_id = name[0]
				break

		if full_id != "":
			return full_id
		else:
			raise Exception("Can not find container fs")

	def pre_dump(self, pid, img, ccon, fs):
		
		if not self._pre_dump_docker:
			return
		
		logging.info("Pre Dump docker container %s", pid)

		# TODO: docker API does not have checkpoint right now
		# cli.checkpoint() so we have to use the command line
		# cli = docker.Client(base_url='unix://var/run/docker.sock')
		# output = cli.info()
		# call docker API
		image_path_opt = "--image-dir=" + img.image_dir()

		pre_dump_log_file = os.path.join(img.work_dir(), "docker_pre_dump.log")
		logf = open(pre_dump_log_file, "a+")
		logging.info("lele: %s checkpoint %s %s; log file: " + pre_dump_log_file,
				docker_bin, image_path_opt, self._ctid)
		ret = sp.call([docker_bin, "checkpoint", image_path_opt, self._ctid],
			stdout = logf, stderr = logf)
		# logging.info("lele: Done: %s pre checkpoint %s %s",
		# 		docker_bin, image_path_opt, self._ctid)
		if ret != 0:
			raise Exception("docker pre checkpoint failed")

		logging.info("lele: %s restore %s %s; log file: " + pre_dump_log_file,
				docker_bin, image_path_opt, self._ctid)
		ret = sp.call([docker_bin, "restore", image_path_opt, self._ctid],
			stdout = logf, stderr = logf)
		# logging.info("lele: Done: %s pre restore %s %s",
		# 		docker_bin, image_path_opt, self._ctid)
		if ret != 0:
			raise Exception("docker local restore failed")
		
		logf.close()

	def final_dump(self, pid, img, ccon, fs):
		logging.info("Dump docker container %s", pid)

		# TODO: docker API does not have checkpoint right now
		# cli.checkpoint() so we have to use the command line
		# cli = docker.Client(base_url='unix://var/run/docker.sock')
		# output = cli.info()
		# call docker API
		logf_name = os.path.join(img.work_dir(), "docker_checkpoint_final.log")
		logf = open(logf_name, "a+")
		image_path_opt = "--image-dir=" + img.image_dir()
		logging.info("lele: %s checkpoint %s %s; log file: /tmp/docker_checkpoint.log",
				docker_bin, image_path_opt, self._ctid)
		ret = sp.call([docker_bin, "checkpoint", image_path_opt, self._ctid],
			stdout = logf, stderr = logf)
		logging.info("lele: Done: %s checkpoint %s %s; log file: /tmp/docker_checkpoint.log",
				docker_bin, image_path_opt, self._ctid)
		if ret != 0:
			raise Exception("docker checkpoint failed")
		logf.close()

	# on source node, get state.json, and descriptors.json files and return
	# Meta-images for docker -- /var/run/docker
	#
	def get_meta_images(self, path):
		# Send the meta state file with criu images
		logging.info("lele: get the meta data images to send with CRIU imgs")
		logging.info("lele: meta files are: %s , %s", 
			os.path.join(self._ct_run_meta_dir, "state.json"), 
			os.path.join(path, "descriptors.json"))
		return [(os.path.join(self._ct_run_meta_dir, "state.json"), "state.json"),
		(os.path.join(path, "descriptors.json"), "descriptors.json")]

	def put_meta_images(self, dir):
		"""
		# TODO: this method is used in legacy mode
		# on target node: docker runtime data:
		# - state.json  #lele: no need now because state.json already sent along with file systems
		# - __load_ct_rootfs # lele: no need now.
		# - __load_ct_diff_layer_dir # lele: no need now. done in setup_docker_layers()
		"""

		# Create docker runtime meta dir on dst side
		with open(os.path.join(dir, "state.json")) as data_file:
			data = json.load(data_file)
		self.full_ctid = data["id"]

		self.__load_ct_config()

		if (not os.path.isdir(self._ct_run_meta_dir)):
			os.makedirs(self._ct_run_meta_dir)
		pd = sp.Popen(["cp", os.path.join(dir, "state.json"), self._ct_run_meta_dir], stdout = PIPE)
		pd.wait()

		# Lele : make sure state.json already transferred:
		# logging.info("Lele: put meta image method for %s", self.full_ctid)
		# if (not os.path.isdir(self._ct_run_meta_dir)):
		# 	raise Exception("LELE: ERROR: state.json not transferred yet.")
		# else:
		# 	logging.info("Lele : don't call me. done nothing actually.")

		self.__load_ct_rootfs()

	def kill_last_docker_daemon(self):
		# p = sp.Popen(['pgrep', '-l', docker_bin], stdout=sp.PIPE)
		# out, err = p.communicate()

		# for line in out.splitlines():
		# 	line = bytes.decode(line)
		# 	pid = int(line.split(None, 1)[0])
		# 	os.kill(pid, signal.SIGKILL)
		# 	logging.info("kill docker pid: " + str(pid))
		ret = sp.call(["killall", "docker"])
		if ret != 0:
			raise Exception("killall docker failed.")
		logging.info("\ndocker killed via 'killall docker' cmd\n")


	def migration_complete(self, img, target_host):

		target_host.migration_complete(None)
		self.source_cleanup(img)
		# pass

	def migration_fail(self, fs):
		pass

	def getContainerDownTime(self):
		"""
		# Lele: this is for future use only;
		# Now not use this since we estimate down time on source node as: 
		# 	downtime = time to restore - time to checkpoint 
		#
		# to measure down time more accurately from inside the container
		# we need to run time synchronization inside container, 
		# and then get downtime; How?
		"""
		# TODO: measure down time here.
		return self.down_time

	def rm_container(self, logf):

		ret = sp.call([docker_bin, "rm", self._ctid],
			stdout = logf, stderr = logf)
		if ret != 0:
			raise Exception("docker rm failed")
		logging.info("docker container removed: %s", self._ctid)
		
	def backup_container_images(self, img, backupdir):
		###############################
		### remove container checkpointed images
		### But keep logs
		### Move logs to dir backupdir
		###############################
		img_dir = img.image_path()
		img_work_dir = img.work_dir()

		if not os.path.isdir(img_dir):
			raise Exception("img doesn't have img path:%s", img_dir)

		# check if parent dir exists
		if not os.path.isdir(os.path.dirname(backupdir)):
			os.mkdir(os.path.dirname(backupdir))
		if not os.path.isdir(backupdir):
			os.mkdir(backupdir)
		
		logf_name = ""
		if(self._on_source):
			backupdir = os.path.join(backupdir, "img-dmps")
			logf_name = os.path.join(backupdir, "backup-src.log")
		else:
			backupdir = os.path.join(backupdir, "img-rsts")
			logf_name = os.path.join(backupdir, "backup-dst.log")
		
		if not os.path.isdir(backupdir):
			os.mkdir(backupdir)

		logf = open(logf_name, "a+")
		logging.info("start backing up image dir: logfile: %s", logf_name)
		
		ret = sp.call(["rm", "-r", img_dir],
			stdout = logf, stderr = logf)
		if ret != 0:
			raise Exception("image path remove failed")
		logging.info("restore image path removed: %s, logfile: %s", img_dir, logf_name)

		ret = sp.call(["mv", img_work_dir, backupdir],
			stdout = logf, stderr = logf)
		if ret != 0:
			raise Exception("backup %s failed", img_work_dir)
		logging.info("all logs in %s move to: %s", img_work_dir, backupdir)
		logf.close()


	def stop_container(self, logf):
		
		ret = sp.call([docker_bin, "stop", self._ctid],
			stdout = logf, stderr = logf)
		if ret != 0:
			raise Exception("docker stop failed")
		logging.info("docker container stopped %s", self._ctid)

	def before_cleanup_target(self,logf):
		"""
		## Lele: 
		## 	- run docker exec conID cat /foo
		"""
		sleep_time = 10
		logging.info("sleep %d seconds before clean up....\n", sleep_time)
		time.sleep(sleep_time)
		ret = sp.call([docker_bin, "exec", self._ctid ,"cat", "/foo"],
			stdout = logf, stderr = logf)
		#if ret != 0:
		#	raise Exception("docker local restore failed")
		logf.write("done running: docker exec %s cat /foo\n" % self._ctid)
		logging.info("done running: docker exec %s cat /foo", self._ctid)

	def target_cleanup(self, img):
		"""
		## Lele: 
		## 	- before_cleanup_target: run docker exec -ti conID cat /foo
		##	- stop rm container
		##	- rm mem images /var/local/p.haul-fs/.../img
		##	- backup /var/local/p.haul-fs/.../ to backup_dir
		##
		##
		"""

		logf_name = os.path.join(img.work_dir(), "docker_target_cleanup.log")
		logf = open(logf_name, "a+")
		self.before_cleanup_target(logf)
		logging.info("start cleanup target: logfile: %s", logf_name)
		self.stop_container(logf)
		self.rm_container(logf)
		logf.close() # logf is in container images dir, so close it before backup.

		self.backup_container_images(img,self.backup_dir_target)
		img.close()

	def source_cleanup(self, img):

		# daemon reload moved to service.py: rpc_reload_docker_daemon_no_block()
		logf_name = os.path.join(img.work_dir(), "docker_source_cleanup.log")
		logf = open(logf_name, "a+")
		logging.info("start cleanup source: logfile: %s", logf_name)

		self.rm_container(logf)

		logf.close() # logf is in container images dir, so close it before backup.

		self.backup_container_images(img,self.backup_dir_source)

		# img.close()

		

	def final_restore(self, img, criu):

		logging.info("final_restore in p_haul_docker.py")

		image_path_opt = ""

		if self._pre_dump_docker:
			# restore from iter dirs img/1, img/2, img/iterN
			# first create a new image dir img/(iterN+1)
			# then apply the diffs img/2 .. img/iterN to img/1
			# then restore from dir img/(iterN+1)
			# img.apply_mem_diff() # called in service.py instead by rpc call
			image_path_opt = "--image-dir=" + img.get_final_dir()
			
		else:
			# retore from one image dir img/1
			image_path_opt = "--image-dir=" + img.image_dir()


		startRS = time.time()
		
		# logf = open(restore_log, "w+")
		restore_log = os.path.join(img.work_dir(), restore_log_name)
		logf = open(restore_log, "a+")

		# daemon reload moved to service.py: rpc_reload_docker_daemon_no_block()
		# but no compression mode need to be done after all stuff has been done, so here is safer
		if self._nocompression:
			# Kill any previous docker daemon in order to reload the
			# status of the migrated container
			logging.info("legacy mode: kill docker daemon")
			self.kill_last_docker_daemon()	# start docker daemon in background
			# daemon reload moved to service.py
			# sp.Popen([docker_bin, "daemon", "-D", "-s", "aufs"],
			# 	stdout = logf, stderr = logf)
			sp.Popen([docker_bin, "daemon", "-s", "aufs"],
				stdout = logf, stderr = logf)
			# sp.Popen([docker_bin, "daemon", "-D", "-s", "aufs"])
			# daemon.wait() TODO: docker daemon not return
			time.sleep(2)
			
		# else:
		# 	logging.info("new mode: don't kill docker daemon")

	
		logging.info("lele: running final_restore: %s restore %s %s", docker_bin, image_path_opt, self._ctid)
		logging.info("lele: log file: " + restore_log)
		ret = sp.call([docker_bin, "restore", image_path_opt, self._ctid],
						stdout = logf, stderr = logf)
		if ret != 0:
			raise Exception("docker restore failed")
		endRS = time.time()
		timeRS = endRS -startRS
		logging.info("lele: time to restore on target node: %ss", timeRS)
		return timeRS

	# Lele: legacy for criu pre_dump. don't use this in new compression mode.
	def can_pre_dump(self):
		# XXX: Do not do predump for docker right now. Add page-server
		# to docker C/R API, then we can enable the pre-dump
		logging.info("\t lele: predump: Do not do predump for docker right now.\n Add page-server to docker C/R API, then we can enable the pre-dump...")
		return False

	def dump_need_page_server(self):
		return False
