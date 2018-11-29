# The P.HAUL core -- the class that drives migration
#

import logging
import images
import mstats
import xem_rpc_client
import criu_api
import criu_cr
import criu_req
import htype
import errno
import time		# lele: debug timing info for target_host rpc communications
import sys
import traceback

TIME_GAP = 5.0
TIME_GAP_MINI = 1
ITERATION_INT = 2

MIGRATION_MODE_LIVE = "live"
MIGRATION_MODE_RESTART = "restart"
MIGRATION_MODES = (MIGRATION_MODE_LIVE, MIGRATION_MODE_RESTART)

PRE_DUMP_AUTO_DETECT = None
PRE_DUMP_DISABLE = False
PRE_DUMP_ENABLE = True


def is_live_mode(mode):
	"""Check is migration running in live mode"""
	return mode == MIGRATION_MODE_LIVE


def is_restart_mode(mode):
	"""Check is migration running in restart mode"""
	return mode == MIGRATION_MODE_RESTART


class iter_consts:
	"""Constants for iterations management"""

	# Maximum number of iterations
	MAX_ITERS_COUNT = 8

	# Minimum count of dumped pages needed to continue iteration
	MIN_ITER_PAGES_COUNT = 64

	# Minimum count of transferred fs bytes needed to continue iteration
	MIN_ITER_FS_XFER_BYTES = 0x100000

	# Maximum acceptable iteration grow rate in percents
	MAX_ITER_GROW_RATE = 10


class phaul_iter_worker:
	def __init__(self, p_type, dst_id, mode, connection, opts):

		self._migration_stats = mstats.live_stats(opts)
		logging.info ("lele: mstats: phaul_iter_worker.__init__(): start to counting time...")
		self._migration_stats.handle_start()

		self.__mode = mode
		self.connection = connection
		self.target_host = xem_rpc_client.rpc_proxy(self.connection.rpc_sk)

		logging.info("Setting up local")

		startTime = time.time()

		self.htype = htype.get_src(p_type)
		if not self.htype:
			raise Exception("No htype driver found")
		
		# self.fs = self.htype.get_fs(self.connection.fdfs)
		# if not self.fs:
		# 	raise Exception("No FS driver found")

		self.img = None
		self.criu_connection = None
		if is_live_mode(self.__mode):
			self.img = images.phaul_images("dmp")
			self.criu_connection = criu_api.criu_conn(self.connection.mem_sk)

		endTime = time.time()
		timing = endTime - startTime
		logging.info("lele: Time of setting up local): %s", str(timing))

		logging.info("Setting up remote")

		startTime = time.time()

		p_dst_type = (p_type[0], dst_id if dst_id else p_type[1])

		# lele: add the third element in htype_id[2]=layer_stackIDs
		# if p_type[0] == 'docker':
		# 	logging.info("lele: source side: docker mode")
		# 	logging.info("lele: \t add third element of stack_layerIDs...")
		# 	layerIDs_stack = self.htype.layerIDs_stack
		# 	p_dst_type = (p_type[0], dst_id if dst_id else p_type[1], layerIDs_stack)
		
		# else:
		# 	raise Exception("something went wrong. not using with Docker? ")

		self.target_host.setup(p_dst_type, mode)

		endTime = time.time()
		timing = endTime - startTime
		logging.info("lele: Time of setting up remote: %s", str(timing))
		self.set_options(opts)

	def get_target_host(self):
		return self.target_host

	def set_options(self, opts):
		self.__force = opts["force"]
		self.__skip_cpu_check = opts["skip_cpu_check"]
		self.__skip_criu_check = opts["skip_criu_check"]
		self.__pre_dump = opts["pre_dump"]
		self.__pre_dump_docker = opts["pre_dump_docker"]
		self.__docker_iters = opts["docker_iters"]
		self.__iter_max = int(opts["iter_max"])
		self.__iter_threshold = self.__get_threshold(opts["iter_threshold"])
		self.nocompression=opts["nocompression"]
		self.htype.set_options(opts)

		self.fs = self.htype.get_fs(self.connection.fdfs)
		if not self.fs:
			raise Exception("No FS driver found")

		self.fs.set_options(opts)
		if self.img:
			self.img.set_options(opts)
		if self.criu_connection:
			self.criu_connection.set_options(opts)
		startRPC = time.time()
		self.target_host.set_options(opts)

		# in new compression mode, we send layerIDs_stack information to target machine.
		if not self.nocompression:
			self.target_host.setup_docker_layers(self.htype.layerIDs_stack)
			
		endRPC = time.time()
		timeRPC = endRPC - startRPC
		logging.info("lele:RPC Timing(set_options): %s", str(timeRPC))

		self._migration_stats.done_init()
	
	def __get_threshold(self,iter_threshold_option):
		""" given threshold by #g, or #G, or #m, #M, or #k, #K, #b, #B. 
			or # as bytes by default.
		"""
		
		if iter_threshold_option.endswith('g') or iter_threshold_option.endswith('G'):
			threshold_ret = 1024 * 1024 * 1024 * int(iter_threshold_option[:-1])
		elif iter_threshold_option.endswith('m') or iter_threshold_option.endswith('M'):
			threshold_ret = 1024 * 1024 * int(iter_threshold_option[:-1])
		elif iter_threshold_option.endswith('k') or iter_threshold_option.endswith('K'):
			threshold_ret = 1024 * int(iter_threshold_option[:-1])
		elif iter_threshold_option.endswith('b') or iter_threshold_option.endswith('B'):
			threshold_ret = int(iter_threshold_option[:-1])
		else:
			threshold_ret = int(iter_threshold_option)
		return threshold_ret
	
	def __validate_cpu(self):
		if self.__skip_cpu_check or self.__force:
			return
		logging.info("Checking CPU compatibility")

		logging.info("\t`- Dumping CPU info")
		req = criu_req.make_cpuinfo_dump_req(self.img)
		resp = self.criu_connection.send_req(req)
		if resp.HasField('cr_errno') and (resp.cr_errno == errno.ENOTSUP):
			logging.info("\t`- Dumping CPU info not supported")
			self.__force = True
			return
		if not resp.success:
			raise Exception("Can't dump cpuinfo")

		logging.info("\t`- Sending CPU info")
		self.img.send_cpuinfo(self.target_host, self.connection.mem_sk)

		logging.info("\t`- Checking CPU info")
		startRPC = time.time()
		if not self.target_host.check_cpuinfo():
			raise Exception("CPUs mismatch")
		endRPC = time.time()
		timeRPC = endRPC - startRPC
		logging.info("lele:RPC Timing (check_cpuinfo): %s", str(timeRPC))

	def __validate_criu_version(self):
		if self.__skip_criu_check or self.__force:
			return
		logging.info("Checking criu version")
		version = criu_api.get_criu_version()
		if not version:
			raise Exception("Can't get criu version")
		startRPC = time.time()
		if not self.target_host.check_criu_version(version):
			raise Exception("Incompatible criu versions")
		endRPC = time.time()
		timeRPC = endRPC - startRPC
		logging.info("lele:RPC Timing (check_criu_version): %s", str(timeRPC))

	def __check_support_mem_track(self):
		req = criu_req.make_dirty_tracking_req(self.img)
		resp = self.criu_connection.send_req(req)
		if not resp.success:
			raise Exception()
		if not resp.HasField('features'):
			return False
		if not resp.features.HasField('mem_track'):
			return False
		logging.info("\t lele: MemTrack - %s", 
				(resp.features.mem_track and "enabled" or "disabled"))
		return resp.features.mem_track

	def __check_use_pre_dumps(self):
		logging.info("Checking for Dirty Tracking")
		use_pre_dumps = False
		if self.__pre_dump == PRE_DUMP_AUTO_DETECT:
			try:
				# Detect is memory tracking supported
				use_pre_dumps = (self.__check_support_mem_track() and
					self.htype.can_pre_dump())
				logging.info("\t`- Auto %s",
					(use_pre_dumps and "enabled" or "disabled"))
			except:
				# Memory tracking auto detection not supported
				use_pre_dumps = False
				logging.info("\t`- Auto detection not possible - Disabled")
		else:
			use_pre_dumps = self.__pre_dump
			logging.info("\t`- Explicitly %s",
				(use_pre_dumps and "enabled" or "disabled"))
		self.criu_connection.memory_tracking(use_pre_dumps)
		return use_pre_dumps

	def start_migration(self):
		logging.info("Start migration in %s mode", self.__mode)
		if is_live_mode(self.__mode):
			if self.__docker_iters and self.__pre_dump_docker:
				self.__start_live_migration_docker_iters()
			elif self.__pre_dump_docker:
				self.__start_live_migration_pre_dump_docker()
			else:
				self.__start_live_migration()
		elif is_restart_mode(self.__mode):
			self.__start_restart_migration()
		else:
			raise Exception("Unknown migration mode")

	def __start_live_migration_pre_dump_docker(self):
		"""
		Start migration in live mode, use pre_dump memory for docker containers

		Migrate memory and fs to target host iteratively while possible,
		checkpoint process tree on source host and restore it on target host.
		"""
		try:
			
			self._migration_stats.start_check()

			self.fs.set_work_dir(self.img.work_dir())
			self.__validate_cpu()
			self.__validate_criu_version()

			root_pid = self.htype.root_task_pid()

			logging.info ("lele: root taskt pid: %s", root_pid)

			self._migration_stats.done_check()

			# Handle predump of docker containers, and send dump images to target before we have migration
			# requests
			logging.info("Pre-dump container") 
			self._migration_stats.start_dump0()

			# prepare directories on both target and source

			# Lele:  this will call img.new_img_dir() on target host.
			self.target_host.start_iter(False)
			self.img.new_image_dir()

			# checkpointing and restore docker containers immediately
			self.htype.pre_dump(root_pid, self.img, self.criu_connection, self.fs)
			self._migration_stats.done_dump0()

			# send the image while container is still running locally
			logging.info("Sending pre-dump-docker images")
			self._migration_stats.start_img_send0()
			self.img.sync_imgs_to_target(self.target_host, self.htype,
				self.connection.mem_sk)
			self._migration_stats.done_img_send0()

			# may also sending premilinary FS here.........


			################################################################
			################################################################
			################################################################
			################################################################
			# the container continues running until a migration request is received.
			# logging.info ("we may need wait for a moment before we trigger migration....")
			logging.info ("will wait for at least " + str(TIME_GAP) + " seconds since dump time")
			interval = float(TIME_GAP) - (time.time() - self._migration_stats.done_dump_time0)
			sleep_time = int(interval) + 1
			if sleep_time < TIME_GAP_MINI:
				sleep_time = TIME_GAP_MINI
			logging.info ("Now wait " + str(sleep_time) + " seconds....")
			start_sleep = time.time()
			time.sleep(sleep_time)
			#time.sleep(1)
			self._migration_stats.sleep_time =  time.time() - start_sleep
			################################################################
			################################################################
			################################################################
			################################################################

			# checkpoint and stop the container and send:
			#	- fs(R/W container layer)
			#	- memory difference (use xdelta3 to get diff and send via tar+ssh pipes)
			logging.info("Final dump and sending FS and Mem Differences")	
			self._migration_stats.start_dump()
			# final dump: prepare directories on both target and source
			# Lele:  this will call img.new_img_dir() on target host.
			self.target_host.start_iter(False)
			self.img.new_image_dir()
			# final dump: checkpoint and stop the container
			self.htype.final_dump(root_pid, self.img, self.criu_connection, self.fs)
			self._migration_stats.done_dump()

			################################
			# TODO: fs and image diff sync might be done together? 
			#	- No: daemon reload need files only available after FS sync.
			#	- Yes: split fs sent to two parts: daemon related vs. no daemon related. 
			#		Send daemon needed first and all following could be sent together with images.
			# TODO : But how to find out which file is necessary for daemon reload?
			#	possible options: 
			#		- state.json and rootfs indexing info. 
			#		- diff layers might not be necessary for deamon reload.
			# 
			#################################
			logging.info("Sending final FS...")
			# lele: sync file systems after image sync, hope to avoid fs contention
			# Lele: this will sync file system for last time.
			self._migration_stats.start_fs_sync_final()
			fsstats = self.fs.stop_migration()
			self.target_host.reload_docker_daemon_no_block()
			self._migration_stats.done_fs_sync_final()
				


			logging.info("Sending final mem diff...")
			self._migration_stats.start_img_send()
			self.img.sync_imgs_diff_to_target(self.target_host, self.htype,
				self.connection.mem_sk)
			self._migration_stats.done_img_send()
			#fsstats = self.fs.stop_migration()
			#fsstats = self.fs.stop_migration2(self.target_host, self.htype,
			#	self.connection.mem_sk)
			logging.info("Asking target host to apply mem diff")
			self._migration_stats.start_apply_diff()
			self.target_host.apply_diff_images()
			self._migration_stats.done_apply_diff()
			# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
			# logging.info()


			# endSync = time.time()
			# timeSync = endSync - startSync
			# logging.info("lele:image sync Timing (img.sync_imgs_to_target): %s", str(timeSync))

			# Restore htype on target
			logging.info("Asking target host to restore")
			self._migration_stats.start_restore()
			self.target_host.restore_from_images()
			self._migration_stats.done_restore()
			# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
			logging.info("Restored on target host")

		except:
			self.htype.migration_fail(self.fs)
			raise

		# Ack previous dump request to terminate all frozen tasks
		# resp = self.criu_connection.ack_notify()
		# if not resp.success:
		# 	logging.warning("Bad notification from target host")

		# dstats = criu_api.criu_get_dstats(self.img)
		# self._migration_stats.handle_iteration(dstats, fsstats)

		logging.info("Migration succeeded")
		self._migration_stats.handle_stop(self)
		self.htype.migration_complete(self.img, self.target_host)
		self.img.close()
		self.criu_connection.close()


	def __start_live_migration_docker_iters(self):
		"""
		Start migration in live mode, use multiple iters of pre_dump memory for docker containers

		Important parameters:

		-- iter_max: control the total time of iterations.
		-- iter_threshold: control the minimal size of memory to transfer.

		Migrate memory and fs to target host iteratively while possible,
		checkpoint process tree on source host and restore it on target host.
		"""
		try:
			
			self._migration_stats.start_check()

			self.fs.set_work_dir(self.img.work_dir())
			self.__validate_cpu()
			self.__validate_criu_version()

			root_pid = self.htype.root_task_pid()

			logging.info ("lele: root taskt pid: %s", root_pid)

			self._migration_stats.done_check()

			# Handle base memory image of Docker container, and send base memory images to target before we have migration
			# requests
			logging.info("Pre-dump container") 
			self._migration_stats.start_iter(1)
			self._migration_stats.start_dump0()

			# prepare directories on both target and source

			# Lele:  this will call img.new_img_dir() on target host.
			self.target_host.start_iter(False)
			self.img.new_image_dir()

			# checkpointing and restore docker containers immediately
			self.htype.pre_dump(root_pid, self.img, self.criu_connection, self.fs)
			self._migration_stats.done_dump0()

			# send the image while container is still running locally
			logging.info("Sending pre-dump-docker images")
			self._migration_stats.start_img_send0()
			self.img.sync_imgs_to_target(self.target_host, self.htype,
				self.connection.mem_sk)
			self._migration_stats.done_img_send0()

			self._migration_stats.done_iter(1)
			# may also sending premilinary FS here.........

			# send additional memory images iteratively
			# according to the parameter of 'iter_max', or 'iter_threshold'
			iterNum = 2
			while (iterNum < self.__iter_max):
				# get memory diff and its size
				logging.info("Iterative dump and sending Mem Differences")	
				self._migration_stats.start_iter(iterNum)
				self.img.new_image_dir()
				# checkpointing and restore docker containers immediately
				self.htype.pre_dump(root_pid, self.img, self.criu_connection, self.fs)
				# self._migration_stats.done_dump()
				

				# # send the image while container is still running locally
				# logging.info("get iteration " + str(iterNum) + "'s memory diff....")
				# ret = self.img.get_mem_diff()
				
				# mem_diff_size = float(self.img.get_mem_diff_size())/1024.0
				# logging.info ("\nlele: get diff size: %.2f KBytes", mem_diff_size)

				# #exit()
				# # only do iteration when size meet the threshold
				# if (mem_diff_size < self.__iter_threshold):
				# 	# don't send the memory diff
				# 	# discard the dump
				# 	self.img.pop_image_dir()
					
				# 	# sleep for a while and continue next dump
				# 	logging.info("mem diff size is smaller than %s KBytes, directory removed and now wait for %d seconds...\n\n", 
				# 		self.__iter_threshold, ITERATION_INT)
				# 	time.sleep(ITERATION_INT)
				# 	self._migration_stats.add_sleep_time(ITERATION_INT)
				# 	#sleep 2
				# 	# continue
				# else:

				# Lele:  this will call img.new_img_dir() on target host.
				# self.target_host.start_iter(False)

				# sending the memory diff to target
				logging.info("Sending iterative mem diff...")
				# self._migration_stats.start_img_send()
				ret = self.img.sync_imgs_diff_to_target(self.target_host, self.htype,
					self.connection.mem_sk)
				# self._migration_stats.done_img_send()
				#fsstats = self.fs.stop_migration()
				#fsstats = self.fs.stop_migration2(self.target_host, self.htype,
				#	self.connection.mem_sk)
				logging.info("Asking target host to apply mem diff")
				# self._migration_stats.start_apply_diff()
				try:
					self.target_host.apply_diff_images()
				except:
					# traceback.print_exec()
					print "Remote exception when calling apply_diff_images on target. now exit with code 3"
					sys.exit(3)
				# self._migration_stats.done_apply_diff()
				# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
				# logging.info()
				self._migration_stats.done_iter(iterNum)
				iterNum += 1
			################################################################
			################################################################
			################################################################
			################################################################
			# the container continues running until a migration request is received.
			# logging.info ("we may need wait for a moment before we trigger migration....")
			logging.info ("will wait for at least " + str(TIME_GAP) + " seconds since dump time")
			interval = float(TIME_GAP) - (time.time() - self._migration_stats.done_dump_time0)
			sleep_time = int(interval) + 1
			if sleep_time < TIME_GAP_MINI:
				sleep_time = TIME_GAP_MINI
			logging.info ("Now wait " + str(sleep_time) + " seconds....")
			start_sleep = time.time()
			time.sleep(sleep_time)
			#time.sleep(1)
			self._migration_stats.add_sleep_time( time.time() - start_sleep )
			################################################################
			################################################################
			################################################################
			################################################################

			# checkpoint and stop the container and send:
			#	- fs(R/W container layer)
			#	- memory difference (use xdelta3 to get diff and send via tar+ssh pipes)
			logging.info("Final dump and sending FS and Mem Differences")	
			self._migration_stats.start_iter(iterNum)
			self._migration_stats.start_dump()
			# final dump: prepare directories on both target and source
			# # Lele:  this will call img.new_img_dir() on target host.
			# self.target_host.start_iter(False)
			self.img.new_image_dir()
			# final dump: checkpoint and stop the container
			self.htype.final_dump(root_pid, self.img, self.criu_connection, self.fs)
			self._migration_stats.done_dump()

			################################
			# TODO: fs and image diff sync might be done together? 
			#	- No: daemon reload need files only available after FS sync.
			#	- Yes: split fs sent to two parts: daemon related vs. no daemon related. 
			#		Send daemon needed first and all following could be sent together with images.
			# TODO : But how to find out which file is necessary for daemon reload?
			#	possible options: 
			#		- state.json and rootfs indexing info. 
			#		- diff layers might not be necessary for deamon reload.
			# 
			#################################
			logging.info("Sending final FS...")
			# lele: sync file systems after image sync, hope to avoid fs contention
			# Lele: this will sync file system for last time.
			self._migration_stats.start_fs_sync_final()
			fsstats = self.fs.stop_migration()
			self.target_host.reload_docker_daemon_no_block()
			self._migration_stats.done_fs_sync_final()
				


			logging.info("Sending final mem diff...")
			self._migration_stats.start_img_send()
			self.img.sync_imgs_diff_to_target(self.target_host, self.htype,
				self.connection.mem_sk)
			self._migration_stats.done_img_send()
			#fsstats = self.fs.stop_migration()
			#fsstats = self.fs.stop_migration2(self.target_host, self.htype,
			#	self.connection.mem_sk)
			logging.info("Asking target host to apply mem diff")
			self._migration_stats.start_apply_diff()
			self.target_host.apply_diff_images()
			self._migration_stats.done_apply_diff()
			self._migration_stats.done_iter(iterNum)
			# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
			# logging.info()


			# endSync = time.time()
			# timeSync = endSync - startSync
			# logging.info("lele:image sync Timing (img.sync_imgs_to_target): %s", str(timeSync))

			# Restore htype on target
			logging.info("Asking target host to restore")
			self._migration_stats.start_restore()
			self.target_host.restore_from_images()
			self._migration_stats.done_restore()
			# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
			logging.info("Restored on target host")

		except:
			self.htype.migration_fail(self.fs)
			raise

		# Ack previous dump request to terminate all frozen tasks
		# resp = self.criu_connection.ack_notify()
		# if not resp.success:
		# 	logging.warning("Bad notification from target host")

		# dstats = criu_api.criu_get_dstats(self.img)
		# self._migration_stats.handle_iteration(dstats, fsstats)

		logging.info("Migration succeeded")
		self._migration_stats.handle_stop(self)
		self.htype.migration_complete(self.img, self.target_host)
		self.img.close()
		self.criu_connection.close()


	def __start_live_migration(self):
		"""
		Start migration in live mode

		Migrate memory and fs to target host iteratively while possible,
		checkpoint process tree on source host and restore it on target host.
		"""

		# self._migration_stats = mstats.live_stats()
		# logging.info ("lele: mstats: start to counting time...")
		# self._migration_stats.handle_start()
		
		self._migration_stats.start_check()

		self.fs.set_work_dir(self.img.work_dir())
		self.__validate_cpu()
		self.__validate_criu_version()

		# use_pre_dumps = self.__check_use_pre_dumps()
		# use_pre_dumps = False

		root_pid = self.htype.root_task_pid()

		logging.info ("lele: root taskt pid: %s", root_pid)

		self._migration_stats.done_check()

		# Handle preliminary FS migration
		logging.info("Preliminary FS migration") 
		self._migration_stats.start_fs_sync1()
		fsstats = self.fs.start_migration()
		#fsstats = self.fs.start_migration2(self.target_host, self.htype,
		#		self.connection.mem_sk)
		self._migration_stats.done_fs_sync1()

		# iter_index = 0
		# prev_dstats = None

		# while use_pre_dumps:
		# 	#lele: this is not called for Dockers.
		# 	# Handle predump
		# 	logging.info("* Iteration %d", iter_index)
		# 	startRPC = time.time()
		# 	self.target_host.start_iter(True)
		# 	endRPC = time.time()
		# 	timeRPC = endRPC - startRPC
		# 	logging.info("lele:RPC Timing(start_iter): %s", str(timeRPC))
		# 	self.img.new_image_dir()
		# 	criu_cr.criu_predump(root_pid, self.img, self.criu_connection, self.fs)
		# 	startRPC = time.time()
		# 	self.target_host.end_iter()
		# 	endRPC = time.time()
		# 	timeRPC = endRPC - startRPC
		# 	logging.info("lele:RPC Timing(end_iter): %s", str(timeRPC))

		# 	# Handle FS migration iteration
		# 	fsstats = self.fs.next_iteration()

		# 	dstats = criu_api.criu_get_dstats(self.img)
		# 	self._migration_stats.handle_iteration(dstats, fsstats)

		# 	# Decide whether we continue iteration or stop and do final dump
		# 	if not self.__check_live_iter_progress(iter_index, dstats, prev_dstats):
		# 		break

		# 	iter_index += 1
		# 	prev_dstats = dstats

		# Dump htype on source and leave its tasks in frozen state
		logging.info("Final dump and restore")
		# startRPC = time.time()

		# Lele:  this will call img.new_img_dir() on target host.
		self.target_host.start_iter(self.htype.dump_need_page_server())
		# endRPC = time.time()
		# timeRPC = endRPC - startRPC
		# logging.info("lele:RPC Timing(start_iter, final): %s", str(timeRPC))
		self.img.new_image_dir()

		self._migration_stats.start_dump()
		# startDump = time.time()
		self.htype.final_dump(root_pid, self.img, self.criu_connection, self.fs)
		# endDump = time.time()
		# timeDump = endDump - startDump
		# logging.info("lele:Dump Timing (final): %s", str(timeDump))
		self._migration_stats.done_dump()

		# startRPC = time.time()
		# end_iter done nothing, so don't call it
		# self.target_host.end_iter()
		# endRPC = time.time()
		# timeRPC = endRPC - startRPC
		# logging.info("lele:RPC Timing(end_iter, final): %s", str(timeRPC))

		try:
			# Handle final FS and images sync on frozen htype
			logging.info("send final FS, reload target daemon and send images.")
			
			# lele: sync file systems after image sync, hope to avoid fs contention
			# Lele: this will sync file system for last time.
			self._migration_stats.start_fs_sync_final()
			fsstats = self.fs.stop_migration()
			if not self.nocompression:
				# only don't run this in nocompression mode
				self.target_host.reload_docker_daemon_no_block()
			self._migration_stats.done_fs_sync_final()
			
			self._migration_stats.start_img_send()
			self.img.sync_imgs_to_target(self.target_host, self.htype,
				self.connection.mem_sk)
			self._migration_stats.done_img_send()
			#fsstats = self.fs.stop_migration()
			#fsstats = self.fs.stop_migration2(self.target_host, self.htype,
			#	self.connection.mem_sk)


			# endSync = time.time()
			# timeSync = endSync - startSync
			# logging.info("lele:image sync Timing (img.sync_imgs_to_target): %s", str(timeSync))

			# Restore htype on target
			logging.info("Asking target host to restore")
			self._migration_stats.start_restore()
			self.target_host.restore_from_images()
			self._migration_stats.done_restore()
			# logging.info("lele:RPC Timing(restore_from_images): %s", str(timeRPC))
			logging.info("Restored on target host")
		except:
			self.htype.migration_fail(self.fs)
			raise

		# Ack previous dump request to terminate all frozen tasks
		# resp = self.criu_connection.ack_notify()
		# if not resp.success:
		# 	logging.warning("Bad notification from target host")

		# dstats = criu_api.criu_get_dstats(self.img)
		# self._migration_stats.handle_iteration(dstats, fsstats)

		logging.info("Migration succeeded")
		self._migration_stats.handle_stop(self)
		self.htype.migration_complete(self.img, self.target_host)
		self.img.close()
		self.criu_connection.close()

	def __start_restart_migration(self):
		"""
		Start migration in restart mode

		Migrate fs to target host iteratively while possible, stop process
		tree on source host and start it on target host.
		"""

		self._migration_stats = mstats.restart_stats()
		self._migration_stats.handle_start()

		# Handle preliminary FS migration
		logging.info("Preliminary FS migration")
		fsstats = self.fs.start_migration()
		self._migration_stats.handle_preliminary(fsstats)

		iter_index = 0
		prev_fsstats = None

		while True:

			# Handle FS migration iteration
			logging.info("* Iteration %d", iter_index)
			fsstats = self.fs.next_iteration()
			self._migration_stats.handle_iteration(fsstats)

			# Decide whether we continue iteration or stop and do final sync
			if not self.__check_restart_iter_progress(iter_index, fsstats, prev_fsstats):
				break

			iter_index += 1
			prev_fsstats = fsstats

		# Stop htype on source and leave it mounted
		logging.info("Final stop and start")
		self.htype.stop(False)

		try:
			# Handle final FS sync on mounted htype
			logging.info("Final FS sync")
			fsstats = self.fs.stop_migration()
			self._migration_stats.handle_iteration(fsstats)

			# Start htype on target
			logging.info("Asking target host to start")
			startRPC = time.time()
			self.target_host.start_htype()
			endRPC = time.time()
			timeRPC = endRPC - startRPC
			logging.info("lele:RPC Timing(start_htype): %s", str(timeRPC))
			logging.info("Started on target host")

		except:
			self.htype.migration_fail(self.fs)
			self.htype.start()
			raise

		logging.info("Migration succeeded")
		self.htype.migration_complete(self.fs, self.target_host)
		self._migration_stats.handle_stop()

	def __check_live_iter_progress(self, index, dstats, prev_dstats):

		logging.info("Checking iteration progress:")

		if dstats.pages_written <= iter_consts.MIN_ITER_PAGES_COUNT:
			logging.info("\t> Small dump")
			return False

		if prev_dstats:
			grow_rate = self.__calc_grow_rate(dstats.pages_written,
				prev_dstats.pages_written)
			if grow_rate > iter_consts.MAX_ITER_GROW_RATE:
				logging.info("\t> Iteration grows")
				return False

		if index >= iter_consts.MAX_ITERS_COUNT:
			logging.info("\t> Too many iterations")
			return False

		logging.info("\t> Proceed to next iteration")
		return True

	def __check_live_iter_progress2(self, index, dstats, prev_dstats):

		logging.info("Checking iteration progress:")

		if index >= iter_consts.MAX_ITERS_COUNT:
			logging.info("\t> Too many iterations")
			return False

		if dstats.pages_written <= iter_consts.MIN_ITER_PAGES_COUNT:
			logging.info("\t> Small dump")
			return False

		if prev_dstats:
			grow_rate = self.__calc_grow_rate(dstats.pages_written,
				prev_dstats.pages_written)
			if grow_rate > iter_consts.MAX_ITER_GROW_RATE:
				logging.info("\t> Iteration grows")
				return False

		logging.info("\t> Proceed to next iteration")
		return True

	def __check_restart_iter_progress(self, index, fsstats, prev_fsstats):

		logging.info("Checking iteration progress:")

		if fsstats.bytes_xferred <= iter_consts.MIN_ITER_FS_XFER_BYTES:
			logging.info("\t> Small fs transfer")
			return False

		if prev_fsstats:
			grow_rate = self.__calc_grow_rate(fsstats.bytes_xferred,
				prev_fsstats.bytes_xferred)
			if grow_rate > iter_consts.MAX_ITER_GROW_RATE:
				logging.info("\t> Iteration grows")
				return False

		if index >= iter_consts.MAX_ITERS_COUNT:
			logging.info("\t> Too many iterations")
			return False

		logging.info("\t> Proceed to next iteration")
		return True

	def __calc_grow_rate(self, value, prev_value):
		delta = value - prev_value
		return delta * 100 / prev_value
