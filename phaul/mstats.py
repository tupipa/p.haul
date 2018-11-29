import time
import logging
import os


class fs_iter_stats:
	def __init__(self, bytes_xferred):
		self.bytes_xferred = bytes_xferred


class live_stats:
	def __init__(self, opts):
		self.options = opts
		self.__start_time = 0.0
		self.__end_time = 0.0
		self.__restore_time = 0
		self.__img_sync_time = 0.0
		self.total_iter = 0
		self.iter_max = int(opts["iter_max"])
		self.compression_level = int (opts["compression_level"])
		self.iter_threshold = opts["iter_threshold"]
		self.__iter_frozen_times = [0.0 for i in range(self.iter_max)]
		self.__iter_start_times = [0.0 for i in range(self.iter_max)]
		self.__iter_done_times = [0.0 for i in range(self.iter_max)]
		self.iter_times = [0.0 for i in range(self.iter_max)]
		self.iter_sizes = [0.0 for i in range(self.iter_max)]

		self.init_time = 0.0
		self.done_init_time = 0.0

		self.env_check_time = 0.0
		self.start_check_time =0.0
		self.done_check_time = 0.0

		# Lele: only used in no-compression mode
		self.fs_first_time = None
		self.start_fs_first_time = None
		self.done_fs_first_time = None

		# Lele: only used in pre-dump-docker mode
		self.dump_time0 = None
		self.start_dump_time0 = None
		self.done_dump_time0 = None
		self.img_send_time0 = None
		self.done_img_send_time0 = None
		self.start_img_send_time0 = None
		self.img_diff_time = None

		self.sleep_time = None

		self.diff_apply_time = None
		self.done_apply_diff_time = None
		self.start_apply_diff_time = None


		#Lele: size info
		# on all modes
		self.transferred_cpuinfo_size = None
		# on --nocompression mode and --imgthread mode
		self.transferred_img_size = None
		# on --pre-dump-docker --imgthread mode.
		self.transferred_memDiff_size = None

		self.dump_time = 0.0
		self.start_dump_time = 0.0
		self.done_dump_time = 0.0

		self.fs_final_time = 0.0
		self.done_fs_final_time = 0.0
		self.start_fs_final_time = 0.0
		
		# img_final_time = self.img_diff_time + self.img_send_time
		# img_final_time is counted in iters.py
		# img_diff_time and img_send_time is counted in images.py, 
		#		read here using iters.img.diff_time and iters.img.sync_time
		self.img_final_time = 0.0
		self.img_send_time = 0.0
		self.done_img_send_time = 0.0
		self.start_img_send_time = 0.0

		self.target_restore_time = 0.0
		self.done_restore_time = 0.0
		self.start_restore_time = 0.0


	def handle_start(self):
		self.__start_time = time.time()

	def done_init(self):
		self.done_init_time = time.time()

	def start_check(self):
		self.start_check_time = time.time()

	def done_check(self):
		self.done_check_time = time.time()
		# self.env_check_time = self.done_check_time - self.start_check_time

	def handle_preliminary(self, fsstats):
		_print_fsstats(fsstats)

	def start_fs_sync1(self):
		self.start_fs_first_time = time.time()

	def done_fs_sync1(self):
		self.done_fs_first_time = time.time()
		# self.fs_first_time = self.done_fs_first_time - self.start_fs_first_time

	def start_dump0(self):
		self.start_dump_time0 = time.time() 

	def done_dump0(self):
		self.done_dump_time0 = time.time()
		# self.dump_time = self.done_dump_time - self.start_dump_time
	
	def start_iter(self, i):
		self.__iter_start_times[i-1] = time.time() 

	def done_iter(self, i):
		self.total_iter = i
		self.__iter_done_times[i-1] = time.time() 
		# self.dump_time = self.done_dump_time - self.start_dump_time
	
	def start_img_send0(self):
		self.start_img_send_time0 = time.time() 

	def done_img_send0(self):
		self.done_img_send_time0 = time.time()
		# self.img_send_time = self.done_img_send_time - self.start_img_send_time

	def start_dump(self):
		self.start_dump_time = time.time() 

	def done_dump(self):
		self.done_dump_time = time.time()
		# self.dump_time = self.done_dump_time - self.start_dump_time
	def start_apply_diff(self):
		self.start_apply_diff_time =time.time()

	def done_apply_diff(self):
		self.done_apply_diff_time =time.time()
		
	def start_fs_sync_final(self):
		self.start_fs_final_time = time.time() 

	def done_fs_sync_final(self):
		self.done_fs_final_time = time.time()
		# self.fs_final_time = self.done_fs_final_time - self.start_fs_final_time

	def start_img_send(self):
		self.start_img_send_time = time.time() 

	def done_img_send(self):
		self.done_img_send_time = time.time()
		# self.img_send_time = self.done_img_send_time - self.start_img_send_time

	def start_restore(self):
		self.start_restore_time = time.time() 

	def done_restore(self):
		self.done_restore_time = time.time()
		# self.target_restore_time = self.done_restore_time - self.start_restore_time

	def handle_iteration(self, dstats, fsstats):
		self.__iter_frozen_times.append(dstats.frozen_time)
		_print_dstats(dstats)
		_print_fsstats(fsstats)
		
	def add_sleep_time(self, slp_time):
		if (not self.sleep_time):
			self.sleep_time = 0
		
		self.sleep_time += slp_time
		
	def set_iter_size(self, itr, size_):
		self.iter_sizes[iter] = size_

	def add_iter_size(self, itr, size_):
		self.iter_sizes[iter] += size_

	def set_sleep_time(self, slp_time):
		self.sleep_time += slp_time

	def handle_stop(self, iters):
		self.__end_time = time.time()

		# docker daemon reload timing in sevice.py
		self.reload_time = iters.get_target_host().reload_time()

		# docker container final restore timing in sevice.py
		self.__restore_time = iters.get_target_host().restore_time()

		self.__img_sync_time = iters.img.img_sync_time()

		self.init_time = self.done_init_time - self.__start_time
		self.env_check_time = self.done_check_time - self.start_check_time

		# for --no-compression option
		if self.start_fs_first_time:
			self.fs_first_time = self.done_fs_first_time - self.start_fs_first_time
	
		# for --pre-dump-docker options
		if self.start_dump_time0:
			self.dump_time0 = self.done_dump_time0 - self.start_dump_time0
			self.img_send_time0 = self.done_img_send_time0 - self.start_img_send_time0
			if iters.img.diff_time:
				self.img_diff_time = iters.img.diff_time

		if self.start_apply_diff_time:
			# time counted by source host
			self.diff_apply_time = self.done_apply_diff_time - self.start_apply_diff_time
		else:
			# time counted by target host
			self.diff_apply_time = iters.get_target_host().diff_apply_time()
		
		if self.total_iter > 0:
			for i in range(self.total_iter):
				self.iter_times[i] = self.__iter_done_times[i] - self.__iter_start_times[i]

	
		self.dump_time = self.done_dump_time - self.start_dump_time
		self.fs_final_time = self.done_fs_final_time - self.start_fs_final_time
		self.img_final_time = self.done_img_send_time - self.start_img_send_time
		self.img_send_time = iters.img.img_sync_time()

		self.target_restore_time = self.done_restore_time - self.start_restore_time

		# size info
		self.transferred_cpuinfo_size = iters.img.transferred_cpuinfo_size
		# on --nocompression mode and --imgthread mode
		self.transferred_img_size = iters.img.transferred_img_size
		# on --pre-dump-docker --imgthread mode.
		self.transferred_memdiff_size = iters.img.transferred_memdiff_size
		# end size info

		self.timing_log_file=self.get_log_file_name(iters)
		
		self.print_to_file(self.timing_log_file, iters)

		self.__print_overall(iters)

	def get_log_file_name(self,iters):

		###################
		# set up log dir, we'll write stat logfile here
		back_dir = iters.htype.backup_dir_source	#this assumed to be /root/logs-myphaul-source/bandwithN

		if(not os.path.isdir(os.path.dirname(back_dir))):
			os.mkdir(os.path.dirname(back_dir))
			os.mkdir(back_dir)
		if(not os.path.isdir(back_dir)):
			os.mkdir(back_dir)

		log_dir = os.path.join(back_dir,"stats")

		if(not os.path.isdir(log_dir)):
			os.mkdir(log_dir)
		# log_dir = os.path.join(iters.img.work_dir(),"timing_log")
		# self.img_work_dir = iters.img.work_dir()

		dir_nocom = "nocompression"
		dir_pre_tar = "predump-docker-tarssh"
		dir_pre_pipe = "predump-docker-pipe"
		dir_pre_thread = "predump-docker-imgthread"
		dir_thread = "imgthread"
		dir_tar = "tarssh"

		# Done setup log dir.

		file_name = ""
		sub_dir = ""
		if (self.options["nocompression"]):
			logging.info("MODE: nocompression")
			sub_dir = os.path.join(log_dir, dir_nocom)
		elif(self.options["pre_dump_docker"] and self.options["imgthread"]):
			# pre dump with imgthread
			logging.info("MODE: pre dump with imgthread")
			tmp_dir = os.path.join(log_dir, dir_pre_thread)
			if (not os.path.isdir(tmp_dir)):
				os.mkdir(tmp_dir)

			tmp_dir2 = os.path.join(tmp_dir, "comlevel_" + str(self.compression_level))
			if (not os.path.isdir(tmp_dir2)):
				os.mkdir(tmp_dir2)

			iter_sub = "iter_max_" + str(self.iter_max)
			tmp_dir3 = os.path.join(tmp_dir2, iter_sub)
			if (not os.path.isdir(tmp_dir3)):
				os.mkdir(tmp_dir3)

			threshold_sub = "threshold_" + self.iter_threshold
			tmp_dir4 = os.path.join(tmp_dir3, threshold_sub)

			sub_dir = tmp_dir4

		elif(self.options["pre_dump_docker"] and self.options["disable_pipe"]):
			# pre dump tarssh with pipe disabled
			logging.info("MODE: pre dump with tarssh, pipe disabled")
			tmp_dir = os.path.join(log_dir, dir_pre_tar)
			if (not os.path.isdir(tmp_dir)):
				os.mkdir(tmp_dir)

			tmp_dir2 = os.path.join(tmp_dir, "comlevel_" + str(self.compression_level))
			if (not os.path.isdir(tmp_dir2)):
				os.mkdir(tmp_dir2)
				
			iter_sub = "iter_max_" + str(self.iter_max)
			tmp_dir3 = os.path.join(tmp_dir2, iter_sub)
			if (not os.path.isdir(tmp_dir3)):
				os.mkdir(tmp_dir3)

			threshold_sub = "threshold_" + self.iter_threshold
			tmp_dir4 = os.path.join(tmp_dir3, threshold_sub)
			
			sub_dir = tmp_dir4

		elif(self.options["pre_dump_docker"]):
			# pre dump with tarssh
			logging.info("MODE: pre dump with tarssh, pipe enabled")

			tmp_dir = os.path.join(log_dir,  dir_pre_pipe)
			if (not os.path.isdir(tmp_dir)):
				os.mkdir(tmp_dir)

			tmp_dir2 = os.path.join(tmp_dir, "comlevel_" + str(self.compression_level))
			if (not os.path.isdir(tmp_dir2)):
				os.mkdir(tmp_dir2)
				
			iter_sub = "iter_max_" + str(self.iter_max)
			tmp_dir3 = os.path.join(tmp_dir2, iter_sub)
			if (not os.path.isdir(tmp_dir3)):
				os.mkdir(tmp_dir3)

			threshold_sub = "threshold_" + self.iter_threshold
			tmp_dir4 = os.path.join(tmp_dir3, threshold_sub)
			
			sub_dir = tmp_dir4
			
		elif (self.options["imgthread"]):
			# no predump, just with imgthread
			logging.info("MODE: imgthread, no pre dump")
			sub_dir = os.path.join(log_dir, "imgthread")
		else:
			# no predump, no imgthread, just tar ssh compression
			logging.info("MODE: tarssh, no pre dump")
			sub_dir = os.path.join(log_dir, "tarssh")

		if (not os.path.isdir(sub_dir)):
			os.mkdir(sub_dir)
		self.img_name = self.options["docker_image"]

		log_file = os.path.join(sub_dir, self.img_name + ".log")

		return log_file

	def print_to_file(self, file_name, iters):

		total_time = self.__end_time - self.__start_time
		restore_time = self.__restore_time
		# frozen_time = 0.0
		# frozen_times = []
		# for iter_time in self.__iter_frozen_times:
		# 	frozen_time += self.__usec2sec(iter_time)
		# 	frozen_times.append("%.2lf" % self.__usec2sec(iter_time))

		logging.info("timing append to %s", file_name)
		with open(file_name, "a+") as time_log:
			time_log.write("----")
			time_log.write("%s %s:----\n" % (self.img_name, self.options["id"]))
			time_log.write("total\t %s\n" % ( str(total_time)))
			time_log.write("initial\t %s\n" % self.init_time)
			time_log.write("syscheck\t %s\n" % self.env_check_time)
			if self.fs_first_time:
				time_log.write("preFS\t %s\n" % self.fs_first_time)
			if self.dump_time0:
				time_log.write("preDump\t %s\n" % self.dump_time0)
				time_log.write("preImage\t %s\n" % self.img_send_time0)
				time_log.write("sleepTime\t %s\n" % self.sleep_time)
				
			time_log.write("finalDump\t %s\n" % self.dump_time)
		
			time_log.write("finalFS\t %s\n" % self.fs_final_time)
			time_log.write("daemonReload\t %s\n" % self.reload_time)
			# time_log.write("\t\t: parallel with img sending")

			time_log.write("imgFinal\t%s\n" % self.img_final_time)

			if self.img_diff_time:
				time_log.write("imgDiff\t %s\n" % self.img_diff_time)
			time_log.write("imgSend\t %s\n" % self.__img_sync_time)
			# time_log.write("imgSend\t %s\n" % self.img_send_time)

			if self.diff_apply_time:
				time_log.write("memApply\t %s\n" % self.diff_apply_time)

			time_log.write("restoreSrc\t %s\n" % self.target_restore_time)
			time_log.write("restoreDst\t %s\n" % restore_time)

			self.down_est1 = self.done_restore_time - self.done_dump_time
			time_log.write("DownTime:done_restore-done_dump\t %s\n" % 
						self.down_est1)
			self.down_est2 = self.done_restore_time - self.start_dump_time
			time_log.write("DownTime:done_restore-start_dump\t %s\n" % 
						self.down_est2)
			self.down_est3 = (self.down_est1 + self.down_est2)/2
			time_log.write("DownTime:average\t %s\n" % self.down_est3)
			
			time_log.write("\n")
			
			if self.total_iter > 0:
				time_log.write("TotalIter:\t %s\n" % self.total_iter)
				for i in range(self.total_iter):
					time_log.write("Iter %s time: \t %s\n" % (i+1, self.iter_times[i]))
			
			time_log.write("\n")

			# size info
			if self.transferred_cpuinfo_size:
				time_log.write("transferred_cpuinfo_size\t %s\n" % str(self.transferred_cpuinfo_size))
			# on --nocompression mode and --imgthread mode
			if self.transferred_img_size:
				time_log.write("transferred_img_size\t %s\n" % str(self.transferred_img_size))
			# on --pre-dump-docker --imgthread mode.
			if self.transferred_memdiff_size:
				time_log.write("transferred_memdiff_size\t %s\n" % str(self.transferred_memdiff_size))


			time_log.write("\n")
			if self.total_iter > 0:
				time_log.write("TotalIter:\t %s\n" % self.total_iter)
				for i in range(self.total_iter):
					time_log.write("Iter %s size: \t %s\n" % (i+1, self.iter_sizes[i]))
			
			time_log.write("\n")

			# dd logs contains size info
			if not self.options["nocompression"]:
				# compression mode: we will have at least fs are using tarssh
				# get tar ssh log file lists and append them to time_log
				tarssh_logs = []

				if iters.fs.dd_log_file_path:
					tarssh_logs.append(iters.fs.dd_log_file_path)
				if iters.fs.tar_log_file_path:
					tarssh_logs.append(iters.fs.tar_log_file_path)

				if iters.img.dd_log_file_path:
					tarssh_logs.append(iters.img.dd_log_file_path)
				if iters.img.tar_log_file_path:
					tarssh_logs.append(iters.img.tar_log_file_path)
				
				for logfile in tarssh_logs:
					if not os.path.isfile(logfile):
						logging.info("WARNNING: logfile missing:%s", logfile)
						continue
					logging.info("append tarssh log: %s", logfile)
					self.append_file_content(logfile, time_log)

			# end size info
			time_log.write("\n\n\n")


	def append_file_content(self, srcfile, dstfilehandle):
		dstfilehandle.write(srcfile+":\n")
		with open (srcfile, 'r') as readfile:
			lines = readfile.read()
		dstfilehandle.write(lines)
		dstfilehandle.write("\n")

	def logging_from_file(self, logfile):
		with open (logfile, 'r') as readfile:
			lines = readfile.read()
		logging.info(logfile+":\n"+lines+"\n")

	def __print_overall(self, iters):

		total_time = self.__end_time - self.__start_time
		if self.sleep_time:
			total_time = total_time - self.sleep_time
			
		restore_time = self.__restore_time
		# frozen_time = 0.0
		# frozen_times = []
		# for iter_time in self.__iter_frozen_times:
		# 	frozen_time += self.__usec2sec(iter_time)
		# 	frozen_times.append("%.2lf" % self.__usec2sec(iter_time))

		# logging.info("\t total time is ~%.2lf sec", total_time)

		logging.info("\t total time is:\t %s s", str(total_time))
		logging.info("\t initial setup:\t %s s", self.init_time)
		logging.info("\t sys env check:\t %s s", self.env_check_time)
		if self.fs_first_time:
			logging.info("\t first FS sync:\t %s s", self.fs_first_time)
		if self.dump_time0:
			logging.info("\t pre dump time:\t %s s", self.dump_time0)
			logging.info("\t pre Image sync:\t %s s", self.img_send_time0)
			logging.info("\t sleeptime:\t %s s", self.sleep_time)

			
		logging.info("\t mem dump time:\t %s s", self.dump_time)
	
		logging.info("\t final FS sync:\t %s s", self.fs_final_time)
		logging.info("\t daemon reload:\t %s s", self.reload_time)
		logging.info("\t\t: parallel with img sending")

		logging.info("\t Image sending:\t %s s", self.img_send_time)
		# logging.info("\t img.sync_time:\t %s s", self.__img_sync_time)
		# logging.info("\t  frozen time is ~%.2lf sec (%s)", frozen_time,
			# str(frozen_times))
		if self.diff_apply_time:
			logging.info("\t mem diff apply:\t %s s", self.diff_apply_time)

		logging.info("\t restore (src):\t %s s", self.target_restore_time)
		logging.info("\t restore (dst):\t %s s", restore_time)

		self.down_est1 = self.done_restore_time - self.done_dump_time
		logging.info("\t est. down time (done restore - done dump):\t %s s", 
					self.down_est1)
		self.down_est2 = self.done_restore_time - self.start_dump_time
		logging.info("\t est. down time (done restore - start dump):\t %s s", 
					self.down_est2)
		self.down_est3 = (self.down_est1 + self.down_est2)/2
		logging.info("\t estimated down time (average from above two):\t %s s", 
					self.down_est3)

		logging.info("\n")
		
		if self.total_iter > 0:
			logging.info("TotalIter:\t %s\n" % self.total_iter)
			for i in range(self.total_iter):
				logging.info("Iter %s time: \t %s" % (i+1, self.iter_times[i]))
		
		logging.info("\n")

		# size info
		if self.transferred_cpuinfo_size:
			logging.info("transferred_cpuinfo_size\t %s\n" % str(self.transferred_cpuinfo_size))
		# on --nocompression mode and --imgthread mode
		if self.transferred_img_size:
			logging.info("transferred_img_size\t %s\n" % str(self.transferred_img_size))
		# on --pre-dump-docker --imgthread mode.
		if self.transferred_memdiff_size:
			logging.info("transferred_memdiff_size\t %s\n" % str(self.transferred_memdiff_size))

		logging.info("\n")
		if self.total_iter > 0:
			logging.info("TotalIter:\t %s\n" % self.total_iter)
			for i in range(self.total_iter):
				logging.info("Iter %s size: \t %s" % (i+1, self.iter_sizes[i]))
		
		logging.info("\n")
		# dd logs contains size info
		if not self.options["nocompression"]:
			# compression mode: we will have at least fs are using tarssh
			# get tar ssh log file lists and append them to time_log
			tarssh_logs = []

			if iters.fs.dd_log_file_path:
				tarssh_logs.append(iters.fs.dd_log_file_path)
			if iters.fs.tar_log_file_path:
				tarssh_logs.append(iters.fs.tar_log_file_path)

			if iters.img.dd_log_file_path:
				tarssh_logs.append(iters.img.dd_log_file_path)
			if iters.img.tar_log_file_path:
				tarssh_logs.append(iters.img.tar_log_file_path)
			
			for logfile in tarssh_logs:
				if not os.path.isfile(logfile):
					logging.info("WARNNING: logfile missing:%s", logfile)
					continue
				logging.info("append tarssh log: %s", logfile)
				self.logging_from_file(logfile)

			# end size info


	def __usec2sec(self, usec):
		return usec / 1000000.


class restart_stats:
	def __init__(self):
		self.__start_time = 0.0
		self.__end_time = 0.0

	def handle_start(self):
		self.__start_time = time.time()

	def handle_preliminary(self, fsstats):
		_print_fsstats(fsstats)

	def handle_iteration(self, fsstats):
		_print_fsstats(fsstats)

	def handle_stop(self):
		self.__end_time = time.time()
		self.__print_overall()

	def __print_overall(self):
		logging.info("\t   total time is ~%.2lf sec",
			self.__end_time - self.__start_time)


def _print_dstats(dstats):
	if dstats:
		logging.info("\tDumped %d pages, %d skipped",
			dstats.pages_written, dstats.pages_skipped_parent)


def _print_fsstats(fsstats):
	if fsstats:
		mbytes_xferred_str = ""
		mbytes_xferred = fsstats.bytes_xferred >> 20
		if mbytes_xferred != 0:
			mbytes_xferred_str = " (~{0}Mb)".format(mbytes_xferred)
		logging.info("\tFs driver transfer %d bytes%s",
			fsstats.bytes_xferred, mbytes_xferred_str)
