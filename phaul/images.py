#
# images driver for migration (without FS transfer)
#

import os
import tempfile
import tarfile
import time
import shutil
import threading
import logging
import util
import criu_api
import subprocess as sp

def_path = "/var/local/p.haul-fs/"
log_dir = "/tmp/"

xdelta3_folder_name = "xdelta3-dir-patcher"
xdelta3_bin = "xdelta3-dir-patcher"
xdelta3_log = "xdelta3.log"

class opendir:
	def __init__(self, path):
		self._dirname = path
		self._dirfd = os.open(path, os.O_DIRECTORY)
		util.set_cloexec(self)

	def close(self):
		#lele: should check wether dirfd is still open or not.
		# if not os.access(self._dirfd, os.W_OK):
		# 	logging.info("dir %s already closed", self._dirname)
		# 	return
		try:
			os.close(self._dirfd)
			os._dirname = None
			os._dirfd = -1
		except OSError:
			logging.info("WARNNING: file %s already closed.", self._dirname)


	def name(self):
		return self._dirname

	def fileno(self):
		return self._dirfd


class untar_thread(threading.Thread):
	def __init__(self, sk, tdir):
		threading.Thread.__init__(self)
		self.__sk = sk
		self.__dir = tdir

	def run(self):
		try:
			tf_fileobj = util.tarfile_fileobj_wrap(self.__sk)
			# logging.info("untar_thread: util.tarfile done.")
			tf = tarfile.open(mode="r|", fileobj=tf_fileobj)
			# logging.info("untar_thread: tf open() done.")
			tf.extractall(self.__dir)
			# logging.info("untar_thread: tf.extractall() done.")
			tf.close()
			# logging.info("untar_thread: tf.close() done.")
			tf_fileobj.discard_unread_input()
			t_size = tf_fileobj.get_read_size()
			logging.info("untar_thread: Total received size: %s bytes", str(t_size))
		except:
			logging.exception("Exception in untar_thread")



class untar_thread_compress(threading.Thread):
	def __init__(self, sk, tdir):
		threading.Thread.__init__(self)
		self.__sk = sk
		self.__dir = tdir

	def run(self):
		try:
			tf_fileobj = util.tarfile_bz2_fileobj_wrap(self.__sk)
			# tf_fileobj = self.__sk.makefile('rb')
			# logging.info("untar_thread_compress: util.tarfile_bz2_fileobj_wrap done.")
			tf = tarfile.open(mode="r|bz2", fileobj=tf_fileobj)
			# logging.info("untar_thread_compress: tf open() done.")
			tf.extractall(self.__dir)
			# logging.info("untar_thread_compress: tf.extractall() done.")
			tf.close()
			# logging.info("untar_thread_compress: tf.close() done.")
			tf_fileobj.discard_unread_input()
			t_size = tf_fileobj.get_read_size()
			logging.info("untar_thread_compress: Total received size: %s bytes", str(t_size))
		except:
			logging.exception("Exception in untar_thread_compress")


class img_tar:
	def __init__(self, sk, dirname):
		self.tf_fileobj = util.tarfile_fileobj_wrap(sk)
		self.__tf = tarfile.open(mode="w|", fileobj=self.tf_fileobj)
		self.__dir = dirname

	def add(self, img, path = None):
		if not path:
			path = os.path.join(self.__dir, img)

		self.__tf.add(path, img)

	def close(self):
		self.__tf.close()
		t_size = self.tf_fileobj.get_write_size()
		# logging.info("tar_thread: Total send size: %s bytes", str(t_size))
		return t_size


class img_tar_compress:
	def __init__(self, sk, dirname):
		self.tf_fileobj = util.tarfile_bz2_fileobj_wrap(sk)
		# tf_fileobj = sk.makefile('wb')
		self.__tf = tarfile.open(mode="w|bz2", fileobj=self.tf_fileobj)
		self.__dir = dirname

	def add(self, img, path = None):
		if not path:
			path = os.path.join(self.__dir, img)

		self.__tf.add(path, img)

	def close(self):
		self.__tf.close()
		t_size = self.tf_fileobj.get_write_size()
		# logging.info("tar_thread_compress Total send size: %s bytes", str(t_size))
		return t_size


class phaul_images:
	WDIR = 1
	IMGDIR = 2
	# DIFFDIR = 3

	def __init__(self, typ):

		# current iter as the index of the current image direcotry we work on in self._image-iter-dirs.
		self.current_iter = 0

		# lasy sync iter as the index of the last sync image directory in self._image-iter-dirs.
		self.last_sync_iter = 0

		# diff apply iters to count how many times we apply diff on target
		self.diff_apply_iters = 0
		
		# diff mem iters to count how many times we do mem diff on source node
		self.diff_mem_iters = 0
		
		self.sync_time = 0.0
		self.diff_time = None

		self.apply_diff_time = 0.0
		self._typ = typ
		self._keep_on_close = False
		self._wdir = None
		self._current_dir = None
		self._image_iter_dirs = []

		self.disable_pipe = False

		#used both on source and target node
		self.__diff_file_name = "diff.bin"

		#used only on source node
		self.tar_log_file = "size_img_tar_total.log"
		self.dd_log_file = "size_img_tar_dd.log"
		
		self.tar_log_file_path = None
		self.dd_log_file_path = None

		self.transferred_cpuinfo_size = None
		self.transferred_img_size = None
		self.transferred_memdiff_size =None

	def save_images(self):
		logging.info("Keeping images")
		self._keep_on_close = True

	def set_options(self, opts):
		self._keep_on_close = opts["keep_images"]
		self._nocompression = opts["nocompression"]		
		self.compression_level = int (opts ["compression_level"])

		self.disable_pipe = opts["disable_pipe"]
		self._imgthread = opts["imgthread"]

		self._thost = opts["to"]

		suf = time.strftime("-%y.%m.%d-%H.%M", time.localtime())
		util.makedirs(opts["img_path"])
		self.wdir = tempfile.mkdtemp("", "%s%s-" % (self._typ, suf), opts["img_path"])
		self._wdir = opendir(self.wdir)
		self._img_path = os.path.join(self._wdir.name(), "img")
		os.mkdir(self._img_path)

	def close(self):
		if not self._wdir:
			return

		self._wdir.close()
		if self._current_dir:
			self._current_dir.close()

		# if not self._keep_on_close:
		# 	logging.info("Removing images")
		# 	shutil.rmtree(self._wdir.name())
		# else:
		# 	# logging.info("Images are kept in %s", self._wdir.name())
		# 	logging.info("Bye from images.py")
		pass

	def img_sync_time(self):
		return self.sync_time

	def new_image_dir(self):
		if self._current_dir:
			self._current_dir.close()
		self.current_iter += 1
		img_dir = "%s/%d" % (self._img_path, self.current_iter)
		logging.info("\tMaking directory %s", img_dir)
		os.mkdir(img_dir)

		#Lele: maintain all iter dirs in a list self._image_iter_dirs
		self._image_iter_dirs.append(img_dir)

		self._current_dir = opendir(img_dir)

	def pop_image_dir(self):
		"""
		delete the last image directory 
		"""

		if self._current_dir:
			self._current_dir.close()
		img_dir = self._image_iter_dirs.pop()
		shutil.rmtree(img_dir)
		logging.info("image directory %s deleted using shutil", img_dir)
		if os.path.isdir(self._diff_dir):
			# if diff dir already exists, rm it
			shutil.rmtree(self._diff_dir)

		img_dir = self._image_iter_dirs[-1]
		self._current_dir = opendir(img_dir)
		logging.info("**** current image directory reset to %s", img_dir)

		# reduce current iter 
		self.current_iter -= 1

	def image_dir_fd(self):
		return self._current_dir.fileno()

	def work_dir_fd(self):
		return self._wdir.fileno()

	def image_dir(self):
		return self._current_dir.name()

	def image_path(self):
		return self._img_path

	# create a dir to store combined mems
	# used only on target node.
	# dir is named as /img/final-02
	def new_final_dir(self, new_iter, old_iter):
		if self._current_dir:
			self._current_dir.close()
		# self.current_iter += 1
		self._final_dir = "%s/%s-%d-%d" % (self._img_path, 'combined', new_iter, old_iter)
		logging.info("\tSet final (combined) directory %s", self._final_dir)

		if os.path.isdir(self._final_dir):
			raise Exception("final dir already exits, what happened?")

		# os.mkdir(_final_dir) # don't make dir since xdelta3 will create it if doesn't exists.
		return self._final_dir
	
	def get_final_dir(self):
		return self._final_dir
		
	# get the diff dir name according to iter Num.
	# dir is named as /img/diff-01-02
	def get_diff_dir_name(self, old_iter, new_iter):
		
		diff_dir = "%s/%s-%d-%d" % (self._img_path, "diff", old_iter, new_iter)
		
		return diff_dir

	# create a new dir store mem diff 
	# used only on source node: to store diff. 
	#	on target node, diff are stored in iter image direstories: /img/2 .. /img/iterN
	# dir is named as /img/diff-01-02
	def new_diff_dir(self, old_iter, new_iter):
		if self._current_dir:
			self._current_dir.close()
		# self.current_iter += 1
		self._diff_dir = self.get_diff_dir_name(old_iter, new_iter)

		if os.path.isdir(self._diff_dir):
			# dir already exists, rm it
			shutil.rmtree(self._diff_dir)

		logging.info("\tMaking diff directory %s", self._diff_dir)
		os.mkdir(self._diff_dir)
		return self._diff_dir

	# used both on source and target
	# on source: it used this list to find which dirs to do diff
	# on target: it used this list to find from which dirs to apply the diff patch
	def image_iter_dir_list(self):
		return self._image_iter_dirs

	def work_dir(self):
		return self._wdir.name()

	def prev_image_dir(self):
		if self.current_iter == 1:
			return None
		else:
			return "../%d" % (self.current_iter - 1)

	# Lele: get mem diff, only used on source node
	#	- create new diff dir
	#	- call xdelta3 on 'img/iterN-1 img/iterN', and store diff to diff dir
	def get_mem_diff(self):

		# count the mem diff operation
		self.diff_mem_iters += 1
		
		log_file_string = os.path.join(self._wdir.name(), xdelta3_log)

		logging.info("get mem diff using xdelta3 diff ...log file: %s", log_file_string)

		start = time.time()

		logf = open(log_file_string, "a+")

		self.new_diff_dir(self.last_sync_iter, self.current_iter)

		iter_dirs = self._image_iter_dirs
		# new_dir = iter_dirs[len(iter_dirs)-1]
		new_dir = iter_dirs[self.current_iter-1]
		old_dir = iter_dirs[self.last_sync_iter-1]
		diff_file = os.path.join(self._diff_dir,self.__diff_file_name)
		# now call xdelta3 on the two dirs and 

		root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) 
		xdelta3_bin_path = os.path.join(root_path, xdelta3_folder_name, xdelta3_bin)
		cmdstring = "python3 " + xdelta3_bin_path + " diff " + old_dir + " " + new_dir + " " + diff_file + " "

		logging.info("lele: cmd: %s", cmdstring)

		ret = sp.call(cmdstring,
			stdout = logf, stderr = logf, shell=True)

		if ret != 0:
			raise Exception("failed running xdelta3 diff to get mem diff, log: %s", log_file_string)

		endTime = time.time()
		self.diff_time = endTime - start
		logging.info ("\nlele: xdelta3: get diff time: %s", str(self.diff_time))
		return True


	# Lele: get mem diff size of the last mem_diff
	def get_mem_diff_size(self):

		diff_file = os.path.join(self._diff_dir,self.__diff_file_name)
		file_stat = os.stat(diff_file)
		diff_size = file_stat.st_size
		logging.info ("\nlele: get diff size: %d Bytes", diff_size)
		return diff_size


	# Lele: apply mem diff, only used on target node
	#	- create a new final dir to store combined images
	#	- call xdelta3 on 'img/iterN-1 img/iterN', and store combined image to final dir
	def apply_mem_diff(self):

		self.diff_apply_iters += 1

		log_file_string = os.path.join(self._wdir.name(), xdelta3_log)

		logging.info("apply mem diff using xdelta3 apply...log file: %s", log_file_string)

		start = time.time()

		logf = open(log_file_string, "a+")

		iter_dirs = self._image_iter_dirs
		# iter_count = len(iter_dirs)
		# if iter_count != 2:
		# 	raise Exception("currently only support 2 iterations!")

		# for the second iter, the last sync iter should be the base image iter
		# set the initialized last sync iter as 1.
		if self.current_iter == 2:
			self.last_sync_iter = 1

		# for all other cases (where cur iter > 2), last sync iter should be smaller than cur iter.
		elif self.current_iter <= self.last_sync_iter:
			raise Exception("current iter(%d) is no more than last sync iter(%d), no mem diff to apply.",
				self.current_iter, self.last_sync_iter)
		
		# for itr in range(0, iter_count-1):
		# new_dir = iter_dirs[1]
		# old_dir = iter_dirs[0]
		
		new_dir = iter_dirs[self.current_iter-1]
		old_dir = iter_dirs[self.last_sync_iter-1]

		diff_file = os.path.join(new_dir,self.__diff_file_name)
		
		final_dir = self.new_final_dir(self.current_iter, self.last_sync_iter)
		
		# now call xdelta3 on one base dir and a patch file 
		# and get results in final_dir

		# lele: this could be put into __init__ ? No, because only this func need this on either host.
		root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) 
		xdelta3_bin_path = os.path.join(root_path, xdelta3_folder_name, xdelta3_bin)

		cmdstring = "python3 " + xdelta3_bin_path + " apply " + old_dir + " " + diff_file + " " + final_dir + " "

		logging.info("lele: cmd: %s", cmdstring)

		# test exception rpc call.
		# raise Exception("failed running xdelta3 to apply mem diff, log: %s", log_file_string)

		ret = sp.call(cmdstring,
			stdout = logf, stderr = logf, shell=True)

		if ret != 0:
			raise Exception("failed running xdelta3 to apply mem diff, log: %s", log_file_string)

		endTime = time.time()
		self.apply_diff_time = endTime - start
		logging.info ("\nlele: xdelta3: apply diff time: %s", str(self.apply_diff_time))

		# append final dir to image iter dirs, and set it as the last sync dir on target.
		self._image_iter_dirs.append(self._final_dir)
		self.current_iter = len(self._image_iter_dirs)
		self.last_sync_iter = len(self._image_iter_dirs)

		return True

	# Images transfer
	# Are there better ways for doing this?
	# Lele: use tar over ssh to send images
	def sync_imgs_to_target_tar_ssh(self, target_host):
		"""send the checkpointed image dir to target dir
		"""

		cdir = self.image_dir()

		target_image_dir = target_host.get_target_image_dir(phaul_images.IMGDIR)
		# logging.info("Lele: get target image dir: %s", target_image_dir)
		# logging.info("Lele: images.py workdir: %s", self._wdir.name())
		self.tar_log_file_path = os.path.join(self._wdir.name(), self.tar_log_file)
		self.dd_log_file_path = os.path.join(self._wdir.name(), self.dd_log_file)

		logf = open(self.tar_log_file_path, "a+")

		logging.info("lele: start sync_imgs_to_target_tar_ssh(), use bash tar command over ssh logging file: %s and %s ", 
		self.tar_log_file_path, self.dd_log_file_path)

		logging.info("lele: start counting time")
		start = time.time()
		
		#dst = "%s:%s" % (self.__thost, os.path.dirname(dir_name))
		dst = self._thost
		# http://meinit.nl/using-tar-and-ssh-to-efficiently-copy-files-preserving-permissions
		
		# cmdstring = "cd " + cdir + " && tar --totals -jcpf - . | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'tar jxpf - -C " + target_image_dir + "'"

		if (self.compression_level > 0 and self.compression_level < 10):

			cmdstring = "cd " + cdir + " && tar --totals -cpf - . | pigz -" + str(self.compression_level) + " - | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'pigz -" + str(self.compression_level) + " -d | tar xpf - -C " + target_image_dir + "'"

		elif (self.compression_level == 0):

			cmdstring = "cd " + cdir + " && tar --totals -cpf - . | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'tar xpf - -C " + target_image_dir + "'"

		else:
			raise Exception ("compression level %d is not defined.",compression_level)

		logging.info("lele: cmd: %s", cmdstring)

		#exit()
		ret = sp.call(cmdstring,
			stdout = logf, stderr = logf, shell=True)
		
		if ret != 0:
			raise Exception("tar over ssh failed")
	

		endTime = time.time()
		self.sync_time = endTime - start
		logging.info ("\nlele: img.sync_time while sending base images to target: %s", str(self.sync_time))
		
		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter

	def sync_imgs_diff_to_target_tar_ssh(self, target_host):
		"""send memory diff from current diff_dir to target image_dir
		"""

		ret = self.get_mem_diff()
		if not ret:
			raise Exception ("cannot get mem diff.")

		cdir = self._diff_dir

		logging.info("lele: sync_diff start counting time")
		start = time.time()

		# Lele:  this will call img.new_img_dir() on target host.
		target_host.start_iter(False)

		target_image_dir = target_host.get_target_image_dir(phaul_images.IMGDIR)
		# logging.info("Lele: get target image dir: %s", target_image_dir)
		# logging.info("Lele: images.py workdir: %s", self._wdir.name())
		self.tar_log_file_path = os.path.join(self._wdir.name(), self.tar_log_file)
		self.dd_log_file_path = os.path.join(self._wdir.name(), self.dd_log_file)

		logf = open(self.tar_log_file_path, "a+")

		logging.info("lele: send Mem Diff use piped tar command over ssh. logging file: %s and %s ", 
		self.tar_log_file_path, self.dd_log_file_path)
		
		#dst = "%s:%s" % (self.__thost, os.path.dirname(dir_name))
		dst = self._thost
		# http://meinit.nl/using-tar-and-ssh-to-efficiently-copy-files-preserving-permissions
		
		# cmdstring = "cd " + cdir + " && tar --totals -jcpf - . | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'tar jxpf - -C " + target_image_dir + "'"

		if (self.compression_level > 0 and self.compression_level < 10):
	
			cmdstring = "cd " + cdir + " && tar --totals -cpf - . | pigz -" + str(self.compression_level) + " - | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'pigz -" + str(self.compression_level) + " -d | tar xpf - -C " + target_image_dir + "'"
		 
		elif (self.compression_level == 0):

			cmdstring = "cd " + cdir + " && tar --totals -cpf - . | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'tar xpf - -C " + target_image_dir + "'"

		else:
			raise Exception ("compression level %d is not defined.",compression_level)

		logging.info("lele: cmd: %s", cmdstring)

		#exit()
		ret = sp.call(cmdstring,
			stdout = logf, stderr = logf, shell=True)
		
		if ret != 0:
			raise Exception("tar over ssh failed")

		endTime = time.time()
		self.sync_time = endTime - start
		logging.info ("\nlele: img.sync_time while sending image diffs to target: %s", str(self.sync_time))

		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter



	def sync_imgs_diff_to_target_pipe_diff_sending_apply(self, target_host):
		"""pipes to get diff >> send via ssh >> apply diff
		- first implement diff >> send via ssh
		- TODO : send via ssh >> apply diff. need to change xdelta3-dir-patcher again.
		"""

		# ret = self.get_mem_diff()
		# if not ret:
		# 	raise Exception ("cannot get mem diff.")
		log_file_string = os.path.join(self._wdir.name(), "pipe-" + xdelta3_log)

		logging.info("get mem diff using xdelta3 diff ...log file: %s", log_file_string)

		start = time.time()

		logf = open(log_file_string, "a+")

		# self.new_diff_dir(self.current_iter - 1, self.current_iter)

		iter_dirs = self._image_iter_dirs

		if (self.last_sync_iter == self.current_iter):
			raise Exception("current iter should not be the same as last sync iter at this point...")
		
		new_dir = iter_dirs[self.current_iter-1]
		old_dir = iter_dirs[self.last_sync_iter-1]

		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter
		
		# diff_file = os.path.join(self._diff_dir,self.__diff_file_name)
		# now call xdelta3 on the two dirs and 

		root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) 
		xdelta3_bin_path = os.path.join(root_path, xdelta3_folder_name, xdelta3_bin)

		logging.info("lele: piped diff->ssh->targetdir start counting time")
		start = time.time()

		# Lele:  this will call img.new_img_dir() on target host.
		target_host.start_iter(False)

		target_image_dir = target_host.get_target_image_dir(phaul_images.IMGDIR)

		self.dd_log_file_path = os.path.join(self._wdir.name(), self.dd_log_file)

		logging.info("lele: send Mem Diff use piped tar command over ssh. logging file: %s and %s ", 
		self.tar_log_file_path, self.dd_log_file_path)
		
		dst = self._thost

		target_diff_file = os.path.join(target_image_dir,self.__diff_file_name)

		# run the piping command 
		#	xdelta3 diff old new | dd | ssh dd of = diff.bin
		# TODO: This command could also be extended to applying stage:
		#	xdelta3 diff old new | dd | ssh dd | xdelta3 apply old_dir new_dir
		#	

		# cmdstring = "python3 " + xdelta3_bin_path + "2 diff " + old_dir + " " + new_dir + " 2>> " + log_file_string + " | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 dd of=" + target_diff_file + " "

		if (self.compression_level > 0 and self.compression_level < 10):

			cmdstring = "python3 " + xdelta3_bin_path + "2 diff " + old_dir + " " + new_dir + " 2>> " + log_file_string + " | pigz -" + str(self.compression_level) + " - | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 'pigz -" + str(self.compression_level) + " -d > " + target_diff_file + "'"
		 
		elif (self.compression_level == 0):

			cmdstring = "python3 " + xdelta3_bin_path + "2 diff " + old_dir + " " + new_dir + " 2>> " + log_file_string + " | dd 2>>" + self.dd_log_file_path + " | ssh " + dst + " -p 22 dd of=" + target_diff_file + " "


		else:
			raise Exception ("compression level %d is not defined.",compression_level)


		logging.info("lele: cmd: %s", cmdstring)
		
		ret = sp.call(cmdstring,
			stdout = logf, stderr = logf, shell=True)

		if ret != 0:
			raise Exception("pipe xdelta3 | dd | ssh | dd failed")

		endTime = time.time()
		self.sync_time = endTime - start
		logging.info ("\nlele: img.sync_time while sending image diffs to target: %s", str(self.sync_time))

	# Images transfer
	# Are there better ways for doing this?
	# Lele: use sync_imgs_to_target_tar_ssh() instead

	def sync_imgs_to_target_old(self, target_host, htype, sk):

		logging.info("lele: start counting time for image pack&sending after checkpointed")

		start = time.time()
		cdir = self.image_dir()

		target_host.start_accept_images(phaul_images.IMGDIR)
		tf = img_tar(sk, cdir)

		logging.info("\tPack")
		for img in filter(lambda x: x.endswith(".img"), os.listdir(cdir)):
			tf.add(img)
		start2 = time.time()
		logging.info("\tAdd htype images")
		for himg in htype.get_meta_images(cdir):
			tf.add(himg[1], himg[0])

		# tf.close()
		t_size = tf.close()
		self.transferred_img_size = t_size
		logging.info("tar_thread: transferred_img_size: %s bytes", str(t_size))
		target_host.stop_accept_images()

		endTime = time.time()
		self.sync_time = endTime - start
		imageTime = start2 -start
		logging.info ("\nlele: img.sync_time while sending image & meta_images to target: %s", str(self.sync_time))
		# logging.info ("\n\t within those, the time for chked images: %s\n", str(imageTime))
		
		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter


	# Images transfer
	# Are there better ways for doing this?
	# LELE: USE bzip2 compression
	def sync_imgs_to_target_tar_thread(self, target_host, htype, sk):

		logging.info("lele: start counting time for image pack&sending after checkpointed")

		start = time.time()
		cdir = self.image_dir()

		target_host.start_accept_images_compress(phaul_images.IMGDIR)
		tf = img_tar_compress(sk, cdir)

		logging.info("\tPack")
		for img in os.listdir(cdir):
			tf.add(img)

		# tf.close()
		t_size = tf.close()
		self.transferred_img_size = t_size
		logging.info("tar_thread_compress: transferred_img_size: %s bytes", str(t_size))
		target_host.stop_accept_images_compress()

		endTime = time.time()
		self.sync_time = endTime - start
		logging.info ("\nlele: img.sync_time while sending image to target: %s", str(self.sync_time))

		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter
		
	
	def sync_imgs_diff_to_target_tar_thread(self, target_host, htype, sk):
		
		ret = self.get_mem_diff()
		if not ret:
			raise Exception ("cannot get mem diff.")

		start = time.time()

		cdir = self._diff_dir

		# Lele:  this will call img.new_img_dir() on target host.
		target_host.start_iter(False)

		target_host.start_accept_images_compress(phaul_images.IMGDIR)
		tf = img_tar_compress(sk, cdir)

		logging.info("\tPack")
		for img in os.listdir(cdir):
			tf.add(img)

		# tf.close()
		t_size = tf.close()
		self.transferred_memdiff_size = t_size
		logging.info("tar_thread_compress: transferred_memdiff_size: %s bytes", str(t_size))
		target_host.stop_accept_images_compress()

		endTime = time.time()
		self.sync_time = endTime - start
		logging.info ("\nlele: img.sync_time while sending image diffs: %s", str(self.sync_time))

		# set current iter as last sync_iter
		self.last_sync_iter = self.current_iter
		
		
	def sync_imgs_to_target(self, target_host, htype, sk):

		if self._nocompression:
			logging.info("images.py: legacy mode, no compression over ssh, using rpc tar threading; send imgs with state.json and descriptors.json")
			self.sync_imgs_to_target_old(target_host, htype, sk)
		else :
			if self._imgthread:
				logging.info("images.py: compression mode. use tar threads with compression to send checkpointed img dir")
				self.sync_imgs_to_target_tar_thread(target_host, htype, sk)
			else:
				logging.info("images.py: compression mode. using tar over ssh to send checkpointed img dir")
				self.sync_imgs_to_target_tar_ssh(target_host)

	def sync_imgs_diff_to_target(self, target_host, htype, sk):
		
		if self._imgthread:
			logging.info("images.py: predump mode, imgthread for diff dir, pipe disabled")
			self.sync_imgs_diff_to_target_tar_thread(target_host, htype, sk)
		elif self.disable_pipe:
			logging.info("images.py: predump mode, tar ssh, pipe disabled")
			self.sync_imgs_diff_to_target_tar_ssh(target_host)
		else:
			logging.info("images.py: predump mode, tar ssh, pipe enabled. using piped xdelta3 | ssh | xdelta3 to send mem diff to img dir")
			self.sync_imgs_diff_to_target_pipe_diff_sending_apply(target_host)


	def send_cpuinfo(self, target_host, sk):
		# if self._imgthread:
		if not self._nocompression:
			# compress via imgthread.
			target_host.start_accept_images_compress(phaul_images.WDIR)
			tf = img_tar_compress(sk, self.work_dir())
			tf.add(criu_api.cpuinfo_img_name)
			t_size = tf.close()
			self.transferred_cpuinfo_size = t_size
			logging.info("tar_thread_compress: transferred_cpuinfo_size: %s bytes", str(t_size))
			target_host.stop_accept_images_compress()
		else: 
			target_host.start_accept_images(phaul_images.WDIR)
			tf = img_tar(sk, self.work_dir())
			tf.add(criu_api.cpuinfo_img_name)
			t_size = tf.close()
			self.transferred_cpuinfo_size = t_size
			logging.info("tar_thread: transferred_cpuinfo_size: %s bytes", str(t_size))
			target_host.stop_accept_images()

	def start_accept_images(self, dir_id, sk):
		if dir_id == phaul_images.WDIR:
			dirname = self.work_dir()
		else:
			dirname = self.image_dir()

		self.__acc_tar = untar_thread(sk, dirname)
		self.__acc_tar.start()
		logging.info("Started images server at: %s", dirname)
		
	def start_accept_images_compress(self, dir_id, sk):
		if dir_id == phaul_images.WDIR:
			dirname = self.work_dir()
		else:
			dirname = self.image_dir()

		self.__acc_tar_compress = untar_thread_compress(sk, dirname)
		self.__acc_tar_compress.start()
		logging.info("Started images server (compress) at: %s", dirname)
		
	def get_target_image_dir(self, dir_id):
		if dir_id == phaul_images.WDIR:
			dirname = self.work_dir()
			logging.info("send work dir to source: %s", dirname)
		else:
			dirname = self.image_dir()
			logging.info("send image dir to source: %s", dirname)

		return dirname
		

	def stop_accept_images(self):
		logging.info("Waiting for images to unpack")
		self.__acc_tar.join()

	def stop_accept_images_compress(self):
		logging.info("Waiting for images to uncompress")
		while self.__acc_tar_compress.isAlive():
			logging.info("join(6.0) in while..")
			self.__acc_tar_compress.join(6.0)
		logging.info("Done uncompress")
