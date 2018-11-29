import os
import fcntl
import errno
import logging
import socket
import tarfile


class tarfile_fileobj_wrap:
	"""Helper class provides read/write interface for socket object

	Current helper class wrap recv/send socket methods in read/write interface.
	This functionality needed to workaround some problems of socket.makefile
	method for sockets constructed from numerical file descriptors passed
	through command line arguments.
	"""

	def __init__(self, sk):
		self.__sk = sk
		self.__nread = 0
		self.__nwrite = 0
		# logging.info("tarfile_fileobj_wrap.__init__() done.")

	def read(self, size=0x10000):
		data = self.__sk.recv(size)
		self.__nread += len(data)
		#logging.info("tarfile_fileobj_wrap.read(%d) done.", size)
		return data

	def write(self, data):
		self.__sk.sendall(data)
		self.__nwrite = self.__nwrite + len(data)
		#logging.info("tarfile_fileobj_wrap.write() done. length: %d", len(data))
		return len(data)

	def discard_unread_input(self):
		"""
		Cleanup socket after tarfile

		tarfile module always align data on source side according to RECORDSIZE
		constant, but it don't read aligning bytes on target side in some cases
		depending on received buffer size. Read aligning manually and discard.
		"""

		# logging.info("tarfile_fileobj_wrap.discard_unread_input(), start")

		remainder = self.__nread % tarfile.RECORDSIZE
		if remainder > 0:
			self.__sk.recv(tarfile.RECORDSIZE - remainder, socket.MSG_WAITALL)
			self.__nread += tarfile.RECORDSIZE - remainder
			logging.info("tarfile_fileobj_wrap.discard_unread_input(). Remainder: %d", remainder)

		# logging.info("tarfile_fileobj_wrap.discard_unread_input(): Remainder: %d", remainder)
		# logging.info("tarfile_fileobj_wrap.discard_unread_input(): Total size %d", self.__nread)

	def get_read_size(self):
		return self.__nread
		
	def get_write_size(self):
		return self.__nwrite

class tarfile_bz2_fileobj_wrap:
	"""Helper class provides read/write interface for socket object

	Current helper class wrap recv/send socket methods in read/write interface.
	This functionality needed to workaround some problems of socket.makefile
	method for sockets constructed from numerical file descriptors passed
	through command line arguments.
	"""

	def __init__(self, sk):
		self.__sk = sk
		self.__nread = 0
		self.__nwrite = 0
		# logging.info("Lele: tarfile_bz2_fileobj_wrap.__init__() done.")

	# def read(self, size=0x10000):
	def read(self, size=0x10001):
		data = self.__sk.recv(size)
		# data = self.discard_padding_data(data)
		self.__nread += len(data)
		#logging.info("tarfile_bz2_fileobj_wrap.read(%d), data length: %d", size, self.__nread)
		return data

	def write(self, data):
		len_sent = self.__sk.send(self.padding_data(data))
		self.__nwrite = self.__nwrite + len_sent
		# len_sent = self.__sk.sendall(data)
		#logging.info("tarfile_bz2_fileobj_wrap.write() done. length: %d, sent: %s", len(data), str(len_sent))
		return len_sent

	def padding_data(self, data):
		"""
		tarfile module always align data on source side according to RECORDSIZE
		constant, but it don't align bytes for bz2 file format. Now aligning manually.
		"""

		#logging.info("tarfile_bz2_fileobj_wrap.align_data")
		# logging.info("tarfile.RECORDSIZE: %d", tarfile.RECORDSIZE)
		padding_size = tarfile.RECORDSIZE - ( len(data) % tarfile.RECORDSIZE )
		if padding_size == tarfile.RECORDSIZE:
			return data
		else:
			i = padding_size 
			while i > 0:
				data = data + '\0'
				i = i - 1
			logging.info("tarfile_bz2_fileobj_wrap.padding_data(), done, padding size: %d", padding_size)

		return data

	# def discard_padding_data(self, data):
	# 	"""
	# 	tarfile module could automatically discard the padding data, so no need to do anything here.
	# 	"""

	# 	# logging.info("tarfile_bz2_fileobj_wrap.align_data, start")
	# 	# while len(data) < tarfile.RECORDSIZE:
	# 	# 	data = data + '\0'

	# 	# logging.info("tarfile_bz2_fileobj_wrap.align_data(), done, padding size: %d",len(data))

	# 	return data

	def discard_unread_input(self):
		"""
		Cleanup socket after tarfile

		tarfile module always align data on source side according to RECORDSIZE
		constant, but it don't read aligning bytes on target side in some cases
		depending on received buffer size. Read aligning manually and discard.
		"""

		#logging.info("tarfile_bz2_fileobj_wrap.discard_unread_input(), start")

		remainder = self.__nread % tarfile.RECORDSIZE
		if remainder > 0:
			self.__sk.recv(tarfile.RECORDSIZE - remainder, socket.MSG_WAITALL)
			self.__nread += tarfile.RECORDSIZE - remainder
			logging.info("tarfile_bz2_fileobj_wrap.discard_unread_input(). Remainder: %d", remainder)

		# logging.info("tarfile_bz2_fileobj_wrap.discard_unread_input(). Remainder: %d", remainder)
		# logging.info("tarfile_bz2_fileobj_wrap.discard_unread_input(). Total read size: %d", self.__nread)

	def get_read_size(self):
		return self.__nread
		
	def get_write_size(self):
		return self.__nwrite

class net_dev:
	def __init__(self, name=None, pair=None, link=None):
		self.name = name
		self.pair = pair
		self.link = link


def ifup(ifname):
	logging.info("\t\tUpping %s", ifname)
	os.system("ip link set %s up" % ifname)


def ifdown(ifname):
	logging.info("\t\tDowning %s", ifname)
	os.system("ip link set %s down" % ifname)


def bridge_add(ifname, brname):
	logging.info("\t\tAdd %s to %s", ifname, brname)
	os.system("brctl addif %s %s" % (brname, ifname))


def set_cloexec(sk):
	flags = fcntl.fcntl(sk, fcntl.F_GETFD)
	fcntl.fcntl(sk, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def makedirs(dirpath):
	try:
		os.makedirs(dirpath)
	except OSError as er:
		if er.errno == errno.EEXIST and os.path.isdir(dirpath):
			pass
		else:
			raise


def log_uncaught_exception(type, value, traceback):
	logging.error(value, exc_info=(type, value, traceback))


def log_header():
	OFFSET_LINES_COUNT = 3
	for i in range(OFFSET_LINES_COUNT):
		logging.info("")


def log_subprocess_output(output):
	for line in output.splitlines():
		logging.info("\t> %s", line)
