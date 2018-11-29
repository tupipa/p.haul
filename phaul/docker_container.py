#!/usr/bin/python
# This program can get a container's rootfs sha256 ID.
# This only works for docker-1.10-dev experimental 
# Author: Lele Ma, Mar 26 2017

import os
import os.path
import commands
import sys
import logging
import time
import signal
import json

# index for writing mapping to files
LAYERID = 0
CACHEID = 1
DELIMITER = ":"

# constains directory for docker
docker_bin = "/usr/bin/docker"
docker_dir = "/var/lib/docker/0.0/"
docker_run_meta_dir = "/var/run/docker/execdriver/native"

class docker_layer_map:
	"""
	 read layerID:cacheID mapping from docker file system, using class 'docker_layer_map'

	 for source node:
	  - function: mapping stack of cache_ids back to stack of layer_ids on source node.
	  - function: writing stack of layer_ids to file. in order to transfer to target.
	
	 for target node:
	  - function: read stack of layer_ids from file, in order to get layers on target.
	  - function: mapping stack of layer_ids to stack of cache_ids on target node.
	  - function: write stack of cache_ids to file. Ultimately, the file should be /aufs/layers/rootfs_id

	 variables:
	 	- _cache_ids{}: {"layerID":"cacheID"}
		- _layer_ids{}: {"cacheID":"layerID"}
		- _layerdb_sha256_dir: ./image/aufs/layerdb/sha256/ori_image_layer_id/, in this
			directory, there are 5 files: cache-id, diff, parent, size, tar.split.json.gz
		
	"""

	def __init__(self):
		self._layerdb_sha256_dir = ""

		#store dictionay: <layer_id, cache_id>
		self._cache_ids = {}
		#store dictionay: <cache_id, layer_id>
		self._layer_ids = {}

		self.load_all_stuff()

	def load_all_stuff(self):
		self.get_layerdb_sha256_dir()
		self.get_id_maps()

	def get_layerdb_sha256_dir(self):
		"""this is /image/aufs/layerdb/sha256 dir; this dir contains all the layers meta data; 
		each layer has a directory named by its original layer id. The dir contains the following file:
		- cache-id: it stores the original image layer id and it's local cache-id;
			this could be used to remapping ids during container migration.
		- diff:
		- parent:
		- size:
		- tar.split.json.gz:
		"""
		self._layerdb_sha256_dir = os.path.join(docker_dir, "image", "aufs", "layerdb", "sha256")
		logging.info("Container layer db sha256 dir: %s", self._layerdb_sha256_dir)

	def get_diff_layer_dir(self, rootfs_id):
		return os.path.join(docker_dir, "aufs", "diff", rootfs_id)
		

	def get_id_maps(self):
		"""get all image's cache_ids in docker system, store it in a dictionary"""

		logging.info("use dir for cache_ids: " + self._layerdb_sha256_dir)

		layer_ids = os.listdir(self._layerdb_sha256_dir)
		for layer_id in layer_ids:
			cache_id_file = os.path.join(self._layerdb_sha256_dir, layer_id, "cache-id")
			cmd = 'cat ' + cache_id_file
			# logging.info("get cache-id, cmd: %s", cmd)
			ret, cache_id = commands.getstatusoutput(cmd)
			if (ret != 0):
				raise Exception ( "ERROR(" + str(ret) + "): cannot run " + cmd )
			if (len(cache_id) <= 0):
				raise Exception ("ERROR: got an empty cache-id from: " + cache_id_file)
			self._layer_ids[cache_id] = layer_id
			self._cache_ids[layer_id] = cache_id
		
		logging.info("get cache-id <-> layer-id maps")

	def convert_cacheID_stack_to_layerIDs(self, cacheID_stack):
		"""get an image layer stack with cache_ids, and convert each cache-id to original layer-id, 
		return the layer stack with original layer-id"""

		logging.info("convert a cache-id stack to layer-id stack")

		firstline = cacheID_stack[0].strip()

		if ( firstline.endswith('-init') ):
			logging.info("cacheID_stack: first line has -init, pop it\n")
			cacheID_stack.pop(0)
			logging.info("after pop: line1 is: " + cacheID_stack[0]+ "\n")
		else:
			logging.info("first line of cacheID stack has no -init")

		layerID_stack = []

		for cache_id in cacheID_stack:
			layerID_stack.append(self._layer_ids[cache_id])

		return layerID_stack

	def convert_layerID_stack_to_cacheIDs(self, layerID_stack):
		"""get an image layer stack with its original layerIDs, and convert each layer-id to the cache-id, 
			if the layer-id doesn't exists, report an error: 
			TODO this should download the missing image instead of report an error. 
		return the layer stack with original layer-id"""

		logging.info("convert a layer-id stack to cache-id stack")

		firstline = layerID_stack[0].strip()

		if ( firstline.endswith('-init') ):
			logging.info("layerID stack: first line has -init, pop it\n")
			layerID_stack.pop(0)
			logging.info("after pop: line1 is: " + layerID_stack[0]+ "\n")
		else:
			logging.info("first line of layerID stack has no -init")

		cacheID_stack = []
		for layer_id in layerID_stack:
			if layer_id in self._cache_ids:
				cacheID_stack.append(self._cache_ids[layer_id])
			else:
				raise Exception ("ERROR: cannot find layer_id locally: " + layer_id)
		return cacheID_stack

	def id_get_cache_from_layer(self, layer_id):
		"""convert an image layer's local cache-id to its original layer id"""
		return self._cache_ids[layer_id]

	def id_get_layer_from_cache(self, cache_id):
		"""convert an image layer's local cache-id to its original layer id"""
		return self._layer_ids[cache_id]

	def write_cacheID_stack_to_file(self, rootfs_id, cacheID_stack, file):
		"""write cacheID_stack to file with init"""

		openfile = open(file, 'w')

		openfile.write(rootfs_id + "-init\n")

		for cache_id in cacheID_stack:
			cache_id=cache_id.strip()
			openfile.write(cache_id + "\n")
		openfile.close()
		logging.info("write cacheID_stack to file: " + file)

	# used on target node
	def write_cacheID_stack_to_init_file(self, cacheID_stack, file):
		"""write cacheID_stack to file"""
		openfile = open(file, 'w')

		for cache_id in cacheID_stack:
			cache_id=cache_id.strip()
			openfile.write(cache_id + "\n")
		openfile.close()
		logging.info("write cacheID_stack_init to file: " + file)

	# used only on source node
	def write_layerID_stack_to_file(self, rootfs_id, layerID_stack, file):
		"""write ID_stack to file, with rootfs_id-init as first line"""
		openfile = open(file, 'w')

		openfile.write(rootfs_id + "-init\n")

		for layer_id in layerID_stack:
			layer_id = layer_id.strip()
			openfile.write(layer_id + "\n")
		openfile.close()
		logging.info("write ID_stack to file with -init: " + file)

	# used only on source node
	def get_layerID_stack_list (self, rootfs_id, layerIDs_stack):
		"""combine the original layer ID stack with the rootfs id, with rootfs_id-init as first line"""
		final_list = []
		final_list.append(rootfs_id + "-init")

		while (self.stringline_has_rootfs_id(layerIDs_stack[0])) :
			logging.info("the layerID_stack has rootfs id. Something Error on Source NODE!!")
			logging.info("will now try to pop it and try again..")
			layerIDs_stack.pop(0)

		for layer_id in layerIDs_stack:
			layer_id = layer_id.strip()
			final_list.append(layer_id)
		return final_list

	def read_cacheID_stack_from_file(self, file):
		"""read cacheID_stack from file"""
		cacheID_stack = []
		with open(file, 'r') as mapfile:
			lines = mapfile.readlines()
		for line in lines:
			line = line.strip()
			cacheID_stack.append(line)
		return cacheID_stack

	def read_layerID_stack_from_file(self, file):
		"""read layerID_stack from file , if rootfs_id-init as first line, omit it"""
		layerID_stack = []

		with open(file, 'r') as mapfile:
			lines = mapfile.readlines()

		firstline=lines[0].strip()
		logging.info("firstline: "+ firstline + "\n")

		if ( firstline.endswith('init') ):
			logging.info("first line has -init, pop it\n")
			lines.pop(0)
			logging.info("after pop: line1 is: " + lines[0]+ "\n")
		else:
			logging.info("first line of " + file + " has no -init")
			logging.info(lines[0])

		for line in lines:
			line = line.strip()
			layerID_stack.append(line)
		return layerID_stack

	def read_rootfs_id_from_file(self, file):
		"""read rootfs_id from layer_ids_stack file"""
		layerID_stack = []

		with open(file, 'r') as mapfile:
			line = mapfile.readline()

		if (not self.stringline_has_rootfs_id(line)) :
			logging.info("the file has no rootfs id. not a file with init?")
			return ""

		root_layerID = line[:-5]
		return root_layerID

	def read_rootfs_id_from_stack(self, layerIDs_stack):
		"""read rootfs_id from layer_ids_stack"""
		line=layerIDs_stack[0]
		if (not self.stringline_has_rootfs_id(line)) :
			logging.info("the layerID_stack has no rootfs id. not a file with init?")
			return ""
		root_layerID = line[:-5]
		return root_layerID

	def stringline_has_rootfs_id(self, stringline):
		"""return true if stringline ends with '-init'"""
	
		if (not stringline.strip().endswith("-init")) :
			#logging.info("the file has no rootfs id. not a file with init?")
			return False
		else:
			return True

	def get_layer_stack_file_from_rootfs_id(self, rootfs_id):
		return os.path.join(docker_dir, "aufs", "layers", rootfs_id)

	def get_layer_stack_init_file_from_rootfs_id(self, rootfs_id):
		return os.path.join(docker_dir, "aufs", "layers", rootfs_id + "-init")
		

	#def get_layer_id_maps_from_file(self, file):
		"""get all image's layer_id:cache_id mapping from a file, store it in a dictionary"""

		"""	logging.info("get image layer mapping from file: " + file)

		with open(file, 'r') as mapfile:
			maplines = mapfile.readlines()
		for line in maplines:
			line.strip()
			one_map = line.split(DELIMITER)
			cache_id = one_map(CACHEID)
			layer_id = one_map(LAYERID)
			layer_ids[cache_id] = layer_id
			cache_ids[layer_id] = cache_id
		return layer_ids, cache_ids"""

	#def write_layer_id_maps_to_file(self, file):
		"""write all image's layer_id:cache_id mapping to a file"""

		"""	logging.info("write image layer mapping from file: " + file)

		mapfile = open(file, 'w')

		for key, value in self._layer_ids.items():
			mapfile.write(key + DELIMITER + value)

		mapfile.close()

		logging.info("file written to: " + file)"""


class docker_container:
	"""
	set up an environment parameters for a container.
	for each container:
	 	- _full_cid : its full container id
		- _is_running: whether the container is running
		- _config_dir: container's configuration directory patch. something like /var/lib/docker/0.0/containers/container_id
		- _run_meta_dir: container's runtime state dir. store a file state.json.
		- _rootfs_id, _rootfs_path: store the root file system sha256 id, and the full path of the mount point.
		- _layer_stack_file: file that has the ids of both container rootfs_id-init and all underlying image layers for the container's file system 
		- _layer_stack_init_file: init file that has only the image layer ids below the container layer
		- _layer_stack[]: lists of all stacks, including rootfs as container layer 
		- _layer_stack_init: lists of stacks excluding the container layer 
		- _layerdb_mnt_dir: ./image/aufs/layerdb/mounts/__conID
		- _imagedb_content_dir: ./image/aufs/imagedb/content/sha256/
		-
	"""
	def __init__(self, cid):
		#if len(ctid)<3:
		#	raise Exception ("docker container ID must be > 3 digits")
		self._cid = cid
		self._full_cid = ""
		self._is_running = ""
		self._config_dir = ""
		self._run_meta_dir = ""

		self._rootfs_id = ""
		self._rootfs_path = ""

		self._layer_stack_file = ""
		self._layer_stack_init_file = ""
		self._layer_stack = []
		self._layer_stack_init = []

		self._layerdb_mnt_dir = ""
		self._imagedb_content_dir = ""

		self.load_all_stuff()

	def load_all_stuff(self):
		self._full_cid = self.get_full_container_ID(self._cid)
		self._is_running = self.is_running()

		self.get_config_dir()
		self.get_layerdb_mnt_dir()
		self.get_rootfs_layerdb()

		if (self._is_running):
			self.get_run_meta_dir()
			old_root = self._rootfs_path
			self.get_rootfs()
			if(old_root != self._rootfs_path):
				raise Exception ("roofs mismatch from layerdb and state.json")
		else:
			logging.exception ("WARNING: container %s is not running", self._full_cid)

		self.get_layer_stack()
		self.get_layer_stack_init()

		self.get_imagedb_content_dir()

	def is_running(self):
		"""return true if container is running"""
		cmd = 'docker ps -qf "id=' + self._full_cid + '"'
		# logging.info("run cmd: %s ", cmd)
		conID = commands.getoutput(cmd)
		if ( len(conID)==0 ):
			logging.info("container is not running")
			return False
		else:
			logging.info("container is running")
			return True

	def get_full_container_ID(self,conName):
		"""get container full id from container name or partial id
		"""
		cmd = 'docker inspect --format="{{.Id}}" ' + conName 
		# logging.info("run cmd: %s ", cmd)
		conID = ""
		ret, conID = commands.getstatusoutput(cmd)
		if (ret != 0):
			raise Exception ( "ERROR(" + str(ret) + "): no such container" )
		else:
			return conID

	def get_config_dir(self):
		self._config_dir = os.path.join(docker_dir, "containers", self._full_cid)
		logging.info("Container config dir: %s", self._config_dir)

	def get_layerdb_mnt_dir(self):
		self._layerdb_mnt_dir = os.path.join(docker_dir, "image", "aufs", "layerdb", "mounts", self._full_cid)
		logging.info("Container layer db mount dir: %s", self._layerdb_mnt_dir)

	def get_run_meta_dir(self):
		"""get runtime meta data dir, /var/run/docker/execdriver/native/cid.
		In this directory, there is one file 'state.json'. 
		This file stores the runtime configurations of the container. For example,
		- the rootfs id, etc.
		"""
		self._run_meta_dir = os.path.join(docker_run_meta_dir, self._full_cid)
		logging.info("Container run meta dir: %s", self._run_meta_dir)

	def get_rootfs(self):
		"""get the union mount point of the container file system; get it from runtime state.json"""
		with open(os.path.join(self._run_meta_dir, "state.json")) as data_file:
			data = json.load(data_file)
			self._rootfs_path = data["config"]["rootfs"]
		#self._rootfs_id = self._rootfs_path.?
		logging.info("Container rootfs from state.json: %s", self._rootfs_path)

	def get_rootfs_layerdb(self):
		"""get the union mount point of the container file system; get it statically from layerdb"""

		cmd = 'cat ' + os.path.join(self._layerdb_mnt_dir, "mount-id")
		# logging.info("run cmd: %s", cmd)
		ret, self._rootfs_id = commands.getstatusoutput(cmd)
		if (ret != 0):
			raise Exception ( "ERROR(" + str(ret) + "): cannot run " + cmd )
		self._rootfs_path = os.path.join(docker_dir, "aufs", "mnt", self._rootfs_id)
		logging.info("Container rootfs id from layerdb: %s", self._rootfs_id)
		logging.info("Container rootfs path from layerdb: %s", self._rootfs_path)

	def get_layer_stack(self):
		"""get layer directories read from 0.0/aufs/layers/rootfs (or rootfs-init? )
		it stores all the layers underlying the rootfs"""

		self._layer_stack_file = os.path.join(docker_dir, "aufs", "layers", self._rootfs_id)
		cmd = 'cat ' + self._layer_stack_file
		logging.info("get layer_stack, cmd: %s", cmd)
		ret, stacks = commands.getstatusoutput(cmd)
		if (ret != 0):
			logging.exception ( "ERROR(" + str(ret) + "): cannot run " + cmd )
		self._layer_stack = stacks.split('\n')
		return self._layer_stack

	def get_layer_stack_init(self):
		"""get layer directories read from 0.0/aufs/layers/rootfs (or rootfs-init? )
		it stores all the layers underlying the rootfs"""

		self._layer_stack_init_file = os.path.join(docker_dir, "aufs", "layers", self._rootfs_id + "-init") 
		cmd = 'cat ' + self._layer_stack_init_file
		logging.info("get layer_stack_init, cmd: %s", cmd)
		ret, stacks = commands.getstatusoutput(cmd)
		if (ret != 0):
			logging.exception ( "ERROR(" + str(ret) + "): cannot run " + cmd )
		self._layer_stack_init = stacks.split('\n')
		return self._layer_stack_init

	def get_imagedb_content_dir(self):
		"""what's this dir used for? It contains files named by each images id, which is a json file and 
		contains the container's static configs; including the diff layers"""

		self._imagedb_content_dir = os.path.join(docker_dir, "image", "aufs", "imagedb", "content", "sha256" )
		logging.info("Container image db content/sha256 dir: %s", self._imagedb_content_dir)

	def __str__(self):
		return self._full_cid
