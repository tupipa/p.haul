#
# p.haul command line arguments parsers
#

import sys
import argparse
import htype
import images
import criu_api
import iters


def parse_client_args():
	"""Parse p.haul command line arguments"""

	parser = argparse.ArgumentParser("Process HAULer")
	parser.set_defaults(pre_dump=iters.PRE_DUMP_AUTO_DETECT)

	parser.add_argument("type", choices=htype.get_haul_names(),
		help="Type of hat to haul, e.g. vz, lxc, or docker")
	parser.add_argument("id", help="ID of what to haul")
	parser.add_argument("--to", help="IP where to haul")
	parser.add_argument("--docker-image", help="docker container's brief image name, used to print log files")
	parser.add_argument("--fdrpc", type=int, required=True, help="File descriptor of rpc socket")
	parser.add_argument("--fdmem", type=int, required=True, help="File descriptor of memory socket")
	parser.add_argument("--fdfs", help="Module specific definition of fs channel")
	parser.add_argument("--mode", choices=iters.MIGRATION_MODES,
		default=iters.MIGRATION_MODE_LIVE, help="Mode of migration")
	parser.add_argument("--dst-id", help="ID at destination")
	parser.add_argument("-v", default=criu_api.def_verb, type=int, dest="verbose", help="Verbosity level")
	parser.add_argument("--keep-images", default=False, action='store_true', help="Keep images after migration")
	parser.add_argument("--nocompression", default=False, action='store_true', help="don't use compression during transfering file systems (--lele)")
	parser.add_argument("--disable-pipe", default=False, action='store_true', help="don't use pipe in compression mode (--lele: only effective in pre-dump-docker mode.)")
	parser.add_argument("--imgthread", default=False, action='store_true', help="use tar threading when transferring checkpointed images (--lele)")
	parser.add_argument("--dst-rpid", default=None, help="Write pidfile on restore")
	parser.add_argument("--img-path", default=images.def_path,
		help="Directory where to put images")
	parser.add_argument("--pid-root", help="Path to tree's FS root")
	parser.add_argument("--force", default=False, action='store_true', help="Don't do any sanity checks")
	parser.add_argument("--skip-cpu-check", default=False, action='store_true',
		help="Skip CPU compatibility check")
	parser.add_argument("--skip-criu-check", default=False, action='store_true',
		help="Skip criu compatibility check")
	parser.add_argument("--log-file", help="Write logging messages to specified file")
	parser.add_argument("-j", "--shell-job", default=False, action='store_true',
		help="Allow migration of shell jobs")
	parser.add_argument('--no-pre-dump', dest='pre_dump', action='store_const',
		const=iters.PRE_DUMP_DISABLE, help='Force disable pre-dumps')
	parser.add_argument('--pre-dump', dest='pre_dump', action='store_const',
		const=iters.PRE_DUMP_ENABLE, help='Force enable pre-dumps')
	# added for docker pre dump, and use mem diff.
	parser.add_argument('--pre-dump-docker', default=False, action='store_true', help='Enable pre-dump of Docker container')
	# added for docker pre dump, and use mem diff.
	parser.add_argument('--docker-iters', default=False, action='store_true', help='Enable iteratively pre-dump of Docker container')
	parser.add_argument("--bandwidth", default="bandwidthN",
		help="bandwith flag, used as backup dir name, e.g. 'bandwidtN' will set backup dir to backupdir/bandwithN")
	parser.add_argument("--iter-max", default="4",
		help="the iterations we need to do before migration. Default is 2. The first iteration transfers the base image, all the others transfers the memory difference.")

	parser.add_argument("--iter-threshold", default="0",
		help="the threshold of memory diff size. Only transfer the memory diff if it's size is equal to or larger than this size. Default value is 0, unlimited.")

	parser.add_argument("--compression-level", default="0",
		help="compression level when sending images/storage layers.")

	# Add haulers specific arguments
	if len(sys.argv) > 1 and sys.argv[1] in htype.get_haul_names():
		htype.add_hauler_args(sys.argv[1], parser)

	return parser.parse_args()


def parse_service_args():
	"""Parse p.haul-service command line arguments"""

	parser = argparse.ArgumentParser("Process HAULer service server")

	parser.add_argument("--fdrpc", type=int, required=True, help="File descriptor of rpc socket")
	parser.add_argument("--fdmem", type=int, required=True, help="File descriptor of memory socket")
	parser.add_argument("--fdfs", help="Module specific definition of fs channel")

	parser.add_argument("--log-file", help="Write logging messages to specified file")

	parser.add_argument("--docker-image", help="docker container's brief image name, used to print log files")
	
	parser.add_argument("--bandwidth", default="bandwidthN",
		help="bandwith flag, used as backup dir name, e.g. 'bandwidtN' will set backup dir to backupdir/bandwithN")

	
	return parser.parse_args()
