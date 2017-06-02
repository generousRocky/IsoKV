#!/usr/bin/env python
from __future__ import print_function
from subprocess import Popen, PIPE
import argparse
import hashlib
import shutil
import time
import yaml
import os

def erase_cmd(dev, blk, dry=None):
	"""Erase the given blk index of line 0-15 0-7"""

	cmd  = ["nvm_vblk", "line_erase", str(dev), "0", "15", "0", "7", str(blk)]

	print(" ".join(cmd))

	if dry:
		return 0, "", ""

	process = Popen(cmd, stdout=PIPE, stderr=PIPE)
	out, err = process.communicate()
	rcode = process.returncode

	return process.returncode, out, err

def backup(mpath):
	"""Create a backup of the given file at mpath"""

	hsh = hashlib.md5()
	with open(mpath, "rb") as f:
		for chunk in iter(lambda: f.read(4096), b""):
			hsh.update(chunk)
	digest = hsh.hexdigest()[:8]

	dst = os.sep.join([
		os.path.dirname(mpath),
		"%s_%s.meta" % (os.path.splitext(os.path.basename(mpath))[0], digest)
	])

	shutil.copyfile(mpath, dst)

def main(args):

	mpath = os.path.abspath(os.path.expanduser(os.path.expandvars(args.mpath)))

	backup(mpath)

	meta = [line.strip() for line in open(mpath).readlines()]
	dev = "/dev/%s" % meta[0].strip()

	for blk, state in enumerate((int(m, 16) for m in meta[3:])):
		if state not in [1, 4]:
			continue

		print("Erasing: {blk: %04d, dev: %s}" % (blk, dev))
		rcode, out, err = erase_cmd(dev, blk, args.dry)
		print("Result: {\n  rcode: %d,\n  out: %s,\n  err: %s\n}" % (
			rcode, out[:70], err[:70]
		))

		if not args.dry:
			continue

		meta[blk+3] = "0x08" if rcode else "0x02"

		with open(mpath, "w") as mfile:
			for line in meta:
				mfile.write("%s\n" % line)

if __name__ == "__main__":
	PRSR = argparse.ArgumentParser(description='vblk purging')
	PRSR.add_argument('--mpath', type=str, required=True)
	PRSR.add_argument('--dry', action='store_true')

	PRSR.set_defaults(dry=False)

	ARGS = PRSR.parse_args()

	main(ARGS)
