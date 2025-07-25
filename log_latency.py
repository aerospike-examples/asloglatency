#!/usr/bin/env python3

####
#
#  Copyright (c) 2008-2019 Aerospike, Inc. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
####
#------------------------------------------------
# log_latency.py
#
# Analyze histograms in a server log file.
# Typical usage:
#	$ ./log_latency.py -h reads
# which uses defaults:
# -l /var/log/aerospike/aerospike.log
# -t 10
# -f tail
# -n 3
# -e 3
# (-d - not set, infinite duration)
# (-r - automatic with -f tail)
#------------------------------------------------


#===========================================================
# Imports
#

import datetime
import getopt
import os
import re
import select
import signal
import sys
import termios
import threading
import time
import types

#===========================================================
# Version
#

if sys.version_info[0] < 3:
    raise Exception("Asloglatency requires Python 3. Use tools package <= 3.27.x for Python 2 support.")

__version__ = '$$__version__$$'

#===========================================================
# Constants
#

DT_FMT = "%b %d %Y %H:%M:%S"
DT_WITH_MS_FMT = "%b %d %Y %H:%M:%S.%f"
DT_TO_MINUTE_FMT = "%b %d %Y %H:%M"
DT_TIME_FMT = "%H:%M:%S"
HIST_TAG_PREFIX = "histogram dump: "
HIST_WITH_NS_PATTERN = "{.+}-[a-zA-Z0-9_-]+"
HIST_TAG_PATTERNS = [HIST_TAG_PREFIX+"%s ",HIST_TAG_PREFIX+"{[a-zA-Z0-9_-]+}-%s "]
NS_HIST_TAG_PATTERNS = [HIST_TAG_PREFIX+"{%s}-%s "]
NS_SLICE_SECONDS=5
SCAN_SIZE = 1024 * 1024
HIST_BUCKET_LINE_SUBSTRING = "hist.c:"
SIZE_HIST_LIST = ["device-read-size", "device-write-size"]
RECORD_HIST_LIST = ["query-rec-count"]
#===========================================================
# Globals
#

g_rolling = False
bucket_labels = ("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", \
	"10", "11", "12", "13", "14", "15", "16")
all_buckets = len(bucket_labels)
bucket_unit = "ms"


# relative stats to input histogram
# format:
# histogram: (
#	[in order path for stat with stat name],
#	[(index of value, "name of output column")]
# )
relative_stat_info = {
	"batch-index" : (
		['batch-sub:', 'read'],
		[(0,"recs/sec")]
	)
}

#===========================================================
# Function Definitions
#

#------------------------------------------------
# bytes conversion.
#
def bytes_to_str(data):
    if data is not None:
        try:
            return data.decode("utf-8")
        except Exception:
            pass

    return data

#------------------------------------------------
# Wait (in another thread) for user to hit return key.
#
def wait_for_user_input():
	global g_rolling
	# Save terminal settings:
	fd = sys.stdin.fileno()
	old = termios.tcgetattr(fd)
	# Turn terminal echo off temporarily:
	new = old[:]
	new[3] &= ~termios.ECHO
	set_flags = termios.TCSAFLUSH
	if hasattr(termios, "TCSASOFT"):
		set_flags |= termios.TCSASOFT
	termios.tcsetattr(fd, set_flags, new)
	# Using non-blocking input method since daemons don't work in Python 2.4:
	while g_rolling:
		r, w, x = select.select([fd], [], [], 0.1)
		if len(r) != 0:
			g_rolling = False
	# Restore terminal echo:
	termios.tcsetattr(fd, set_flags, old)

#------------------------------------------------
# Also wait for user to hit ctrl-c.
#
def signal_handler(signal, frame):
	global g_rolling
	g_rolling = False

#------------------------------------------------
# Check line contains valid date format
#
def has_timestamp(line):
	try:
		dt = parse_dt(line)
		return True
	except Exception:
		return False

#------------------------------------------------
# Read a complete line from the log file.
#
def read_line(file_id):
	global g_rolling
	line = ""
	while True:
		temp_line = bytes_to_str(file_id.readline())
		if temp_line:
			if line:
				line = line + temp_line
			else:
				if has_timestamp(temp_line):
					line = line + temp_line
				else:
					continue
		if line.endswith("\n"):
			return line
		if not g_rolling:
			break
		time.sleep(0.1)

#------------------------------------------------
# Parse a histogram total from a log line.
#
def parse_total_ops(line, file_id):
	return int(line[line.rfind("(") + 1: line.rfind(" total)")])

#------------------------------------------------
# Set bucket details.
#
def set_bucket_details(hist):
	global bucket_labels, all_buckets, bucket_unit
	if any(ht in hist for ht in SIZE_HIST_LIST):
		bucket_labels = ("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", \
						"10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", \
						 "21", "22", "23", "24", "25")
		all_buckets = len(bucket_labels)
		bucket_unit = "bytes"
	elif any(ht in hist for ht in RECORD_HIST_LIST):
		bucket_labels = ("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", \
						"10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", \
						 "21", "22", "23", "24", "25")
		all_buckets = len(bucket_labels)
		bucket_unit = "records"
#------------------------------------------------
# Get one set of bucket values.
#
def read_bucket_values(line, file_id):
	values = {}
	for b in range(all_buckets):
		values[b] = 0
	total = parse_total_ops(line, file_id)
	line = read_line(file_id)
	if not line:
		return 0, 0, 0
	b_min = 0
	b_total = 0
	while True:
		found = 0
		if HIST_BUCKET_LINE_SUBSTRING in line:
			for b in range(b_min, all_buckets):
				pattern = r'.*?\(' + bucket_labels[b] + r': (.*?)\).*?'
				r = re.compile(pattern)
				if r.search(line):
					found = found + 1
					values[b] = int(r.search(line).group(1))
					b_total = b_total + values[b]
			if found == 0:
				break
		line = read_line(file_id)
		if not line:
			if b_total < total:
				# Incomplete bucket details
				return 0, 0, 0
			else:
				line = 0
				break
		if b_total >= total:
			break
		b_min = b_min + found
	return total, values, line

#------------------------------------------------
# Subtract one set of bucket values from another.
#
def subtract_buckets(new_values, old_values):
	slice_values = {}
	for b in range(all_buckets):
		if new_values[b] < old_values[b]:
			new_values[b] = old_values[b]
		slice_values[b] = new_values[b] - old_values[b]
	return slice_values

#------------------------------------------------
# Add one set of bucket values to another.
#

def add_buckets(b1_values, b2_values):
	slice_values = {}
	for b in range(all_buckets):
		slice_values[b] = b1_values[b] + b2_values[b]
	return slice_values

#------------------------------------------------
# Get the percentage of operations within every bucket.
#
def bucket_percentages(total, values):
	percentages = [0.0] * all_buckets
	if total > 0:
		for b in range(all_buckets):
			percentages[b] = (float(values[b]) / total) * 100
	return percentages

#------------------------------------------------
# Get the percentage of operations in all buckets > bucket.
#
def percentage_over(bucket, percentages):
	percentage = 0.0
	for b in range(all_buckets):
		if b > bucket:
			percentage = percentage + percentages[b]
	return percentage

#------------------------------------------------
# Parse a datetime from a log line.
#
def parse_dt(line):
	prefix = line[0: line.find(" GMT")]
	try:
		return datetime.datetime(*(time.strptime(prefix, DT_FMT)[0:6]))
	except Exception:
		# new server version supports timestamp with milliseconds
		# try to parse with new format
		return datetime.datetime(*(time.strptime(prefix, DT_WITH_MS_FMT)[0:6]))

#------------------------------------------------
# Check line contains strings from strs in given order.
#
def contains_substrings_in_order(line="", strs=[]):
	if not strs:
		return True

	if not line:
		return False

	s_str = strs[0]
	if not s_str:
		return True

	if s_str in line:
		try:
			main_str = line.split(s_str, 1)[1]

		except Exception:
			main_str = ""

		if len(strs) <= 1:
			return True

		return contains_substrings_in_order(main_str, strs[1:])

	else:
		return False

#-------------------------------------------------
# Get a stat value from line.
#
def read_stat(line, stat=[]):
	values = []
	if not stat:
		return values

	latency_pattern1 = r'%s (\d+)'
	latency_pattern2 = r'%s \(([0-9,\s]+)\)'
	latency_pattern3 = r'(\d+)\((\d+)\) %s'
	latency_pattern4 = r'%s \((\d+)'

	grep_str = stat[-1]

	m = re.search(latency_pattern1 % (grep_str), line)
	if m:
		values.append(int(m.group(1)))
		return values

	m = re.search(latency_pattern2 % (grep_str), line)
	if m:
		values = [int(x) for x in m.group(1).split(",")]
		return values

	m = re.search(latency_pattern3 % (grep_str), line)
	if m:
		values = [int(x) for x in m.groups()]

	m = re.search(latency_pattern4 % (grep_str), line)
	if m:
		values.append(int(m.group(1)))
		return values

	return values

#------------------------------------------------
# Add one list of stat values to another.
#
def add_stat_values(v1, v2):
	if not v1:
		return v2

	if not v2:
		return v1

	l1 = len(v1)
	l2 = len(v2)

	values = []
	for i in range(max(l1, l2)):
		val = 0
		if i < l1:
			val += v1[i]

		if i < l2:
			val += v2[i]

		values.append(val)

	return values

#------------------------------------------------
# Subtract one list of stat values from another.
#
def subtract_stat_values(new_values, old_values):
	values = []

	newl = len(new_values)
	oldl = len(old_values)

	for i in range(max(newl, oldl)):
		if i < newl:
			# next item from new_values
			newval = new_values[i]
			if i < oldl:
				# item available for same index in old_values
				values.append(newval - old_values[i])

			else:
				# item not available for same index in old_values
				values.append(newval)

		else:
			# item not available in new_values
			# add 0
			values.append(0)

	return values

#------------------------------------------------
# Find max from two lists of stat values.
#
def get_max_stat_values(new_values, old_values):
	values = []

	newl = len(new_values)
	oldl = len(old_values)

	for i in range(max(newl, oldl)):
		if i >= newl:
			# no item in new_values
			values.append(old_values[i])
		elif i >= oldl:
			# no item in old_values
			values.append(new_values[i])
		else:
			# items available for index i in both list
			values.append(max(old_values[i], new_values[i]))

	return values

#-------------------------------------------------
# Get a histogram at or just after the specified datetime.
#
def read_hist(hist_tags, after_dt, file_id, line = 0, end_dt=None, before_dt=None, read_all_dumps=False, relative_stat_path=[]):
	global bucket_unit
	if not line:
		# read next line
		line = read_line(file_id)

	total = 0
	values = 0
	stat_values = []
	dt = ""

	while True:
		if not line:
			return total, values, 0, 0, stat_values

		dt = parse_dt(line)

		if dt<after_dt:
			# ignore lines with timestamp before before_dt
			line = read_line(file_id)
			continue

		if end_dt and dt>end_dt:
			# found line with timestamp after end_dt
			return total, values, dt, line, stat_values

		if before_dt and dt>before_dt:
			# found line with timestamp after before_dt
			return total, values, dt, line, stat_values

		if relative_stat_path and contains_substrings_in_order(line, relative_stat_path):
			temp_sval = read_stat(line, relative_stat_path)
			stat_values = add_stat_values(stat_values, temp_sval)

		elif any(re.search(ht, line) for ht in hist_tags):
			break

		line = read_line(file_id)

	# Latency units can change in the middle of a log file.
	if 'usec' in line:
		bucket_unit = 'us'
	elif 'msec' in line:
		bucket_unit = 'ms'

	total, values, line = read_bucket_values(line, file_id)
	if not line:
		return 0, 0, 0, 0, stat_values

	if read_all_dumps or relative_stat_path:
		if not before_dt:
			before_dt = dt+datetime.timedelta(seconds=NS_SLICE_SECONDS)
		r_total, r_values, r_dt, line, r_stat_values = read_hist(hist_tags, after_dt, file_id, line, end_dt,
																		 before_dt, read_all_dumps=read_all_dumps,
																		 relative_stat_path=relative_stat_path)
		total += r_total
		if r_values:
			values = add_buckets(values, r_values)

		if r_stat_values:
			stat_values = add_stat_values(stat_values, r_stat_values)

	return total, values, dt, line, stat_values

#------------------------------------------------
# Find first log line datetime.
#
def read_head_dt(file_id):
	line = read_line(file_id)
	if not line:
		print("empty log file")
		return 0
	return parse_dt(line)

#------------------------------------------------
# Find last (complete) log line datetime.
#
def read_tail_dt(file_id, file_name):
	line_size = 2048
	while True:
		if line_size > os.stat(file_name)[6]:
			file_id.seek(0, 0)
			lines = bytes_to_str(file_id.read()).rsplit("\n", 2)
			if len(lines) == 1:
				print("shouldn't get here - shrinking file?")
				return 0
			break
		file_id.seek(-line_size, 2)
		lines = bytes_to_str(file_id.read()).rsplit("\n", 2)
		if len(lines) > 2:
			break
		line_size = line_size + 2048
	return parse_dt(lines[1])

#------------------------------------------------
# Parse (positive) timedelta from user input.
#
def parse_timedelta(arg):
	toks = arg.split(":")
	num_toks = len(toks)
	if num_toks > 3:
		return 0
	toks.reverse()
	try:
		arg_seconds = int(toks[0].strip())
		if num_toks > 1:
			arg_seconds = arg_seconds + (60 * int(toks[1].strip()))
		if num_toks > 2:
			arg_seconds = arg_seconds + (3600 * int(toks[2].strip()))
	except:
		return 0
	return datetime.timedelta(seconds = arg_seconds)


#------------------------------------------------
# Parse absolute or relative datetime from user input.
#
def parse_init_dt(arg_from, tail_dt):
	if arg_from.startswith("-"):
		# Relative start time:
		try:
			init_dt = tail_dt - parse_timedelta(arg_from.strip("- "))
		except:
			print("can't parse relative start time " + arg_from)
			return 0
	else:
		# Absolute start time:
		try:
			init_dt = datetime.datetime(\
				*(time.strptime(arg_from, DT_FMT)[0:6]))
		except:
			print("can't parse absolute start time " + arg_from)
			return 0
	return init_dt

#------------------------------------------------
# Get a timedelta in seconds.
#
def elapsed_seconds(td):
	return td.seconds + (td.days * 24 * 3600)

#------------------------------------------------
# Seek backwards to first log line with time before init_dt.
#
def seek_back(init_dt, head_dt, tail_dt, file_id, file_name):
	if init_dt == head_dt:
		file_id.seek(0, 0)
		return
	file_seconds = elapsed_seconds(tail_dt - head_dt)
	if file_seconds < 3600:
		file_id.seek(0, 0)
		return
	back_seconds = elapsed_seconds(tail_dt - init_dt)
	file_size = os.stat(file_name)[6]
	seek_size = (file_size * back_seconds) // file_seconds
	if seek_size < SCAN_SIZE:
		seek_size = SCAN_SIZE
	if seek_size >= file_size - SCAN_SIZE:
		file_id.seek(0, 0)
		return
	file_id.seek(-seek_size, 2)
	while True:
		bytes_to_str(file_id.readline())
		dt = parse_dt(bytes_to_str(file_id.readline()))
		if dt < init_dt:
			return
		if SCAN_SIZE >= file_id.tell():
			file_id.seek(0, 0)
			return
		file_id.seek(-SCAN_SIZE, 1)

#------------------------------------------------
# Generate padding.
#
def repeat(what, n):
	pad = ""
	for i in range(n):
		pad = pad + what
	return pad

#------------------------------------------------
# Print a latency data output line.
#
def print_line(slice_tag, overs, num_buckets, every_nth, rate = 0, \
		slice_seconds_actual = 0, stat_values=[], stat_index=[]):
	output = "%8s" % (slice_tag)
	if slice_seconds_actual != 0:
		output = output + "%6s" % (slice_seconds_actual)
	else:
		output = output + repeat(" ", 6)
	for i in range(num_buckets):
		if i % every_nth == 0:
			output = output + "%7.2f" % (overs[i])
	output = output + "%11.1f" % (rate)

	if stat_index:
		for idx_name in stat_index:
			if idx_name[0] < len(stat_values):
				output += "%11.1f" % (stat_values[idx_name[0]])
			else:
				output += "%12s" % ("-")

	print(output)

#------------------------------------------------
# Print usage.
#
def usage():
	print("Usage:")
	print(" -l log file")
	print("    default: /var/log/aerospike/aerospike.log")
	print(" -h histogram name")
	print("    MANDATORY - NO DEFAULT")
	print("    e.g. 'reads nonet'")
	print(" -N namespace name. If specified will display histogram latency ")
	print("    for the namespace related histogram. e.g read, write.")
	print("    Not required for")
	print("    - Non-namespace histograms, e.g. svc-demarshal")
	print("    - Fully qualified namespace histogram names, e.g. {test}-read")
	print(" -t analysis slice interval")
	print("    default: 10")
	print("    other e.g. 3600 or 1:00:00")
	print(" -f log time from which to analyze")
	print("    default: tail")
	print("    other e.g. head or 'Sep 22 2011 22:40:14' or -3600 or -1:00:00")
	print(" -d maximum duration for which to analyze")
	print("    default: not set")
	print("    e.g. 3600 or 1:00:00")
	print(" -n number of buckets to display")
	print("    default: 3")
	print(" -e show 0-th then every n-th bucket")
	print("    default: 3")
	print(" -r (roll until user hits return key or ctrl-c)")
	print("    default: set if -f tail, otherwise not set")
	print(" -V show tool version")

#------------------------------------------------
# Print version.
#
def print_version():
	sVersion = __version__.split("-")
	version = sVersion[0]
	build = sVersion[-1] if len(sVersion) > 1 else ""
    
	print("Aerospike Log Latency Tool")
	print("Version " + version)
 
	if build:
		print("Build " + build)

#------------------------------------------------
# Main function.
#
def main(arg_log, arg_hist, arg_slice, arg_from, arg_duration, \
		arg_num_buckets, arg_every_nth, arg_ns=None, arg_relative_stats=False):
	global g_rolling

	# Sanity-check some arguments:
	if arg_hist == None:
		usage()
		sys.exit(-1)
	if arg_num_buckets < 1:
		print("num_buckets must be more than 0")
		sys.exit(-1)
	if arg_every_nth < 1:
		print("every_nth must be more than 0")
		sys.exit(-1)

	# Set slice timedelta:
	slice_timedelta = parse_timedelta(arg_slice)
	if not slice_timedelta:
		print("invalid slice time " + arg_slice)
		sys.exit(-1)

	# sometimes slice timestamps are not perfect, there might be some delta
	if slice_timedelta > parse_timedelta("1"):
		slice_timedelta -= parse_timedelta("1")

	# Set buckets
	set_bucket_details(arg_hist)

	# Find index + 1 of last bucket to display:
	for b in range(all_buckets):
		if b % arg_every_nth == 0:
			max_bucket = b + 1
			if arg_num_buckets == 1:
				break
			else:
				arg_num_buckets = arg_num_buckets - 1

	# Open the log file:
	try:
		file_id = open(arg_log, "rb")
	except:
		print("log file " + arg_log + " not found.")
		sys.exit(-1)

	# By default reading one bucket dump for 10 second slice,
	# In case of multiple namespaces, it will read all bucket dumps for all namepspaces for same slice
	read_all_dumps = False

	# Set histogram tag:
	# hist_tag = HIST_TAG_PREFIX + arg_hist + " "
	if arg_ns:
		# Analysing latency for histogram arg_hist for specific namespace arg_ns
		# It needs to read single bucket dump for a slice
		hist_tags = [s%(arg_ns,arg_hist) for s in NS_HIST_TAG_PATTERNS]

	elif re.match(HIST_WITH_NS_PATTERN, arg_hist):
		# Analysing latency for specific histogram for specific namespace ({namespace}-histogram)
		# It needs to read single bucket dump for a slice
		hist_tags = [HIST_TAG_PREFIX + "%s "%(arg_hist)]

	else:
		# Analysing latency for histogram arg_hist
		# It needs to read all bucket dumps for a slice
		hist_tags = [s%(arg_hist) for s in HIST_TAG_PATTERNS]
		read_all_dumps = True

	# After this point we may need user input to stop:
	if arg_from == "tail":
		g_rolling = True
	if g_rolling:
		input_thread = threading.Thread(target = wait_for_user_input)
		# Note - apparently daemon threads just don't work in Python 2.4.
		# For Python versions where daemons work, set thread as non-daemon so
		# non-blocking input method can restore terminal echo when g_rolling is
		# set False via ctrl-c.
		input_thread.daemon = False
		input_thread.start()
		# Also wait for ctrl-c:
		signal.signal(signal.SIGINT, signal_handler)

	# Print first line of output table header to let user know we're live:
	if arg_ns:
		print("Histogram : {%s}-%s"%(arg_ns, arg_hist))
	else:
		print("Histogram Name : %s"%(arg_hist))
		print("Log       : " + str(arg_log))
	# Find datetime at which to start, and seek to starting point:
	head_dt = read_head_dt(file_id)
	if arg_from == "head":
		init_dt = head_dt
		file_id.seek(0, 0)
	else:
		tail_dt = read_tail_dt(file_id, arg_log)
		if arg_from == "tail":
			# Start from 5 min behind to get context
			init_dt = tail_dt - datetime.timedelta(minutes=5)
		else:
			init_dt = parse_init_dt(arg_from, tail_dt)
			if not init_dt:
				g_rolling = False
				sys.exit(-1)
			if init_dt < head_dt:
				init_dt = head_dt
		seek_back(init_dt, head_dt, tail_dt, file_id, arg_log)

		print("From      : " + str(init_dt))
		print("")

	relative_stat_path = []
	relative_stat_index = []
	if arg_relative_stats and arg_hist in relative_stat_info:
		info = relative_stat_info[arg_hist]
		relative_stat_path = info[0]
		relative_stat_index = info[1]

	# Find first histogram:
	old_total, old_values, old_dt, line, old_stat_values = \
		read_hist(hist_tags, init_dt, file_id, read_all_dumps=read_all_dumps, relative_stat_path=relative_stat_path)
	if not line:
		print("can't find histogram " + arg_hist + \
			" from start time " + arg_from)
		g_rolling = False
		sys.exit(-1)

	# Find datetime at which to stop, if any:
	if arg_duration != None:
		duration_td = parse_timedelta(arg_duration)
		if not duration_td:
			print("invalid duration " + arg_duration)
			g_rolling = False
			sys.exit(-1)
		end_dt = old_dt + duration_td

	# Other initialization before processing time slices:
	print_time_slice_header = True
	last_bucket_unit = ""
	new_dt = None
	which_slice = 0
	after_dt = old_dt + slice_timedelta
	overs, avg_overs, max_overs = \
		[0.0] * max_bucket, [0.0] * max_bucket, [0.0] * max_bucket
	total_ops, total_seconds = 0, 0
	max_rate = 0.0

	total_stat_values = [0.0] * len(old_stat_values)
	max_stat_values = [0.0] * len(old_stat_values)

	# Process all the time slices:
	while arg_duration == None or end_dt > old_dt:
		new_total, new_values, new_dt, line, new_stat_values = \
			read_hist(hist_tags, after_dt, file_id, line, read_all_dumps=read_all_dumps, relative_stat_path=relative_stat_path)
		
		# Wait to print header until after first read_hist which sets bucket_unit to usec if needed
		if print_time_slice_header:
			print_time_slice_header = False
			last_bucket_unit = bucket_unit

			# Print the output table header:
			labels_prefix = "slice-to (sec)"
			print(old_dt.strftime(DT_FMT))
			print(repeat(" ", len(labels_prefix)) + " %> (" + bucket_unit +")")

			labels = labels_prefix

			bucket_underline = ""
			for i in range(max_bucket):
				if i % arg_every_nth == 0:
					labels = labels + "%7s" % (pow(2, i))
					bucket_underline += " ------"

			labels = labels + "%11s" % "ops/sec"

			relative_stat_underline = ""
			if relative_stat_index:
				for idx_name in relative_stat_index:
					labels += "%11s" % idx_name[1]
					relative_stat_underline += " ----------"

			print(labels)

			underline = repeat("-", len(labels_prefix))
			underline += bucket_underline + " ----------" + relative_stat_underline

			print(underline)

		if last_bucket_unit != bucket_unit:
			break

		if not new_values:
			# This can happen in either eof or end of input time range
			break

		# Get the "deltas" for this slice:
		slice_total = new_total - old_total
		slice_values = subtract_buckets(new_values, old_values)
		slice_seconds_actual = elapsed_seconds(new_dt - old_dt)

		slice_stat_values = []
		slice_stat_rates = []
		if relative_stat_path:
			slice_stat_values = subtract_stat_values(new_stat_values, old_stat_values)
			slice_stat_rates = [v / slice_seconds_actual
								 for v in slice_stat_values]

		# Get the rate for this slice:
		rate = slice_total / slice_seconds_actual
		total_ops = total_ops + slice_total
		total_seconds = total_seconds + slice_seconds_actual
		if rate > max_rate:
			max_rate = rate

		if relative_stat_path:
			total_stat_values = add_stat_values(total_stat_values, slice_stat_values)
			max_stat_values = get_max_stat_values(max_stat_values, slice_stat_rates)

		# Convert bucket values for this slice to percentages:
		percentages = bucket_percentages(slice_total, slice_values)

		# For each (displayed) theshold, accumulate percentages over threshold:
		for i in range(max_bucket):
			if i % arg_every_nth:
				continue
			overs[i] = percentage_over(i, percentages)
			avg_overs[i] += overs[i]
			if overs[i] > max_overs[i]:  
				max_overs[i] = overs[i]

		# Print this slice's data:
		slice_to = new_dt.strftime(DT_TIME_FMT)
		print_line(slice_to, overs, max_bucket, arg_every_nth, rate, \
			slice_seconds_actual, slice_stat_rates, relative_stat_index)

		# Prepare for next slice:
		which_slice = which_slice + 1
		after_dt = new_dt + slice_timedelta
		old_total, old_values, old_dt = new_total, new_values, new_dt
		old_stat_values = new_stat_values

	# Print averages and maximums:
	total_slices = which_slice	
	if total_slices > 0:
		for i in range(max_bucket):
			if i % arg_every_nth == 0:
				avg_overs[i] = avg_overs[i] / total_slices
    
    	# The average for the duration of the analysis. This is different than rate_sum
		avg_rate = total_ops / total_seconds
		avg_stat_values = []
		if relative_stat_path:
			avg_stat_values = [v / total_seconds for v in total_stat_values]

		print(underline)
		print_line("avg", avg_overs, max_bucket, arg_every_nth, avg_rate, stat_values=avg_stat_values, stat_index=relative_stat_index)
		print_line("max", max_overs, max_bucket, arg_every_nth, max_rate, stat_values=max_stat_values, stat_index=relative_stat_index)

		if last_bucket_unit != bucket_unit:
			print('WARNING: asloglatency stopped early because latency units have changed from %s to %s.' % (last_bucket_unit, bucket_unit))
			print("Use 'asloglatency -h <histogram> -f '%s'' to bypass this problem." % new_dt.strftime(DT_FMT))
	else:
		print("could not find " + str(slice_timedelta) + " of data")

	# Should not need this, but daemon threads don't work in Python 2.4:
	# (Only needed when both -d and -r options are set.)
	g_rolling = False


#===========================================================
# Execution
#

try:
	opts, args = getopt.getopt(sys.argv[1:], "l:h:t:f:d:n:e:rN:VE", \
		["log=", "histogram=", "slice=", "from=", "duration=", \
		 "num-buckets=", "every-nth=", "rolling", "namespace=", "version", "help", "relative-stats",

		 # Deprecated
		 "num_buckets=", "every_nth=",
		 ])
except getopt.GetoptError as err:
	print(str(err))
	usage()
	sys.exit(-1)

# Default values for arguments:
arg_log = "/var/log/aerospike/aerospike.log"
arg_hist = None
arg_slice = "10"
arg_from = "tail"
arg_duration = None
arg_num_buckets = 3
arg_every_nth = 3
arg_ns = None
arg_relative_stats = False

# Set the arguments:
for o, a in opts:
	if o == "-V" or o == "--version":
		print_version()
		exit(0)
	if o == "-E" or o == "--help":
		usage()
		exit(0)
	if o == "-l" or o == "--log":
		arg_log = a
	if o == "-h" or o == "--histogram":
		arg_hist = a
	if o == "-t" or o == "--slice":
		arg_slice = a
	if o == "-f" or o == "--from":
		arg_from = a
	if o == "-d" or o == "--duration":
		arg_duration = a
	if o == "-n" or o == "--num-buckets" or o == "--num_buckets":
		arg_num_buckets = int(a)
	if o == "-e" or o == "--every-nth" or o == "--every_nth":
		arg_every_nth = int(a)
	if o == "-r" or o == "--rolling":
		g_rolling = True
	if o == "-N" or o == "--namespace":
		arg_ns = a
	if o == "--relative-stats":
		arg_relative_stats = True

# Call main():
main(arg_log, arg_hist, arg_slice, arg_from, arg_duration, \
	arg_num_buckets, arg_every_nth, arg_ns, arg_relative_stats)
