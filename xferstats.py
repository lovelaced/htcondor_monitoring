import os
import sys
import time
import socket
import pickle
import struct

# Reads from an HTCondor XferStatsLog and sends pickled data to a graphite server.
# An example log entry looks like the following:
#
# 06/09/16 14:53:10 File Transfer Download: JobId: 626.19 files: 7 \\
# bytes: 391021382 seconds: 1.0 dest: 128.105.14.141 rto: 203000 ato: 40000 snd_mss: 33408 \\
# rcv_mss: 65468 unacked: 1 sacked: 0 lost: 0 retrans: 0 fackets: 0 pmtu: 65520 \\
# rcv_ssthresh: 1964430 rtt: 3000 snd_ssthresh: 2147483647 snd_cwnd: 10 \\
# advmss: 65483 reordering: 3 rcv_rtt: 1000 rcv_space: 4451824 total_retrans: 0

LOGFILE = "/path/to/XferStatsLog"
TMPFILE = "/tmp/xferstats_byte"
SCHEMA = "your.graphite.schema."
CARBON_SERVER = '127.0.0.1'
CARBON_PICKLE_PORT = 2004
DELAY = 60

last_timestamp = 0
jobs = {}

# Validate input file
if not os.path.isfile(LOGFILE):
    print "The file '%s' does not exist." % LOGFILE
    sys.exit(1)


def run(sock, delay):
    curr_byte = 0
    if os.path.isfile(TMPFILE):
        tmpfile = open(TMPFILE, 'r')
        curr_byte = int(tmpfile.read())
        tmpfile.close()
    logfile = open(LOGFILE)

    while True:
        curr_inode = os.fstat(logfile.fileno()).st_ino
        # check to see the log hasn't rotated, get the most recent file
        try:
            if os.stat(LOGFILE).st_ino != curr_inode:
                newlog = open(LOGFILE, "r")
                logfile.close()
                logfile = newlog
                curr_byte = 0
                continue
        except IOError:
            pass

        # start reading the file where we left off
        logfile.seek(curr_byte, 0)
        tuples = ([])

        # check to make sure the line is valid
        for line in logfile:
            if len(line.split()) is not 55:
                continue

            timestamp = " ".join(line.split()[:2])
            pattern = '%m/%d/%y %H:%M:%S'
            epoch = int(time.mktime(time.strptime(timestamp, pattern)))

            xfer_type = line.split()[6][:-1]
            logline = " ".join(line.split()[7:])
            # format the line for easy dict creation
            entry = logline.replace(": ", "=")
            metrics = dict(item.split("=") for item in entry.split())

            for key in metrics.keys():
                # format our data:
                # pools.chtc.jobs.xferstats.download.attr
                # pools.chtc.jobs.xferstats.upload.attr
                if key != "JobId":
                    message = SCHEMA + xfer_type + "." + key
                    tuples.append((message, (epoch, metrics[key])))

            #  pickle our data and send it
            package = pickle.dumps(tuples, 1)
            size = struct.pack('!L', len(package))
            # make sure we don't get a broken pipe
            try:
                sock.sendall(size)
                sock.sendall(package)
            except socket.error:
                sock.close()
                sock = socket.socket()
                sock.connect((CARBON_SERVER, CARBON_PICKLE_PORT))

        curr_byte = logfile.tell()
        tmpfile = open("/tmp/xferstats_byte", 'w')
        tmpfile.write(str(curr_byte))
        tmpfile.close()
        logfile.close()
        time.sleep(delay)


def main():
    """Wrap it all up together"""
    delay = DELAY
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)

    sock = socket.socket()
    try:
        sock.connect((CARBON_SERVER, CARBON_PICKLE_PORT))
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" %
                         {'server': CARBON_SERVER, 'port': CARBON_PICKLE_PORT})

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()

