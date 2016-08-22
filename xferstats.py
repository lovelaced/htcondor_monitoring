import re
import os
import sys
import time
import pickle
import binascii
import socket
import struct
from subprocess import Popen, PIPE

# Reads from an HTCondor XferStatsLog and sends pickled data to a carbon server.
# An example log entry looks like the following:
#
# 06/09/16 14:53:10 File Transfer Download: JobId: 626.19 files: 7 \\
# bytes: 391021382 seconds: 1.0 dest: 128.105.14.141 rto: 203000 ato: 40000 snd_mss: 33408 \\
# rcv_mss: 65468 unacked: 1 sacked: 0 lost: 0 retrans: 0 fackets: 0 pmtu: 65520 \\
# rcv_ssthresh: 1964430 rtt: 3000 snd_ssthresh: 2147483647 snd_cwnd: 10 \\
# advmss: 65483 reordering: 3 rcv_rtt: 1000 rcv_space: 4451824 total_retrans: 0

LOGDIR = Popen(['condor_config_val', 'log'], stdout=PIPE).communicate()[0].rstrip()
LOGFILE = LOGDIR + '/XferStatsLog'
TMPFILE = "/tmp/xferstats_hosts_last_byte_read"
SCHEMA = "pools.chtc.jobs.xferstats"
HOSTNAME = re.sub('[^0-9a-zA-Z]+', '_', socket.gethostname())
CARBON_SERVER = 'monitor0.chtc.wisc.edu'
CARBON_PICKLE_PORT = 2004
DELAY = 60

# Validate input file
if not os.path.isfile(LOGFILE):
    print "The file '%s' does not exist." % LOGFILE
    sys.exit(1)

# Load IP/CIDR-to-Location dictionaries
d = pickle.load(open('osg_ip_dicts.pkl', 'rb'))
ip2site = d['ip2site']
cidr2site = d['cidr2site']

# functions for checking if IP is in CIDR
# http://diego.assencio.com/?index=85e407d6c771ba2bc5f02b17714241e2

def ip_in_subnetwork(ip_address, subnetwork):
 
    """
    Returns True if the given IP address belongs to the
    subnetwork expressed in CIDR notation, otherwise False.
    Both parameters are strings.
 
    Both IPv4 addresses/subnetworks (e.g. "192.168.1.1"
    and "192.168.1.0/24") and IPv6 addresses/subnetworks (e.g.
    "2a02:a448:ddb0::" and "2a02:a448:ddb0::/44") are accepted.
    """
 
    (ip_integer, version1) = ip_to_integer(ip_address)
    (ip_lower, ip_upper, version2) = subnetwork_to_ip_range(subnetwork)
 
    if version1 != version2:
        raise ValueError("incompatible IP versions")
 
    return (ip_lower <= ip_integer <= ip_upper)
 
 
def ip_to_integer(ip_address):
 
    """
    Converts an IP address expressed as a string to its
    representation as an integer value and returns a tuple
    (ip_integer, version), with version being the IP version
    (either 4 or 6).
 
    Both IPv4 addresses (e.g. "192.168.1.1") and IPv6 addresses
    (e.g. "2a02:a448:ddb0::") are accepted.
    """
 
    # try parsing the IP address first as IPv4, then as IPv6
    for version in (socket.AF_INET, socket.AF_INET6):
 
        try:
            ip_hex = socket.inet_pton(version, ip_address)
            ip_integer = int(binascii.hexlify(ip_hex), 16)
 
            return (ip_integer, 4 if version == socket.AF_INET else 6)
        except:
            pass
 
    raise ValueError("invalid IP address")
 
 
def subnetwork_to_ip_range(subnetwork):
 
    """
    Returns a tuple (ip_lower, ip_upper, version) containing the
    integer values of the lower and upper IP addresses respectively
    in a subnetwork expressed in CIDR notation (as a string), with
    version being the subnetwork IP version (either 4 or 6).
 
    Both IPv4 subnetworks (e.g. "192.168.1.0/24") and IPv6
    subnetworks (e.g. "2a02:a448:ddb0::/44") are accepted.
    """
 
    try:
        fragments = subnetwork.split('/')
        network_prefix = fragments[0]
        netmask_len = int(fragments[1])
 
        # try parsing the subnetwork first as IPv4, then as IPv6
        for version in (socket.AF_INET, socket.AF_INET6):
 
            ip_len = 32 if version == socket.AF_INET else 128
 
            try:
                suffix_mask = (1 << (ip_len - netmask_len)) - 1
                netmask = ((1 << ip_len) - 1) - suffix_mask
                ip_hex = socket.inet_pton(version, network_prefix)
                ip_lower = int(binascii.hexlify(ip_hex), 16) & netmask
                ip_upper = ip_lower + suffix_mask
 
                return (ip_lower,
                        ip_upper,
                        4 if version == socket.AF_INET else 6)
            except:
                pass
    except:
        pass
 
    raise ValueError("invalid subnetwork")

def run(sock, delay):

    # Open LOGFILE where reading last left off
    curr_byte = 0
    if os.path.isfile(TMPFILE):
        print 'Reading LOGFILE from last run'
        with open(TMPFILE) as tmpfile:
            curr_byte = int(tmpfile.read())
    else:
        print 'Reading LOGFILE from beginning'

    logfile = open(LOGFILE)

    # Initialize list of tuples for sending to Carbon
    tuples = []

    # Read LOGFILE every DELAY seconds
    while True:

        # Get the inode of the current LOGFILE
        curr_inode = os.fstat(logfile.fileno()).st_ino

        # Check to see if LOGFILE has rotated
        try:
            if os.stat(LOGFILE).st_ino != curr_inode:
                newlog = open(LOGFILE, "r")
                logfile.close()
                logfile = newlog
                curr_byte = 0
                continue
        except IOError:
            pass

        # Fast-forward to last read byte in LOGFILE
        logfile.seek(curr_byte, 0)

        # Loop over all lines in LOGFILE
        for line in logfile:

            # Check for validity
            if len(line.split()) is not 55:
                continue

            # Get the UNIX timestamp
            timestamp = " ".join(line.split()[:2])
            pattern = '%m/%d/%y %H:%M:%S'
            epoch = int(time.mktime(time.strptime(timestamp, pattern)))

            # Get the transfer type (Upload/Download)
            # and rename due to FW's request...
            xfer_type = line.split()[6][:-1]
            if xfer_type == "Download":
                xfer_type = "OutputFiles"
            else:
                xfer_type = "InputFiles"

            # Format the rest of the line for easy dict creation
            logline = " ".join(line.split()[7:])
            entry = logline.replace(": ", "=")
            metrics = dict(item.split("=") for item in entry.split())

            # Determine the pool location
            ip = metrics["dest"]
            pool_site = 'Unknown'
            if ip in ip2site:
                pool_site = ip2site[ip]
            else:
                for cidr in cidr2site:
                    if ip_in_subnetwork(ip, cidr):
                        pool_site = cidr2site[cidr]
                        break
                if pool_site == 'Unknown':
                    print 'No known site for %s' % (ip)

            for key in metrics.keys():

                # Set the schema, e.g.:
                #   pools.chtc.jobs.xferstats.submit-3_chtc_wisc_edu.\
                #   University_of_Wisconsin-Madison.Download.attr
                if (key != "JobId") and (key != "dest"):
                    message = ".".join([SCHEMA, HOSTNAME, pool_site, xfer_type, key])
                    tuples.append((message, (epoch, metrics[key])))

            # Only push ~5,000 entries at a time
            if len(tuples) >= 5000:

                # Pickle entries
                package = pickle.dumps(tuples, protocol=2)
                header = struct.pack('!L', len(package))
                message = header + package

                print 'sending %s metrics (%.1f KB, last timestamp %s)' % \
                    (len(tuples), sys.getsizeof(message)/1024., timestamp)

                # Push to carbon
                try:
                    sock.sendall(message)
                except socket.error:
                    print socket.error
                    sock.close()
                    sock = socket.socket()
                    sock.connect((CARBON_SERVER, CARBON_PICKLE_PORT))
                else: # only clear data and store LOGFILE location if successful
                    tuples = []
                    curr_byte = logfile.tell()
                    with open(TMPFILE, 'w') as tmpfile:
                        tmpfile.write(str(curr_byte))
                
                # Wait 5 seconds to push to carbon again
                time.sleep(5)

        # If at the end of the file, push everything we have
        if len(tuples) > 0:

            # Pickle entries
            package = pickle.dumps(tuples, protocol=2)
            header = struct.pack('!L', len(package))
            message = header + package

            print 'sending %s metrics (%.1f KB, last timestamp %s)' % \
                (len(tuples), sys.getsizeof(message)/1024., timestamp)

            # Push to carbon
            try:
                sock.sendall(message)
            except socket.error:
                print socket.error
                sock.close()
                sock = socket.socket()
                sock.connect((CARBON_SERVER, CARBON_PICKLE_PORT))
            else: # only clear data and store LOGFILE location if successful
                tuples = []
                curr_byte = logfile.tell()
                with open(TMPFILE, 'w') as tmpfile:
                    tmpfile.write(str(curr_byte))
                
        # Wait to check LOGFILE again
        time.sleep(delay)

def main():

    # Get LOGFILE check delay from command-line if provided
    delay = DELAY
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)

    # Open connection to carbon
    sock = socket.socket()
    try:
        sock.connect((CARBON_SERVER, CARBON_PICKLE_PORT))
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" % {'server': CARBON_SERVER, 'port': CARBON_PICKLE_PORT})

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sock.close() # Close socket nicely
        sys.exit(0)

if __name__ == "__main__":
    main()

