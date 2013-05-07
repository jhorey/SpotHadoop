#
# Copyright 2013 Oak Ridge National Laboratory
# Original Author: James Horey <horeyjl@ornl.gov>
# Supplemental Author: Seung-Hwan Lim <lims1@ornl.gov> 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 
# This script is used to:
# 1) Dynamically generate Hadoop configuration files 
# 2) Launch the Hadoop processes
# 3) Instantiate user job flows
#
# We make many assumptions in this script. For example, that everything is
# accessed via shared storage, and that networking information is provided
# by an external service (called "plaunch"). 
#

from string import Template
import math
import socket
import sys
import os
from subprocess import *
import getopt
import argparse
import time
import shutil
from time import localtime, strftime

# This is where we store the Hadoop environment variables. 
henv = {}

#
# Populate the environment using data from an "environment"
# file. This is done using a file instead of relying on the actual shell environment
# since all the machines share access to the file. 
#
def read_env(dir_name):
    file_name = dir_name + "/" + get_hostname()
    try:
        f = open(file_name, "r")
        for line in f:
            v = line.split("=")
            henv[v[0]] = v[1].rstrip()

        # Check if the important variables are there. 
        if henv['SPOT_HADOOP'] == None:
            print 'Please supply a SPOT_HADOOP value in the environment'
            return None

        # Get information about the networking configuration and
        # Hadoop binaries. We should probably not hard-code the Hadoop version :)
        henv['CONF_DIR'] = henv['SPOT_HADOOP'] + '/hadoop/conf'
        henv['NETWORK_DIR'] = henv['SPOT_HADOOP'] + '/data/network'
        henv['HADOOP_HOME'] = henv['SPOT_HADOOP'] + '/hadoop/hadoop-1.1.1/bin'
        henv['LOGNAME'] = os.environ['LOGNAME']

        # We need to know which machine is going to be the JobTracker.
        # Since Hadoop can't run without one, spit out a warning and quit. 
        if henv['JOBTRACKER'] == None:
            print 'Please supply a value for JOBTRACKER in the environment'
            return None
        else:
            # Get the MPI rank and hostname of the JobTracker machine. 
            r = int(henv['JOBTRACKER'])
            hostname = get_hostname_from_rank(r) 

            # Sometimes there is no rank-0 mpi process! Not sure why,
            # should check into this. 
            while hostname == None:
                r += 1
                hostname = get_hostname_from_rank(r)
                print "JOBTRACKER", hostname
            print "JOBTRACKER", hostname
            henv['JOBTRACKER']=hostname
    except TypeError:
        print 'Please provide an environment file'
    except IOError:
        print 'Not a valid environment file:' + file_name

#    
# Get this node's hostname. 
#
def get_hostname():
    return socket.gethostname().rstrip()

#
# Find the machine hostname associated with an MPI rank. 
#
def get_hostname_from_rank(rank):
    try:
        in_file = open(henv['NETWORK_DIR'] + '/hosts', 'r')
        for line in in_file:
            line = line.rstrip()
            r = get_ip(line, 'mpi')
            if rank == int(r):
                return line
        return None
    except IOError:
        print 'could not get hostname from rank', rank

#
# Get the IP address associated with a hostname on a part
# device (eth0, ib0, etc.). 
#
def get_ip(hostname, dev):
    # Find the networking file. The file consists of records like
    # eth0:<ip>
    # ib0:<ip>
    try:
        f = open(henv['NETWORK_DIR'] + '/' + hostname, 'r')
        for line in f:
            v = line.split(':')
            if v[0] == dev:
                return str(v[1].rstrip())
        return None
    except IOError:
        print 'could not get ip address for ' + hostname

#
# Helper method to apply a set of changes to a pre-existing
# configuration template file. The output is a new configuration
# file with the changes applied. 
#
def configure_file(input_file, output_file, changes):
    try:
        in_file = open(input_file, 'r')
        out_file = open(output_file, 'w+')

        for line in in_file:
            s = line
            if changes != None:
                s = Template(line).substitute(changes)

            out_file.write(s)

        in_file.close()
        out_file.close()
    except IOError:
        print 'could not configure file'

#
# Create the "hadoop_env.sh" file
#
def create_hadoop_environment():
    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    env_template = henv['CONF_DIR'] + '/templates/hadoop-env.sh'
    output_file = directory + '/hadoop-env.sh'
    
    configure_file(env_template, output_file, None)
    env_template = henv['CONF_DIR'] + '/templates/hadoop-metrics2.properties'
    output_file = directory + '/hadoop-metrics2.properties'
    configure_file(env_template, output_file, None)

#
# Create the masters file.
#
def create_masters():
    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Every single node will produce the same masters file.
    out_file = open(directory + '/masters', 'w+')

    print "create_masters:",henv['JOBTRACKER']
    out_file.write(get_ip(henv['JOBTRACKER'],henv['DEV'])+ '\n') # Use the IP address!
    out_file.close()

#
# Create the slaves file. 
#
def create_slaves():
    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Every single node will produce the same slaves file.
    # Use the 'plaunch' network hosts file to get list of machines. 
    in_file = open(henv['NETWORK_DIR'] + '/hosts', 'r')
    out_file = open(directory + '/slaves', 'w+')

    # Record every machine's IP address except the master.
    slaves=0
    for line in in_file:
        line = line.rstrip()
        if line != henv['JOBTRACKER'] and line != henv['NAMENODE']:
            out_file.write(get_ip(line,henv['DEV']) + '\n')
	    slaves += 1
            
    henv['SLAVES']=slaves
    out_file.close()

#
# Configure the core-site file. 
#
def configure_core_site():
    # Batch up all the changes we will make to the configuration file.
    changes = {}

    # Where is the mapred_site.xml file located? 
    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    core_template = henv['CONF_DIR'] + '/templates/core-site.xml'
    output_file = directory + '/core-site.xml'

    # Make jobtracker and local map directory changes.
    changes['node'] = get_hostname()
    changes['user'] = henv['LOGNAME'].rstrip()
    changes['namenode'] = get_ip(henv['NAMENODE'], henv['DEV'])
    configure_file(core_template, output_file, changes)

#
# Configure the mapred site config.
#
def configure_mapred_site():
    changes = {}

    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    mapred_template = henv['CONF_DIR'] + '/templates/mapred-site.xml'
    output_file = directory + '/mapred-site.xml'

    # Configure the various Hadoop parameters. This can be modified
    # to suit a particular environment. Ideally, these models would be
    # more rigorously defined. 
    num_procs = int(henv['PROCS'])
    map_waves = 1.2
    reduce_waves = 1.75
    concurrent_map_tasks = int(math.ceil(num_procs/2))
    concurrent_reduce_tasks =int(math.ceil(num_procs / 3))
    total_map_tasks = int(math.ceil(map_waves * concurrent_map_tasks * int(henv['SLAVES'])))
    total_reduce_tasks = int(math.ceil(reduce_waves * concurrent_reduce_tasks * int(henv['SLAVES'])))
    parallel_copies = int(num_procs / concurrent_reduce_tasks)
    sortfactor=int(math.ceil(int(total_map_tasks)*1.2))
    copies=int(henv['SLAVES'])
    max_used_memory = 0.50 # Only use fifty percent of available memory
    
    # Make jobtracker and local map directory changes.
    changes['jobtracker'] = get_ip(henv['JOBTRACKER'], henv['DEV'])
    changes['mapnode'] = get_hostname()
    changes['user'] = henv['LOGNAME'].rstrip()
    changes['addr'] = get_ip(get_hostname(),henv['DEV'])
    changes['reducer'] = str(concurrent_reduce_tasks)
    changes['mapper'] = str(concurrent_map_tasks)
    changes['maxmapper'] = str(total_map_tasks)
    changes['maxreduce'] = str(total_reduce_tasks)
    changes['sortfactor'] = str(sortfactor)
    changes['copies'] = str(copies)

    # Make the actual changes. 
    configure_file(mapred_template, output_file, changes)

#
# Configure the hdfs site config.
# Note that this isn't actually used for our current experiments. We only include this
# for posterity. 
#
def configure_hdfs_site():
    changes = {}

    directory = henv['CONF_DIR'] + '/' + get_hostname()
    if not os.path.exists(directory):
        os.makedirs(directory)

    hdfs_template = henv['CONF_DIR'] + '/templates/hdfs-site.xml'
    output_file = directory + '/hdfs-site.xml'

    # Change the HDFS data directory to be node specific. 
    changes['node'] = get_hostname()
    changes['user'] = henv['LOGNAME'].rstrip()
    changes['addr'] = get_ip(get_hostname(),henv['DEV'])
    configure_file(hdfs_template, output_file, changes)

#
# Start a Hadoop daemon
#
def start_hadoop_daemon(daemon, conf):
    # Actual command to execute. 
    cmd = "hadoop-daemon.sh --config " + conf + " start " + daemon
    print cmd

    # Get the environment variabes.
    e = os.environ.copy()

    # Find the Java home. This is a heuristic, different nodes
    # apparently have Java installed in different places. 
    e['JAVA_HOME'] = '/usr/lib/jvm/java-1.6.0-sun'

    # Execute the command. 
    procs = [Popen(cmd.split(), env=e)]

#
# Format HDFS. This must be called before any of the daemons are started!
# Note that we do not actually use, but include it only for posterity. 
#
def format_hdfs(conf):
    hostname = get_hostname()
    if hostname == henv['NAMENODE']:
        # First remove the old name node directory, just in case
        # there was one. Otherwise the NameNode will prompt us. 
        # This is hard-coded for now (replace later). 
        try:
            # name_dir = "/lustre/widow1/scratch/s52c3/med002/"+hostname+"/name"
            name_dir = "/lustre/widow1/scratch/" + henv["LOGNAME"] + "/med002/"+hostname+"/name"
            shutil.rmtree(name_dir)
        except:
        	# Ignore all errors.
        	print "formatting new name node"

        # Now ask namenode to format. 
        cmd = "hadoop --config " + conf + " namenode -format"
	print cmd

	check_call(cmd.split());
    	print "HDFS formatted." + strftime("%Y-%m-%d %H:%M:%S", localtime())

#
# Start the job tracker. 
#
def start_job_tracker():
    hostname = get_hostname()
    if hostname == henv['JOBTRACKER']:
        start_hadoop_daemon("jobtracker", henv['CONF_DIR'] + '/' + hostname)

#
# Start the task tracker process. 
#
def start_task_trackers():
    hostname = get_hostname()
    if hostname != henv['NAMENODE'] and hostname != henv['JOBTRACKER']:
    	start_hadoop_daemon("tasktracker", henv['CONF_DIR'] + '/' + hostname)

#
# Start the name node.
#
def start_name_node():
    hostname = get_hostname()
    if hostname == henv['NAMENODE']:
        start_hadoop_daemon("namenode", henv['CONF_DIR'] + '/' + hostname)

#
# Start the data nodes
#
def start_data_nodes():
    hostname = get_hostname()
    if hostname != henv['NAMENODE'] and hostname != henv['JOBTRACKER']:
        start_hadoop_daemon("datanode", henv['CONF_DIR'] + '/' + hostname)

#
# Use JPS to confirm everyting is working
#
def confirm_servers():
    check_call("jps")

#
# Check the status of the filesystem. 
#
def check_status():
    conf=henv['CONF_DIR']+ '/' + get_hostname()
    cmd = "hadoop --config " + conf + " dfsadmin -report "
    check_call(cmd.split())

#
# Main function
#
if __name__ == '__main__':

    # Parse the command-line arguments. 
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env')  # Environment file
    args = parser.parse_args()
    vargs = vars(args)

    # Read the environment file.
    read_env(vargs['env'])

    # Create all the configuration files. 
    create_hadoop_environment()
    create_masters()
    create_slaves()
    configure_core_site()
    configure_mapred_site()

    
    # Start the daemons. Introduce some artificial
    # waiting times to give the daemons some time to start. 
    time.sleep(1)
    start_job_tracker()
    time.sleep(1)
    start_task_trackers()

