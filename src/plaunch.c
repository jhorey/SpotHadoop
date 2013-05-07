/**
 * Copyright 2013 Oak Ridge National Laboratory
 * Original Author: James Horey <horeyjl@ornl.gov>
 * Supplemental Author: Seung-Hwan Lim <lims1@ornl.gov> 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This small program is used to simplify launching and configuring
 * applications in an MPI environment. For example, it can be used to
 * launch a binary across a cluster, or to set environment variables
 * on multiple machines. 
 * 
 * It was developed for the spot-hadoop project, but may be useful for 
 * other projects as well. 
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <stdio.h>
#include <arpa/inet.h>
#include "mpi.h"

/**
 * Store command line arguments. 
 */
typedef struct cargs_t {
  char *name;
  char *value;
} cargs_t;

/**
 * Parse the command line arguments.
 *
 * @param argc Number of arguments
 * @param argv Array of arguments
 * @return List of parsed arguments
 */
cargs_t** parse_args(int argc, char *argv[])
{
  int i;
  int b;
  char buffer[2048];
  int num_args;
  cargs_t* *pargs;
  cargs_t* *iter;

  pargs = (cargs_t**)malloc(sizeof(cargs_t*) * 4);
  iter = pargs;
  num_args = 0;

  for(i = 0; i < argc; ) {
    if(argv[i][0] == '-' && argv[i][1] == '-') { 
      num_args++;
      *iter = (cargs_t*)malloc(sizeof(cargs_t));

      (*iter)->name = (char*)malloc(strlen(argv[i]) - 1); 
      strcpy((*iter)->name, &argv[i][2]); // Copy over the name. 
      (*iter)->value = NULL; // Initialize the value.

      // Now find the bounds of the actual argument. 
      char *bi = buffer; 
      for(b = i + 1; b < argc; ++b) {
	if(argv[b][0] == '-' && argv[b][1] == '-') { // Found next arg. 
	  break;
	}
	else {
	  // Add the value to our value buffer. 
	  strcpy(bi, argv[b]);
	  bi[strlen(argv[b])]= ' ';
	  bi += strlen(argv[b]) + 1; // Advance the pointer. 
	}
      }

      // Copy over the value buffer.
      if(bi != buffer) {
	*bi = '\0'; 
	(*iter)->value = (char*)malloc(bi - buffer + 1);
	strcpy((*iter)->value, buffer);
      }

      // Advance 'i'. 
      i = b;
      iter++;
    }
    else {
      // Not a valid argument.
      ++i;
    }
  }

  // Fill the rest with NULL values. 
  for(i = num_args; i < 4; ++i) {
    *iter = NULL;
    iter++;
  }

  return pargs;
}

/**
 * Indicates whether the argument exists and returns the value. 
 *
 * @param args List of all the arguments
 * @param arg Name of the argument we are searching for
 * @return The found argument. NULL is not found. 
 */
cargs_t* contains_arg(cargs_t* *args, char *arg)
{
  cargs_t *ai;

  for(ai = *args; ai != NULL; ) {
    if( strcmp(ai->name, arg) == 0 ) {
      return ai;
    }
    else {
      ai = *(++args);
    }
  }

  return NULL;
}

/**
 * Get the local host name. 
 * 
 * @param name Name of the machine
 * @param len Length of the machine name
 */
void get_local_name(char* name, size_t len)
{
  gethostname(name, len);
}

/**
 * Get all the network devices and IP addresses. 
 *
 * @param rank MPI rank of the machine
 * @param ips New list of all the IPs for the machine
 * @param labels New list of all the device names for the machine
 * @return Number of devices inserted into "ips" and "labels"
 */
int get_local_addresses(int rank, char** ips, char** labels)
{
  char buf[1024]; 
  struct ifconf ifc;
  struct ifreq *ifr;
  int sh;
  int numDev;
  int i;

  // Get socket handle.
  sh = socket(AF_INET, SOCK_DGRAM, 0);
  if(sh < 0) { return 1; }

  // Query available interfaces
  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = buf;
  if(ioctl(sh, SIOCGIFCONF, &ifc) < 0) { return 1; }

  // Iterate through interfaces.
  ifr = ifc.ifc_req;
  numDev = ifc.ifc_len / sizeof(struct ifreq);
  for(i = 0; i < numDev; ++i) {
    struct ifreq *item = &ifr[i];

    labels[i] = (char*)malloc(strlen(item->ifr_name) + 1);
    strcpy(labels[i], item->ifr_name);

    char *ip = inet_ntoa(((struct sockaddr_in *)&item->ifr_addr)->sin_addr);
    ips[i] = (char*)malloc(strlen(ip) + 1);
    strcpy(ips[i], ip);
  }

  // Now create a special "device" for the MPI rank. 
  labels[i] = (char*)malloc(strlen("mpi") + 1);
  strcpy(labels[i], "mpi");

  char rank_word[5];
  sprintf(rank_word, "%d", rank);
  ips[i] = (char*)malloc(strlen(rank_word) + 1);
  strcpy(ips[i], rank_word);

  return numDev + 1;
}

/**
 * Export the hostname
 * This function is depracated and not actually used at the moment.  
 *
 * @param host Name of the machine
 */
void export_host(char *host)
{
    setenv("HOSTNAME", host, 1);
}

/**
 * Export these variables to the environment. 
 * This function is depracated and not actually used at the moment.  
 *
 * @param ips The IP addresses
 * @param labels The device names
 * @param num The number of devices
 */
void export_local_addresses(char** ips, char** labels, int num)
{
  int i;

  for(i = 0; i < num; ++i) {
    // Allocate the export command and populate. 
    char name[32];
    sprintf(name, "IP_ADDRESS%s", labels[i]);

    // Export into the environment.
    setenv(name, ips[i], 1);
  }
}

/**
 * Helper function. This is where we will store all the host names.
 * 
 * @param nprocs Number of machines
 * @return Allocated buffer to store the host names. 
 */
char* create_global_hosts(int nprocs)
{
  // Each host has a maximum of 32 characters.
  return (char*)malloc(sizeof(char) * nprocs * 32);
}

/**
 * Collect all the host names from the MPI cluster. 
 *
 * @param nprocs Number of machines
 * @return Buffer of all the host names.
 */
char* collect_global_hosts(int nprocs)
{
  char* global_hosts = create_global_hosts(nprocs);

  // Get the local machine names. 
  char local_host[32];
  get_local_name(local_host, sizeof(local_host));

  // Use MPI Gather to get this information from all machines. 
  MPI_Gather(local_host, sizeof(local_host), MPI_CHAR,
	     global_hosts, sizeof(local_host), MPI_CHAR,
	     0, MPI_COMM_WORLD);

  return global_hosts;
}

/**
 * Out these "environment variables" in a file. It may be nicer
 * to use normal environment variables, but there's no nice way
 * to share those values across processes. 
 *
 * @param label The environment label
 * @param value The environment value
 * @param dir The directory to store the environment data
 */
void output_env(char *label, char *value, char *dir) 
{
    MPI_File f;
    char *file_name;
    char local_host[32];

    // Create a unique file name. 
    get_local_name(local_host, sizeof(local_host));
    file_name = (char*)malloc(strlen(dir) + strlen(local_host) + 2);
    sprintf(file_name, "%s/%s", dir, local_host);

    // Try opening the file.
    if(MPI_File_open(MPI_COMM_SELF, file_name,
    		     MPI_MODE_CREATE | MPI_MODE_RDWR | MPI_MODE_APPEND, 
		     MPI_INFO_NULL, &f) == MPI_SUCCESS) {
	char line[128];
    	MPI_Status ws;

	// We need to add the newline to each line. 
	sprintf(line, "%s=%s\n", label, value);
    	MPI_File_write(f, line, strlen(line), MPI_CHAR, &ws);
    }
    else {
      printf("failed opening %s\n", file_name);
    }

    MPI_File_close(&f); // Close the file
}

/**
 * Output all the host names into a single file. This is useful for 
 * discovery the list of all the machines available to the cluster.
 *
 * @param rank Rank of the current machine
 * @param nprocs Number of machines in the cluster. 
 * @param global_hosts Host names
 * @param file_name File to store the information
 */
void output_global_hosts(int rank, 
			 int nprocs,
			 char *global_hosts, 
			 char *file_name)
{
  if(rank == 0) { // Only a single output. 
    int i;
    MPI_File f;

    // Try opening the file.
    if(MPI_File_open(MPI_COMM_SELF, file_name,
    		     MPI_MODE_CREATE | MPI_MODE_WRONLY, 
		     MPI_INFO_NULL, &f) == MPI_SUCCESS) {

      printf("attempting write %s\n", file_name);
      for(i = 0; i < nprocs; ++i) {
	char line[32];
    	MPI_Status ws;

	// We need to add the newline to each line. 
	sprintf(line, "%s\n", global_hosts);
    	MPI_File_write(f, line, strlen(line), MPI_CHAR, &ws);

      	global_hosts += 32; // Increment to next string.
      }
    }
    else {
      printf("failed opening %s\n", file_name);
    }

    MPI_File_close(&f); // Close the file
    setenv("HOSTSFILE", file_name, 1); // Output to env. variable
  }
}

/**
 * Output specific machine IP information. This is used
 * to learn about all the devices/IPs belonging to a particular machine. 
 * Since this file is shared by all machines, it is an easy way
 * to learn about other machines. 
 *
 * @param name Name of the machine
 * @param ips IP addresses associated with the machine
 * @param labels Device names associated with the machine
 * @param num_dev Number of devices
 * @param dir Directory where the information is stored
 */
void output_local_hosts(char * name, 
			char* *ips,
			char* *labels,
			int num_dev, 
			char *dir)
{
    MPI_File f;
    char *file_name;
    char *host;

    // Create a unique file name. 
    file_name = (char*)malloc(strlen(dir) + strlen(name) + 2);
    sprintf(file_name, "%s/%s", dir, name);

    // Try opening the file.
    printf("attempting local write %s\n", file_name);
    if(MPI_File_open(MPI_COMM_SELF, file_name,
    		     MPI_MODE_CREATE | MPI_MODE_WRONLY, 
		     MPI_INFO_NULL, &f) == MPI_SUCCESS) {
      int i;
	char line[64];
    	MPI_Status ws;

	for(i = 0; i < num_dev; ++i) {
	  sprintf(line, "%s:%s\n", labels[i], ips[i]);
	  MPI_File_write(f, line, strlen(line), MPI_CHAR, &ws);
	}
    }
    else {
      printf("failed local write %s\n", file_name);
    }

    MPI_File_close(&f); // Close the file
    setenv("HOSTSDIR", dir, 1); // Output to env. variable
}

/**
 * Launch an external program.
 *
 * @param cmd The command to run
 */
void launch(char* cmd)
{
  char name[50];

  // Get the local host name. 
  get_local_name(name, sizeof(name));  

  // Create an output redirect file specific to this node. 
  char output[1024];
  sprintf(output, "%s > %s.out 2> %s.err", cmd, name, name);
  printf("%s cmd: %s\n", name, output);

  // Run the command. 
  system(output);
}

/**
 * Print a help message.
 **/
void print_help()
{
  printf("========================================================================\n");
  printf("|| plaunch - A parallel launcher for dynamic environments              ||\n");
  printf("========================================================================\n");
  printf("|| authors: James Horey and Seung-Hwan Lim                             ||\n");
  printf("========================================================================\n");
  printf("|| local  <DIR>- output local networking information in <DIR>          ||\n");
  printf("|| global <FILE> - collect global networking information to <FILE>     ||\n");
  printf("|| export <LABEL,NUM> - set LABEL environment variables for NUM nodes  ||\n");
  printf("|| cmd   <CMD> - start the <CMD>                                       ||\n");
  printf("========================================================================\n");
}

/**
 * Main function. 
 */
int main (int argc, char *argv[])
{
  char name[50];
  char* ips[15];
  char* labels[15];
  int num;
  int rank,nprocs,nid;
  MPI_Status status;
  cargs_t *arg;

  // Parse all the arguments. 
  cargs_t* *cargs = parse_args(argc, argv);

  // Initialize the MPI libs. 
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Does the user need help? 
  if( (arg = contains_arg(cargs, "help") ) != NULL ) {
    print_help();
  }

  // Do we need to generate local information?
  if( (arg = contains_arg(cargs, "local") ) != NULL ) {
    // Get local information.
    get_local_name(name, sizeof(name));
    export_host(name);
    num = get_local_addresses(rank, ips, labels);

    // Output local information. 
    export_local_addresses(ips, labels, num);

    // Where to put the information? 
    char *dir = "/tmp/network";
    if(arg->value != NULL) {
      dir = strtok(arg->value, " ");
    }

    output_local_hosts(name, ips, labels, num, dir);
  }

  arg = NULL;
  if( (arg = contains_arg(cargs, "global") ) != NULL ) {
    // Do we need to generate global information? 
    char* hosts = collect_global_hosts(nprocs);

    // Where to put the information? 
    char *dir = "/tmp/hosts.txt";
    if(arg->value != NULL) {      
      dir = strtok(arg->value, " ");
    }

    output_global_hosts(rank, nprocs,
			hosts, dir);
  }

  // Do we need to export some special variables?
  // (Used to indicate leaders, etc.)
  arg = NULL;
  if( (arg = contains_arg(cargs, "export") ) != NULL ) {
    char *label;
    char *to_rank;

    // Fetch the directory where we store the environment vars. 
    cargs_t *dir_arg = NULL;
    char *export_dir = NULL;
    if( (dir_arg = contains_arg(cargs, "dir") ) != NULL ) {
      export_dir = strtok(dir_arg->value, " ");
    }
    else {
      export_dir = "/tmp/environment";
    }

    // First value is the label to export.
    // Second value is the rank of the node.  
    label = strtok(arg->value, " ");
    to_rank = strtok(NULL, " ");

    // Output the value. 
    output_env(label, to_rank, export_dir);
  }

  // Is there a command? 
  arg = NULL;
  if( (arg = contains_arg(cargs, "cmd") ) != NULL ) {
    launch(arg->value);
  }

  // Finish MPI
  MPI_Finalize();

  return 0;
}
