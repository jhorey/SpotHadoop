# Spot-Hadoop
Spot-Hadoop is a tool to dynamically configure, launch, and benchmark Hadoop on shared, parallel filesystems (namely Lustre). 
The primary motivation behind this project is get a dynamic Hadoop tool (in a manner similar to Amazon's EMR service) running
on Oak Ridge National Lab's infrastructure. As ORNL runs primarily large-scale MPI clusters with a shared Lustre filesystem, it
is unclear exactly the best way to actually accomplish this. This project attempts to explore both the software architecture and
performance issues related to getting Hadoop running on this environment. 

# Design and Usage
There are three scripts that constitute our service. 

* spot-hadoop: This is the front-facing script that users should interact with
* hadooprun.py: This is the script that does the heavy lifting
* plaunch.c: A helper utility to interact with the MPI cluster

These scripts make many assumptions about the underlying environment. For example, the scripts currently assume the use of PBS/QSUB for
dynamic machine allocation. To use the scripts, the user would typically do:

~~~
spot-hadoop reserve      # This allocates the machines 
spot-hadoop environment  # This configures the environment
spot-hadoop hadoop       # This launches Hadoop
spot-hadoop benchmark    # This runs some basic benchmarks
~~~

For further detail, it is worth checking out the spot-hadoop script. 

# Current Status
Spot-Hadoop is a research tool. While the scripts work for us, they were not designed to work anywhere
else. That being said, other HPC-type environments should be able to use these scripts with only slight
modifications. It is my hope that this tool evolves to be more general-purpose. In the meantime, if you
are interested in using this and assistance is required, feel free to contact me.

# Copyright and License
This code was developed as part of an internal research project Oak Ridge National Lab. Consequently
the copyright belongs to Oak Ridge National Laboratory. The code is released under an 
Apache 2.0 license.