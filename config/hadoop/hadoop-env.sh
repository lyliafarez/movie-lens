# Set Java Home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# (Optional but good) Set Hadoop Heap Size if needed
# export HADOOP_HEAPSIZE=1024

# Set Hadoop options (optional, for native libs / debug)
# export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Set Hadoop log directory
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs

# (Optional) Set user running Hadoop daemons
# export HADOOP_IDENT_STRING=$USER
