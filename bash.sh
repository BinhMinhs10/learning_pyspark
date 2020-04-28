# add for spark
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVE_PYTHON="jupyter"
export PYSPARK_DRIVE_PYTHON_OPTS="notebook"
export PYTHONPATH="/usr/local/spark/python/:/usr/local/spark/python/lib/py4j-0.10.7-src.zip"
export SPARK_OPTS="--packages graphframes:graphframes:0.7.0-spark2.4-s_2.11"

# --driver-class-path /usr/local/spark/python/lib/graphframes-0.3.0-spark2.0-s_2.11.jar --jars "/usr/local/spark/python/lib/graphframes-0.3.0-spark2.0-s_2.11.jar"
PATH=$(echo $PATH | awk -v RS=: -v ORS=: '!($0 in a) {a[$0]; print}')


#source ~/.bash_profile