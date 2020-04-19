
##############################################################################################

# Part 1: Setting up Spark
# As a part of Mini Project 1, we had set-up Hadoop on the VMs. 
# Checked whether Hadoop cluster is running (by typing 'jps' command in each of the VMs.

# Spark Set-up in CC-MON-25 (Master)

# Download the Spark package
wget https://mirror.olnevhost.net/pub/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

# Unpack
tar zxvf spark -2.4.5-bin-hadoop2.7.tgz
ln -s spark-2.4.5-bin-hadoop2.7 spark

# Install Scala
sudo apt install scala

# Try Spark Shell (local mode)
bin/spark-shell --master local[2]

# Sample example
val NUM_SAMPLES = 1000val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>	val x = math.random	val y = math.random	x*x + y*y < 1       }.count()println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")

# Repeat the same installation process with the slaves: CC-MON-26 and CC-MON-27


# Run Spark Shell with YARN
# Check HDFS and YARN services and environment configuration
jps

# Add "Add “export HADOOP_CONF_DIR=/home/student/hadoop/etc/hadoop” to the ~/.bashrc
# source ~/.bashrc
echo $HADOOP_CONF_DIR 

bin/spark-shell --master yarn --deploy-mode client
# Execute the sample example

val NUM_SAMPLES = 1000000val count = sc.parallelize (1 to NUM_SAMPLES).filter { _ =>	val x = math.random	val y = math.random	x*x + y*y < 1      }.count()println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")






##############################################################################################

# Part 2: Developing Spark Programs

# 1. Put user_artists.dat file into hdfs

hdfs dfs put ~/spark/user_artists.dat

# 2. Install Maven

sudo apt get install maven

# 3. Make Jar file

cd workspace/ListeningCount/

mvn package

# 4. Find the Jar File

find .

# 5. Run the Jar

/spark/bin/spark-submit --class "ListeningCount" --master local --driver-memory 1g --executor-memory 1g --executor-cores 1 --queue default target/ListeningCount*.jar


##############################################################################################
# Part 3: Developing Spark Programs 2

# 1. Test the local program (provided in “LinearRegressionExample.ipynb”) on your own laptop by running a jupyter notebook.

# 2. Install Python and run PySpark

hdfs dfs put ~/spark/README.md

cd spark
sudo apt install python
./bin/pyspark

textFile = spark.read.text("README.md")
textFile.count()

textFile.first()
linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
textFile.filter(textFile.value.contains("Spark")).count()

# 3. Run SimpleApp 

./bin/spark-submit --master local[4] SimpleApp.py

# 4. Run LinearRegressionExample

./bin/spark-submit --master local[4] LinearRegressionExample.py

# 5. Check Saved weights

../hadoop/bin/hdfs dfs -ls


