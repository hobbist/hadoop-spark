# hadoop-spark

pre-requistites:

install java,python on computer. set JAVA_HOME,PYTHON home pointing torespective installation directories.
check environmental variables properly set.
install maven and set M2_HOME to point to maven installation folder.

1)download sources for hadoop and spark from internet. 	

2)Maven build hadoop(2.7.2) tar file to generate windows excutable excatly follow https://www.youtube.com/watch?v=J61R-eVRmzc. For windows,winutils.exe is important file to be generated.confirm after successful build.

3)set hadoop home and path vairiable to bin folder of hadoop installation directory. set yarn home and path variable if yarn to be used as resource manager

4)maven build spark(2.2.0) binaries by downloading it from spark official website. 

5)set spark home and path vairiable to bin folder of hadoop installation directory.

6)check installation by running commands hdfs or spark-shell,pyspark. spark and hadoop installation is done for windows.

7)hadoop and spark will run in pseudo-distributed mode.master and slave will be on same machine.


steps for creating standalone spark cluster in multiple machines:-

1)connect laptops we want to connect as cluster.install hadoop and spark on each laptop as explained above. ping on each other ips to check connections(disable firewalls if necessary),once confirmed:	

2)run spark-class org.apache.spark.deploy.master.Master on master node.note down thhe spark master url :spark://<master-ip>:<master-port>

3)run spark-class org.apache.spark.deploy.worker.Worker spark://<master-ip>:<master-port> on worker node.

