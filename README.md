# hadoop_samples

Sample Java client for HDFS and Hive.

## Geting Started

### 1 Clone code from github

    git clone git@github.com:baikai/hadoop_samples.git

### 2 Replace hdfs/hive connection properties with your value

     vim src/main/java/com/asiainfo/bdx/ocdp/HadoopSamplesTest.java

### 3. Build by maven

     mvn clean package

### 4. Run

     java -jar target/hadoopSamples-1.0-SNAPSHOT-jar-with-dependencies.jar