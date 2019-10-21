# README

## Build

Build a lightweight JAR:

    mvn package

Build a shaded (standalone, dependencies are included) JAR:

    mvn package shade:shade

## Run

### Run locally

Let's assume that `data/1.tfile` is a `TFile`-encoded logfile on our local filesystem (presumably downloaded from the HDFS filesystem).

Run the standalone JAR, extract contents under a directory named `container-logs` (any existing content will be overwritten):

    java -jar target/tfile-log-extractor-0.0.1-SNAPSHOT-standalone.jar data/1.tfile container-logs/

### Run on the cluster

Let's assume that YARN application `application_1571434978819_0004` has generated a `TFile`-encoded logfile at HDFS location `hdfs:///logs/user/logs/application_1571434978819_0004/datanode_01_33524`.

Run the lightweight JAR, extract contents under (local) directory `container-logs`:
    
    hadoop jar target/tfile-log-extractor-0.0.1-SNAPSHOT.jar \
        /logs/user/logs/application_1571434978819_0004/datanode_01_33524 \
        container-logs/

Or the same but directly calling java:

     java -cp "target/tfile-log-extractor-0.0.1-SNAPSHOT.jar:$(yarn classpath)" gr.helix.hadoop_utils.TFileLogExtractor \
        /logs/user/logs/application_1571434978819_0004/datanode_01_33524 \
        container-logs/


