# ETL Light

A light and effective ETL job based on Apache Spark, tailored for use-cases where the source is Kafka and the sink is either HDFS or Amazon S3.

**Features:**

* **HDFS/S3 Sink:** write can be done to both HDFS and Amazon S3 by using different URL schemes ('hdfs://', 's3://') in relevant configuration entries.

* **Configurable and Customizable:** configurable through application.conf file (see example at: core/main/resources), both transformer and writer classes can be replaced through 'pipeline' configuration.

* **Scalable:** by leveraging Spark framework, scalability is achieved by default. Using Kafka RDD source enables maximum efficiency having as many executors as the number of topics partitions (can be configured for less). 

* **Easy Upgrades:** as this ETL runs as a Spark job either on demand or more typically using some external job-scheduler (e.g. oozie), upgrading it means stopping current job-scheduling, deploying the new artifacts (jar, config) and restarting job-scheduling (no management of long running processes lifecycle running in multiple machines).

* **Transparent:** all consumed Kafka topics/partitions offsets within a single executed job are being committed (saved) into a single state file (in Json format) as the last step of the job execution, apart from using this state as a starting point for the next job that runs, it also provides an easy way to view the history of recent job executions (number of recent files to maintain is configurable), their time and topics/partitions/offsets.

* **Exactly Once Delivery:** this relates to the process of moving events between Kafka to target storage, since each job run starts from the last successful saved state and critical failures exits the job without committing a new state - any job running after that will try to re-consume all events since last successful run until success. duplicates are avoided by creating a unique file names containing the job id, any re-run of a job will use the same job-id and will override existing files.

* **Error Handling:** each consumed Kafka event that failed the processing phase (e.g. because of parsing error) will be written into a corresponding output error file including the processing error message so it can be later inspected and resolved.

* **Offsets Replay:** As each job run starts from the last saved state file, it is possible to run jobs starting from an older state just by deleting all proceeding state files, this will cause the next job run to consume all events starting from that state offsets.

* **Contention Protected:** uses a distributed lock (through zookeeper) to promise a single Spark job runs concurrently avoiding write contention and potential inconsistency/corruption.
 
## Overview

![Alt text](./etl_light.png)

A single job goes through the following steps:

* Reads the last committed state file (Json format) containing consumed Kafka topics/partitions offsets of the last successful job run.
* On the first run (no available state yet) or whenever a new topic or partition is added, a configuration property is used to determine the starting offset (smallest / largest) to be consumed.
* Processing is done per topic/partition (max of one Spark executor per Kafka partition).
* Each partition processing transforms available events read from Kafka into an Avro GenericRecord and saves them into a Parquet format file partitioned by type (using configuration to map event types into base folder names) and time (using a timestamp field of the read events to map into folder time partition).
* Only after successful processing of all partitions - a new state file is saved in target storage (e.g. HDFS/S3) to be used as a starting point by the next job run.

### **Pipeline, Writers and Transformers:**

A Pipeline (yamrcraft.etlight.pipeline.Pipeline) is a processor of individual event as it was extracted from Kafka source (given as a raw byte array type). it is a composition of a Transformer and Writer instances where the output of the Transformer is the input of the Writer.

A Pipeline instance is created by an implementation of yamrcraft.etlight.pipeline.PipelineFactory.

A PipelineFactory class must be configured in the provided job' configuration file (application.conf). 
  
There are 2 implementations of the PipelineFactory trait:

* JsonPipelineFactory:
    Creates a Pipeline that is composed of a JsonTransformer and TimePartitioningWriter (configured with a string writer).
    Output is a time partitioned folder structure containing files with string format (Json events).
* ParquetPipelineFactory - 
    Creates a Pipeline that is composed of a AvroTransformer and TimePartitioningWriter (configured with a parquet writer).
    Output is a time partitioned folder structure containing files with Parquet format.

Different compositions of Transformers and Writers can be built into Pipelines as needed.

## Configuration

See configuration examples under: core/src/main/resources

**spark.conf:** properties used to directly configure Spark settings, passed to SparkConf upon construction.

**kafka.topics:** a list of kafka topics to be consumed by this job run.

**kafka.conf:** properties used to directly configure Kafka settings, passed to KafkaUtils.createRDD(...). 

**etl.lock:** can be used to set a distributed lock in order to prevent the same job from running concurrently avoiding possible contention/corruption of data. 

**etl.state:** sets the destination folder that holds the state files and the number of last state files to keep.

**etl.errors-folder:** location of error folder to hold failed processed events (e.g. parse errors).

**etl.pipeline:** defines a pair of transformer and writer used together to create a processing pipeline for processing a single Kafka topic partition. 

**etl.pipeline.transformer.config:** transformer configurations.

**etl.pipeline.writer.config:** writer configurations. 


## Build

Generates an uber jar ready to be used for running as a Spark job:

    $ sbt assembly  

## Run

Copy assembly jar and application.conf file into \<deploy folder\>. 
 
Run in **yarn client** mode (running driver locally):

    $ cd <deploy folder>
    $ spark-submit --driver-java-options "-DSPARK_YARN_MODE=true" --master yarn-client <assembly jar> <application.conf>
    
Run in **yarn-cluster** mode (running driver in yarn application master):
    
    $ cd <deploy folder>
    $ spark-submit --driver-java-options "-DSPARK_YARN_MODE=true" --master yarn-cluster <assembly jar> <application.conf>

## Run periodically (using 'oozie' job-scheduler)

[TBD]

## Test

Testing is based on integration tests running docker containers, docker-compose is used to start zookeeper, kafka and spark (standalone mode) container instances.
 
    $ sbt it:test