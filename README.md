# ETL Light

Light and effective Extract-Transform-Load job based on Apache Spark, tailored for use-cases where the source is Kafka and the sink is hadoop file system implementation such as HDFS, Amazon S3 or local FS (useful for testing).

**Features:**

* **HDFS/S3 Sink:** write can be done to both HDFS and Amazon S3 by using different URL schemes ('hdfs://', 's3://') in relevant configuration entries.

* **Configurable and Customizable:** configurable through application.conf file (see example at: etlite/src/it/resource and etlite/src/main/resources), both transformer and writer classes can be replaced through 'pipeline' configuration.

* **Protobuf full support:** includes a fully configurable pipeline consuming protobuf messages from Kafka (see examples section).

* **Scalable:** by leveraging Spark framework, scalability is achieved by default. Using Kafka RDD source enables maximum efficiency having as many executors as the number of topics partitions (can be configured for less). 

* **Easy Upgrades:** as this ETL runs as a Spark job either on demand or more typically using some external job-scheduler (e.g. oozie), upgrading it means stopping current job-scheduling, deploying the new artifacts (jar, config) and restarting job-scheduling (no management of long running processes lifecycle running in multiple machines).

* **Transparent:** all consumed Kafka topics/partitions offsets within a single executed job are being committed (saved) into a single state file (in Json format) as the last step of the job execution, apart from using this state as a starting point for the next job that runs, it also provides an easy way to view the history of recent job executions (number of recent files to maintain is configurable), their time and topics/partitions/offsets.

* **Exactly Once Delivery:** this relates to the process of moving events between Kafka to target storage, since each job run starts from the last successful saved state and critical failures exits the job without committing a new state - any job running after that will try to re-consume all events since last successful run until success. duplicates are avoided by creating a unique file names containing the job id, any re-run of a job will use the same job-id and will override existing files.

* **Error Handling:** each consumed Kafka event that failed the processing phase (e.g. because of parsing error) will be written into a corresponding output error file including the processing error message so it can be later inspected and resolved.

* **ETL Replay:** As each job run starts from the last saved state file, it is possible to run jobs starting from an older state just by deleting all proceeding state files or by modifying offset entries within the file, this will cause the next job run to consume all events starting from that state offsets.

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

A Pipeline (yamrcraft.etlite.pipeline.Pipeline) is a processor of individual event as it was extracted from Kafka source (given as a raw byte array type). it is a composition of a Transformer and Writer instances where the output of the Transformer is the input of the Writer.

A Pipeline instance is created by an implementation of yamrcraft.etlite.pipeline.PipelineFactory.

A PipelineFactory class must be configured in the provided job' configuration file (application.conf). 
  
There are several implementations of the PipelineFactory trait:

* JsonToSimpleFilePipelineFactory:
    Creates a Pipeline that is composed of a JsonTransformer and TimePartitioningWriter (configured with a string writer).
    Output is a time partitioned folder structure containing files with string format (Json events).
* JsonToParquetPipelineFactory: 
    Creates a Pipeline that is composed of a JsonToAvroTransformer and TimePartitioningWriter (configured with a parquet writer).
    Output is a time partitioned folder structure containing files with Parquet format.
* GenericProtoPipelineFactory:
    Creates a Pipeline that is composed of a ProtoTransformer and TimePartitioningWriter.
    For more info see examples.

Different compositions of Transformers and Writers can be built into Pipelines as needed.

## Configuration

See configuration examples under: etlite/src/main/resources

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

Generates an uber jar ready to be used for running as a Spark job (specific use cases might require extension):

    $ sbt etlite/assembly  

## Test

Integration (black-box) tests can be found under 'etlite' project: src/it.

Each integration test generally goes through these steps:
1. docker-compose up - starting zookeeper, kafka and spark containers.
2. ingests events that are relevant for the specific test case into kafka (e.g. json, protobuf).
3. runs ETL spark job inside the spark container, the job to run is determined by the provided configuration file (application.conf).
4. asserts ETL job run artifacts.
5. docker-compose down.

To run integration tests first create etlite uber jar and then run the tests: 
 
    $ sbt etlite/assembly           # creates etlite assembly jar
    $ sbt proto-messages/assembly   # creates an external protobuf messages jar used by the protobuf integration test
    
    $ sbt etlite/it:test            # runs all integration tests under src/it using dockers for kafka/spark
    

## Run

Copy generated 'etlite' assembly jar and your relevant application.conf file into target spark machine and place in the following commands: 

(*) Note: to read configuration file from local file system, prefix path with: "file:///"

    $ spark-submit <assembly jar> <application.conf>
 
Run in **yarn client** mode (running driver locally):

    $ spark-submit --master yarn-client <assembly jar> <application.conf>
    
Run in **yarn-cluster** mode (running driver in yarn application master):
    
    $ spark-submit --master yarn-cluster <assembly jar> <application.conf>

## Examples

### Protobuf to Parquet ETL

Off the shelf support for protobuf events transformation and loading - only requires custom configuration and inclusion of the referenced protobuf events jar in spark classpath.  

(*) assuming each kafka topic holds messages of a single protobuf schema, which is a good practice to have anyway.

In order to run a job that reads protobuf serialized messages from Kafka you will need to do the following:

1. set the relevant configuration entries in application.conf file as follows:

1.1 set the pipline factory class to be used, in this case protobuf pipeline class:
 
    factory-class = "yamrcraft.etlite.pipeline.GenericProtoPipelineFactory"
    
1.2 set the mapping between your topic name to the protobuf event class fully qualified name, in this example the topic 'events' is expected to have protobuf messages that can be desrialized into an object of class 'examples.protobuf.UserOuterClass$User' which must exist in the classpath (see next how to configure it).   

1.3 set the timestamp-field to specify a field in the protobuf event of a protobuf.Timestamp type, it is later used by the writer to resolve the storage partition.
    
    transformer = {
          config = {
            timestamp-field = "time"
            topic-to-proto-class {
              "events" = "examples.protobuf.UserOuterClass$User"
            }
          }
        }

    
2. run the job and add your external protobuf messages jar (the jar that contains your protobuf generated classes) to spark classpath, for example:

        $ spark-submit --jar=<path to external protobuf messages> <assembly jar> <application.conf>



See running example under 'etlite/src/it/scala/yamrcraft/etlite/proto/ProtobufETLIntegrationTest'.