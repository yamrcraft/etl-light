package yamrcraft.etlight

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import yamrcraft.etlight.state.{KafkaOffsetsState, StateSerde}

import scala.sys.process._

class JsonETLIntegrationTest extends FlatSpec with BeforeAndAfterAll {

  val confFile = "json_application.conf"
  val eventTime = "2017-02-21 07:30:01"

  override def beforeAll(): Unit = {
    // run containers
    val code = DockerUtils.dockerComposeUp
    info(s"starting docker containers [exit code: $code]")

    // ingest 100 Json events
    publishToKafka("events", List.fill(100)(("my-key".getBytes(), s"""{"name": "joe", "location": "somewhere", "ts": "${eventTime}" }""".getBytes)))
    info("100 Json events ingested")

    // ingest malformed Json event
    publishToKafka("events", List(("my-key".getBytes(), """{"name": "joe", "location": somewhere }""".getBytes)))
    info("malformed Json event ingested")

    // ingest Json with missing timestamp property
    publishToKafka("events", List(("my-key".getBytes(), """{"name": "joe", "location": "somewhere" }""".getBytes)))
    info("Json with missing timestamp event ingested")

    //Thread.sleep(5000)
    val sparkExitCode = runSparkJob(confFile)
    info(s"spark job run [exit code: $sparkExitCode]")
  }

  override def afterAll(): Unit = {
    val code = DockerUtils.dockerComposeDown
    info(s"stopping docker containers [exit code: $code]")
  }

  "an ETL job consuming JSON events from Kafka" should "write parsed events into output folder" in {
//    val outputPath = "/root/etl/output/events/AuditEvent/2017/02/21/07/"
//    val output: String = s"""docker exec it_spark_1 find $outputPath -name '*.txt'  -type f -exec cat {} + """.!!
//    println("output = " + output)
//    println(output.split('\n').length)
//    assert(output.split('\n').length === 100)
  }

  it should "write error file with malformed events" in {

  }


  it should "commit new state file" in {
    val state = readStateFile("/root/etl/work/state/state-0")
    assert(state.jobId === 0)

    // Note: topic is configured to have two partitions
    assert(state.ranges.length === 2)

    // num of ingested events should be equal to stored indexes in state file
    assert(state.ranges.foldLeft(0)((a,b) => a + b.untilOffset.asInstanceOf[Int]) === 102)
  }

  private def runSparkJob(confFile: String): Int = {
    s"docker exec  it_spark_1 /usr/spark-2.1.0/bin/spark-submit /usr/etl-light/core-assembly-0.1.0.jar  /usr/etl-light/resources/$confFile".!
  }

  private def publishToKafka(topic: String, events: List[(Array[Byte], Array[Byte])]) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("partition.assignment.strategy", "range")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    for (event <- events) {
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, event._1, event._2))
    }
  }

  private def readStateFile(stateFile: String): KafkaOffsetsState = {
    val stateFileContent = s"docker exec it_spark_1 cat $stateFile".!!
    StateSerde.deserialize(stateFileContent)
  }

}
