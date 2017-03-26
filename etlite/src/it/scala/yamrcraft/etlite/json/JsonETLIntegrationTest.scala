package yamrcraft.etlite.json

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import yamrcraft.etlite.state.{KafkaOffsetsState, StateSerde}
import yamrcraft.etlite.{DockerEnv, KafkaPublisher}

class JsonETLIntegrationTest extends FlatSpec with BeforeAndAfterAll {

  val jarFileName = "etlite-assembly-0.1.0.jar"
  val configFileName = "json_application.conf"

  val eventTime = "2017-02-21 07:30:01"

  override def beforeAll(): Unit = {

    // run containers
    info(s"starting docker containers...")
    val code = DockerEnv.dockerComposeUp
    info(s"docker containers started [exit code: $code]")

    val kafkaPublisher = new KafkaPublisher()

    // ingest 100 Json events.
    val event = s"""{"type": "UserEvent", "name": "joe", "location": "somewhere", "ts": "$eventTime" }""".getBytes
    kafkaPublisher.send("events", List.fill(100)(event))
    info("100 Json events ingested")

    // ingest malformed Json event
    kafkaPublisher.send("events", List("""{"type": "UserEvent", "name": "joe", "location": somewhere }""".getBytes))
    info("1 malformed Json event ingested")

    // ingest Json with missing timestamp property
    kafkaPublisher.send("events", List("""{"type": "UserEvent", "name": "joe", "location": "somewhere" }""".getBytes))
    info("1 Json with missing timestamp event ingested")

    val sparkExitCode = DockerEnv.runSparkJob(jarFileName, configFileName)
    info(s"spark job run [exit code: $sparkExitCode]")
  }

  override def afterAll(): Unit = {
    val code = DockerEnv.dockerComposeDown
    info(s"stopping docker containers [exit code: $code]")
  }

  "an ETL job consuming JSON events from Kafka" should "write parsed events into output folder" in {
//    val outputPath = "/var/etl/output/events/users_events/2017/02/21/07/"
//    val output: String = s"""docker exec it_spark_1 find $outputPath -name '*.txt'  -type f -exec cat {} + """.!!
//    println("output = " + output)
//    println(output.split('\n').length)
//    assert(output.split('\n').length === 100)
  }

  it should "write error files with malformed events" in {

  }


  it should "commit new state file" in {
    val state = readStateFile("/var/etl/state/state-0")
    assert(state.jobId === 0)

    // Note: topic is configured to have two partitions
    assert(state.ranges.length === 2)

    // num of ingested events should be equal to stored indexes in state file
    assert(state.ranges.foldLeft(0)((a,b) => a + b.untilOffset.asInstanceOf[Int]) === 102)
  }

  private def readStateFile(stateFile: String): KafkaOffsetsState = {
    StateSerde.deserialize(DockerEnv.readFileFromDocker(stateFile))
  }

}
