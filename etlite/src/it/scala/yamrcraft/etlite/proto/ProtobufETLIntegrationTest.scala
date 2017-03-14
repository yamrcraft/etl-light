package yamrcraft.etlite.proto

import examples.protobuf.UserOuterClass
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import yamrcraft.etlite.{DockerEnv, KafkaPublisher}
import com.google.protobuf.Timestamp
import yamrcraft.etlite.state.{KafkaOffsetsState, StateSerde}

class ProtobufETLIntegrationTest extends FlatSpec with BeforeAndAfterAll {

  val jarFileName = "proto-example-assembly-0.1.0.jar"
  val configFileName = "proto_application.conf"
  val topic = "events"

  override def beforeAll(): Unit = {
    // run containers
    info(s"starting docker containers...")
    val code = DockerEnv.dockerComposeUp
    info(s"docker containers started [exit code: $code]")

    val kafkaPublisher = new KafkaPublisher()

    val user =
      UserOuterClass.User.newBuilder()
        .setId(1234)
        .setName("John Smith")
        .setTime(Timestamp.newBuilder().setSeconds(1489483311))
        .setAddress("5th avenue")
        .setCity("New York")
        .setCountry("USA")
        .build()

    // ingest a User instance protobuf event
    info("1 protobuf event being ingested ...")
    kafkaPublisher.send(topic, user.toByteArray)
//
//    // ingest malformed Json event
//    kafkaPublisher.send("events", List("""{"type": "UserEvent", "name": "joe", "location": somewhere }""".getBytes))
//    info("1 malformed Json event ingested")
//
//    // ingest Json with missing timestamp property
//    kafkaPublisher.send("events", List("""{"type": "UserEvent", "name": "joe", "location": "somewhere" }""".getBytes))
//    info("1 Json with missing timestamp event ingested")
//
    val sparkExitCode = DockerEnv.runSparkJob(jarFileName, configFileName)
    info(s"spark job run [exit code: $sparkExitCode]")
  }

  override def afterAll(): Unit = {
    val code = DockerEnv.dockerComposeDown
    info(s"stopping docker containers [exit code: $code]")
  }

  "an ETL job consuming Protobuf events from Kafka" should "write parsed events into output folder" in {

  }

  it should "commit new state file" in {
    val state = readStateFile("/var/etl/state/state-0")
    assert(state.jobId === 0)

    // Note: topic is configured to have two partitions
    assert(state.ranges.length === 2)

    // num of ingested events should be equal to stored indexes in state file
    assert(state.ranges.foldLeft(0)((a,b) => a + b.untilOffset.asInstanceOf[Int]) === 1)
  }

  private def readStateFile(stateFile: String): KafkaOffsetsState = {
    StateSerde.deserialize(DockerEnv.readFileFromDocker(stateFile))
  }

}
