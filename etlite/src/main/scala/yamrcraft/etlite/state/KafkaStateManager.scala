package yamrcraft.etlite.state

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, ShortTypeHints}
import yamrcraft.etlite.{Settings, StateSettings}

case class KafkaOffsetsState(
  jobId: Long,
  ranges: List[OffsetRange]
) extends State

object StateSerde {

  implicit val formats =
    new Formats {
      val dateFormat = DefaultFormats.lossless.dateFormat
      override val typeHints = ShortTypeHints(List(classOf[KafkaOffsetsState], classOf[OffsetRange]))
      override val typeHintFieldName = "type"
    }

  def serialize(state: KafkaOffsetsState) = Serialization.writePretty(state).getBytes

  def deserialize(content: String) = Serialization.read[KafkaOffsetsState](content)
}

class KafkaStateManager(settings: StateSettings) extends StateManager[KafkaOffsetsState] {

  override def readState: Option[KafkaOffsetsState] = {
    val reader = new StateReader(settings.stateFolder)
    val state = reader.readLastState
    state map (content => StateSerde.deserialize(content))
  }

  override def commitState(state: KafkaOffsetsState): Unit = {
    val stateBytes = StateSerde.serialize(state)
    val writer = new StateWriter(settings.stateFolder, settings.stateFilesToKeep)
    writer.write(stateBytes, state.jobId)
  }

  override def fetchNextState(lastState: Option[KafkaOffsetsState], settings: Settings): KafkaOffsetsState = {

    val largestOffsets = fetchOffsetRanges(settings.kafka.properties, settings.kafka.topics)

    lastState match {
      case None => {
        val startingOffsets = fetchSmallestOrElse(settings, largestOffsets)
        val offsetRanges = mergeOffsets(startingOffsets, largestOffsets, settings)
        KafkaOffsetsState(0, offsetRanges)
      }

      case Some(lastState) => {
        val stateOffsets = lastState.ranges.map(r => new TopicAndPartition(r.topic, r.partition) -> r.untilOffset).toMap
        val offsetRanges = mergeOffsets(stateOffsets, largestOffsets, settings)
        KafkaOffsetsState(lastState.jobId + 1, offsetRanges)
      }
    }
  }

  /**
    * Merges two offset lists into one offset range, the 'until' list contains a complete list of topics and partitions
    * for which a matching starting offset is searched from the 'from' list (last saved state), if not found (for example
    * when new topic was added to config) then starting point is fetched from Kafka.
    */
  private def mergeOffsets(from: Map[TopicAndPartition, Long], until: Map[TopicAndPartition, Long], settings: Settings): List[OffsetRange] = {
    var fromOverride: Option[Map[TopicAndPartition, Long]] = None
    val result =
      for (untilEntry <- until) yield {
        val fromOffset =
          from.getOrElse(untilEntry._1, {
            fromOverride = fromOverride.orElse(Some(fetchSmallestOrElse(settings, until)))
            fromOverride.get.get(untilEntry._1).get
          })

        OffsetRange.create(untilEntry._1.topic, untilEntry._1.partition, fromOffset, untilEntry._2)
      }
    result.toList
  }

  private def fetchSmallestOrElse(settings: Settings, default: Map[TopicAndPartition, Long]) = {
    val isSmallest = settings.kafka.properties.get("auto.offset.reset").map(_.toLowerCase).exists(_.equals("smallest"))
    if (isSmallest) {
      fetchOffsetRanges(settings.kafka.properties, settings.kafka.topics, isSmallest)
    } else {
      default
    }
  }

  private def fetchOffsetRanges(kafkaProps: Map[String, String], topics: Set[String], smallest: Boolean = false): Map[TopicAndPartition, Long] = {
    val kc = new KafkaCluster(kafkaProps)
    val result =
      for {
        topicPartitions <- kc.getPartitions(topics).right
        leaderOffsets <- (if (smallest) {
          kc.getEarliestLeaderOffsets(topicPartitions)
        } else {
          kc.getLatestLeaderOffsets(topicPartitions)
        }).right
      } yield {
        leaderOffsets.map { case (tp, lo) => (tp, lo.offset) }
      }

    KafkaCluster.checkErrors(result)
  }

}

