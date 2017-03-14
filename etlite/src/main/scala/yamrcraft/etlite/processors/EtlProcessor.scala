package yamrcraft.etlite.processors

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.slf4j.LoggerFactory
import yamrcraft.etlite.Settings
import yamrcraft.etlite.state.{KafkaOffsetsState, KafkaStateManager}
import yamrcraft.etlite.transformers.InboundMessage

object EtlProcessor {

  val logger = LoggerFactory.getLogger(this.getClass)

  def run(settings: Settings) = {
    val context = createContext(settings)

    val stateManager = new KafkaStateManager(settings.etl.state)

    val lastState = stateManager.readState
    logger.info(s"last persisted state: $lastState")

    val currState = stateManager.fetchNextState(lastState, settings)
    logger.info(s"batch working state: $currState")

    val rdd = createRDD(context, currState, settings)
    processRDD(rdd, currState.jobId, settings)

    logger.info("committing state")
    stateManager.commitState(currState)
  }

  private def createContext(settings: Settings) = {
    val sparkConf = new SparkConf()
      .setAppName(settings.spark.appName)
      .setAll(settings.spark.conf)

    new SparkContext(sparkConf)
  }

  private def createRDD(context: SparkContext, state: KafkaOffsetsState, settings: Settings): RDD[InboundMessage] = {
    KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, InboundMessage](
      context,
      settings.kafka.properties,
      state.ranges.toArray,
      Map[TopicAndPartition, Broker](),
      (msgAndMeta: MessageAndMetadata[Array[Byte], Array[Byte]]) => { InboundMessage(msgAndMeta.topic, msgAndMeta.key(), msgAndMeta.message()) }
    )
  }

  private def processRDD(kafkaRDD: RDD[InboundMessage], jobId: Long, settings: Settings) = {
    // passed to remote workers
    val etlSettings = settings.etl

    logger.info(s"RDD processing started [rdd=${kafkaRDD.id}, jobId=$jobId]")

    val rdd = settings.etl.maxNumOfOutputFiles.map(kafkaRDD.coalesce(_)).getOrElse(kafkaRDD)

    rdd.foreachPartition { partition =>
        // executed at the worker
        new PartitionProcessor(jobId, TaskContext.get.partitionId(), etlSettings)
          .processPartition(partition)
      }

    logger.info(s"RDD processing ended [rdd=${kafkaRDD.id}, jobId=$jobId]")
  }


}
