package yamrcraft.etlite

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaPublisher {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("partition.assignment.strategy", "range")
  props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  def send(topic: String, event: Array[Byte]): Unit = {
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, event))
  }

  def send(topic: String, events: List[Array[Byte]]): Unit = {
    for (event <- events) {
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, event))
    }
  }

}
