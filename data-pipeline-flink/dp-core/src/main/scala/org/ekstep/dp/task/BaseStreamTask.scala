package org.ekstep.dp.task

import java.nio.charset.StandardCharsets
import java.util

import scala.reflect.{ClassTag, classTag}
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.ekstep.dp.core.BaseJobConfig
import org.ekstep.ep.samza.events.domain.Events

abstract class BaseStreamTask(config: BaseJobConfig) extends Serializable {

  def createKafkaStreamConsumer(kafkaTopic: String): FlinkKafkaConsumer[util.Map[String, AnyRef]] = {
    new FlinkKafkaConsumer[util.Map[String, AnyRef]](kafkaTopic, new ConsumerStringDeserializationSchema, config.kafkaConsumerProperties)
  }

  def createObjectStreamConsumer[T <: Events](kafkaTopic: String)(implicit m: Manifest[T]): FlinkKafkaConsumer[T] = {
    new FlinkKafkaConsumer[T](kafkaTopic, new ConsumerObjectDeserializationSchema[T], config.kafkaConsumerProperties)
  }

  def createObjectStreamProducer[T <: Events](kafkaTopic: String)(implicit m: Manifest[T]): FlinkKafkaProducer[T] = {
    new FlinkKafkaProducer[T](kafkaTopic,
      new ProducerV3EventSerializationSchema[T](kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

}

class ConsumerObjectDeserializationSchema[T <: Events](implicit ct: ClassTag[T]) extends KafkaDeserializationSchema[T] {

  override def isEndOfStream(nextElement: T): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = {
    val parsedString = new String(record.value(), StandardCharsets.UTF_8)
    val result = new Gson().fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass)
    ct.runtimeClass.getConstructor(classOf[util.Map[String, AnyRef]]).newInstance(result).asInstanceOf[T]
  }

  override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(classTag[T].runtimeClass).asInstanceOf[TypeInformation[T]]
}

class ConsumerStringDeserializationSchema extends KafkaDeserializationSchema[util.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L

  override def isEndOfStream(nextElement: util.Map[String, AnyRef]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): util.Map[String, AnyRef] = {
    val parsedString = new String(record.value(), StandardCharsets.UTF_8)
    val gson: Gson = new Gson()
    gson.fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass)
  }

  override def getProducedType: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
}

class ProducerV3EventSerializationSchema[T <: Events : Manifest](topic: String, key: Option[String] = None) extends KafkaSerializationSchema[T] {
  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), element.getJson.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getJson.getBytes(StandardCharsets.UTF_8)))
  }
}

