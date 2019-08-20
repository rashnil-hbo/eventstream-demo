package com.hbo.dma.eventstream

import java.util
import java.util.{Map, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer


class DapKafkaUtils {

  protected var kafka_params: util.Map[String, String] = null
  protected var messages_sent = 0
  protected var kafka_retention_days = 7
  protected var producer_batch_size: Int = 131072
  protected val producer_buffer_memory :Long = 536870912l
  protected var producer: KafkaProducer[Long, String] = null

  def this(kafka_broker_host: String) {
    this()
    this.kafka_params = buildKafkaConfig(kafka_broker_host)
  }

  def buildKafkaConfig(brokers: String): util.Map[String, String] = {
    val kafka_params:util.Map[String, String] = null
    kafka_params.put("metadata.broker.list", brokers)
    kafka_params.put("serializer.class", "kafka.serializer.StringEncoder")
    kafka_params.put("producer.type", "async")
    kafka_params.put("request.required.acks", "0")
    kafka_params.put("queue.buffering.max.ms", "5000")
    kafka_params.put("queue.buffering.max.messages", "2000")
    kafka_params.put("batch.num.messages", "300")
    kafka_params.put("fetch.message.max.bytes", "20971520")
    kafka_params.put("enable.auto.commit", "false")
    kafka_params
  }

  //def getKafkaConfigs(): uti

  @throws[Exception]
  def sendMessage(topic: String, message: String): RecordMetadata = {
    val time = System.currentTimeMillis
    var metadata: RecordMetadata = null
    try {
      val record: ProducerRecord[Long, String] = new ProducerRecord[Long, String](topic, time, message)
      metadata = this.getProducer.send(record).get()
    } catch {
      case e: Exception =>
        throw e
    } finally {
      this.messages_sent += 1
      if (this.messages_sent >= this.producer_batch_size.intValue) {
        this.flushProducer()
        this.messages_sent = 0
      }
    }
    metadata
  }

  def getProducer: KafkaProducer[Long, String] = {
    if (this.producer == null) {
      val props = new Properties
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka_params.get("bootstrap.servers"))
      // props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 536870912l)
      this.producer_batch_size = if (this.kafka_params.get("producer.batch.size") == null) this.producer_batch_size
      else this.kafka_params.get("producer.batch.size").toInt
      // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      this.producer = new KafkaProducer[Long, String](props)
    }
    this.producer
  }

  def flushProducer(): Unit = {
    if (this.producer != null) {
      try{
        this.producer.flush()
        this.messages_sent = 0
      }catch {
        case e: Exception =>
          throw e
      }
    }
  }

  def closeProducer(): Unit = {
    if (this.producer != null) {
      this.flushProducer()
      this.producer.close()
      this.producer = null
    }
  }

}
