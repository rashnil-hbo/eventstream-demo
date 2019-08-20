package com.hbo.dma.eventstream

import com.hbo.dma.eventstream.DapKafkaUtils
import org.apache.spark.sql.SparkSession

object DapKafkaProducer {

  def main(args:Array[String]): Unit = {

    val topic = args(1)
    val kafka_bootstrap_servers = args(2)

    val kafka = new DapKafkaUtils(kafka_bootstrap_servers)
    val sTime = System.currentTimeMillis

    val s3_src_path = args(3)
    val partitions = args(4).toInt

    val spark = SparkSession.builder.appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","10")

    val testDF = spark.read
      .option("inferschema" , "true").json(s3_src_path)
      .foreach(row => {
        kafka.sendMessage(topic,row.toString())
      })

    kafka.closeProducer()
  }

}
