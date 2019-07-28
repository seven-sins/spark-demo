package com.cg.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

object Kafka2Streaming {
  
  
  
  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.205:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new StreamingContext(conf, Seconds(3))
    // TODO: 修改日志输入
    // sc.sparkContext.setLogLevel("WARN")
    
    sc.checkpoint("d:/kafka_receveiers")

    val topics = Array("testTopic")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    //
    stream.foreachRDD(rdd => {
      rdd.foreach(line => {
        println(line)
      })
    })
    
    // 开启流失计算
    sc.start()
    
    // 等待终止退出
    sc.awaitTermination()
  }
}