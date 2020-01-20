package com.wbbigdata

import java.lang

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark消费canal发送到kafka中的数据
  */
object Canal2Kafka {

  def main(args: Array[String]): Unit = {

    // 设置运行时的用户
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val conf = new SparkConf()
      .setAppName("Spark2Kafka")
      .setMaster("local")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))

    // 直连方式相当于跟kafka的topic直接连接
    // "auto.offset.reset" -> "earliest"(每次重启重新开始消费)，"auto.offset.reset" -> "latest"(每次重启从最新的offset消费)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "wbbigdata00:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "canal",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array("example")

    val canalDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val valueRDD = canalDStream.map(item => item.value())

    //foreachRDD作用于DStream中每一个时间间隔的RDD
    //foreachPartition作用于每一个时间间隔中RDD的每一个partition
    //foreach作用于每一个RDD中的每一个元素
    val aggrDStream = valueRDD.foreachRDD(item => item.foreachPartition{
      partitionRecords => partitionRecords.foreach{
        //通常在分区中创建连接，然后将每个处理后的数据存进数据库
        //val connection = ConnectionPool.getConnection()
        rdd => {
          val jSONObject = JSON.parseObject(rdd).getJSONArray("data").getJSONObject(0)
          val id = jSONObject.getString("id")
          val article_id = jSONObject.getString("article_id")
          val collect_website_name = jSONObject.getString("collect_website_name")
          val title = jSONObject.getString("title")

          val aggrInfo = "id=" + id + "|" +
          "article_id" + article_id + "|" +
          "collect_website_name" + collect_website_name + "|" +
          "title" + title

          //connection.send(aggrInfo)
        }
      }
    })

//    canalDStream.foreachRDD(rdd => rdd.foreach(println))
//    println(aggrDStream)

    ssc.start()
    ssc.awaitTermination()
  }

}
