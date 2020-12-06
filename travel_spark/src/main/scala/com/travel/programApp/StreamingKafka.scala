package com.travel.programApp

import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.{Cell, CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable

object StreamingKafka {

  def main(args: Array[String]): Unit = {


    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    val conf = new SparkConf().setMaster("local[1]").setAppName("sparkKafka")
    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )



    //每次消费kafka的数据之前，都需要从hbase里面去查询offset的值，带上offset的值去消费即可
    //hbase的rowkey该如何设计？？

    /**
     * ssc: StreamingContext,
     * locationStrategy: LocationStrategy,
     * consumerStrategy: ConsumerStrategy[K, V]
     */


    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("sparKafka")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")

    val streamingContext = new StreamingContext(context, Seconds(1))



    /**
     * pattern: ju.regex.Pattern,
     * kafkaParams: collection.Map[String, Object],
     * offsets: collection.Map[TopicPartition, Long]  offset的值  保存在了hbase里面了
     * 需要从hbase里面去查询offset的值
     * 第一次查询的时候，有没有offset值？？？  没有offset的值
     * 第二次及以后消费kafka的数据，就会有了offset的值
     *
     * 你该如何设计hbase的表模型用于保存offset的值？？？
     * rowkey，列族，列名，列值
     *
     * 鱼苗：宽表
     *
     * 1：topic + 分区
     */

    //查询hbase的表数据，获取offset的值
   /* val connection: Connection = HbaseTools.getHbaseConn
    val admin: Admin = connection.getAdmin
    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
      val hTableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      hTableDescriptor.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(hTableDescriptor)
      admin.close()

    }

    val table: Table = connection.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
    //从table里面获取offset得值，将获取的值保存到一个map集合里面
    val partitionToLong = new mutable.HashMap[TopicPartition, Long]()

    for(eachTopic <- topics){
      val get = new Get((group + ":" + eachTopic).getBytes())
      val result: Result = table.get(get)
      val cells: Array[Cell] = result.rawCells()
      for(result <- cells){
        //列名
        val topicPartition: String = Bytes.toString(CellUtil.cloneQualifier(result))
        //列值  offset值
        val offsetValue: String = Bytes.toString(CellUtil.cloneValue(result))
        val strings: Array[String] = topicPartition.split(":")
        val partition = new TopicPartition(strings(1), strings(2).toInt)
        partitionToLong +=(partition -> offsetValue.toLong)
      }
    }

    //这里终于获取到了consumerStrategy 这个对象了
    val consumerStrategy: ConsumerStrategy[String, String] = if (partitionToLong.size > 0) {
      //第二次及以后消费kafak的数据，需要带上offset的值
      ConsumerStrategies.SubscribePattern[String, String](Pattern.compile("(.*)gps_topic"), kafkaParams, partitionToLong)
    } else {
      //第一次消费kafka的数据，没有offset的值
      ConsumerStrategies.SubscribePattern[String, String](Pattern.compile("(.*)gps_topic"), kafkaParams)

    }




    ConsumerStrategies.SubscribePattern[String,String]()



    KafkaUtils.createDirectStream()*/


    //通过封装的方法，直接获取到了kafka的数据
    val result: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, "(.*)gps_topic")


    result.foreachRDD(eachRdd =>{
      if(!eachRdd.isEmpty()){
        eachRdd.foreachPartition(eachPartition =>{
          val connection: Connection = HBaseUtil.getConnection
          val jedis: Jedis = JedisUtil.getJedis

          eachPartition.foreach(record =>{
            HbaseTools.saveToHBaseAndRedis(connection,jedis,record)

          })
          JedisUtil.returnJedis(jedis)
          connection.close()


        })
        //准备更新offset的值  获取到了offset的值，更新到hbase里面去即可
        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges


      }
    })

  }


}
