package edu.zhku


import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/*
 *  @author : 钱伟健 gonefuture@qq.com
 *  @version    : 2018/4/12 21:32.
 *  说明：
 */
/**
  * <pre> <pre>
  */
object BehaviorHourly {
    System.setProperty("hadoop.home.dir", "/data/install/apache/hadoop-2.9.0")
    //System.setProperty("hadoop.home.dir", "D:\\program\\hadoop")

    var zookeeperservers = "master:2181,slave1:2181,slave2:2181"
    var tablename = "userHourly"

    //  hbase的相关配置
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum",zookeeperservers)
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")

    var table: Table = _

    // 定义类
    case class apptimes(activetime:String, `package`:String)
    case class UserHourly(userId:String, endtime:Long, data: List[(String,Long)])
    case class log(userId:String, day:String, begintime:String,  endtime:Long, data: List[apptimes])


    def main(args: Array[String]): Unit = {
    //def BehaviorHourly() {
        val conf = new SparkConf().setMaster("local[2]").setAppName("behavior")

        val ssc = new StreamingContext(conf, Seconds(3))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "use_a_separate_group_id_for_each_stream",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("behavior")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )


        stream.map(record => record.value)
                .map(value => {
                    //  隐式转换，使用json4s的默认转化器
                    implicit val formats: DefaultFormats.type = DefaultFormats
                    val json = parse(value)
                    // 样式类从JSON对象中提取值
                   json.extract[log]
                }).window( Seconds(3600), Seconds(60))
                .foreachRDD(
                    rdd => {
                        rdd.foreachPartition(partitionOfRecords => {    // 循环分区
                            // 获取Hbase连接，分区创建一个连接，分区不跨节点，不需要序列化
                            var connection: Connection = ConnectionFactory.createConnection(hbaseconf)
                            table = connection.getTable(TableName.valueOf(tablename))
                            partitionOfRecords.foreach(logData => {
                                val theput= new Put(Bytes.toBytes(String.valueOf(new Date().getTime)+"_"+logData.endtime))
                                theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userId"),Bytes.toBytes(logData.userId.toString))
                                logData.data.foreach(
                                    appTime => {
                                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes(appTime.`package`.toString),Bytes.toBytes(appTime.activetime.toString))
                                    }
                                )
                                table.put(theput)
                                table.close()
                            })
                        })
                    }
                )


        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
    }

}
