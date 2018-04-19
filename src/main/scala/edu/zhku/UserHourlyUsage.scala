package edu.zhku

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._



/*
 *  @author : 钱伟健 gonefuture@qq.com
 *  @version    : 2018/3/14 17:02.
 *  说明：
 */

/**
  * <pre> </pre>
  */
object UserHourlyUsage {
    System.setProperty("hadoop.home.dir", "/data/install/apache/hadoop-2.9.0")
    var zookeeperservers = "master:2181,slave1:2181,slave2:2181"
    var tablename = "userHourly"

    //  hbase的相关配置
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum",zookeeperservers)
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    //hbaseconf.set("zookeeper.znode.parent","/hbase-unsecure")
    var connection: Connection = ConnectionFactory.createConnection(hbaseconf)
    var table: Table = _

    //def main(args: Array[String]) {
    def userHourlyUsage() {
        val spark = SparkSession
            .builder()
            .appName("UserHourlyUsage")
            .master("local[2]")
            .getOrCreate()



        import spark.implicits._

        println("========================= 计算用户每个小时实时的应用的使用时间 ==================================")
        println("=========================    需要输入查找天数参数======================================")

        val logData = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
            .option("subscribe", "behavior")
            //.schema(DataSchema.kafkaSchema)
            .load()
            //.schema(DataSchema.logSchema)
            //.json("D:\\data\\behavior\\"+"*.log")



        println("=============================开始计算每个小时的用户app使用情况======================================")
        val appActiveTimeByDay = logData
           .selectExpr("CAST(value AS STRING)")
            .as[ String]

//            .withColumn("data", explode($"data"))
//            .select("endtime","userId","day","data.*")
//            .groupBy(
//                window($"endtime", "60 minutes", "2 minutes"),
//                $"userId",
//                $"day",
//                $"package")
//            .sum("activetime")
            .writeStream
            .outputMode("update")
//            .foreach(
//                new ForeachWriter[Row]{
//                    def open(partitionId:Long,version:Long):Boolean={
//
//                        table = connection.getTable(TableName.valueOf(tablename))
//                        true
//                    }
//
//                    def process(record:Row):Unit={
//                        println(record.toString)
//                        println("=========================开始储存==============================")
//
//                        val theput= new Put(Bytes.toBytes(String.valueOf(new Date().getTime)+"_"+record.get(3).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userId"),Bytes.toBytes(record.get(1).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("day"),Bytes.toBytes(record.get(2).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("packagename"),Bytes.toBytes(record.get(3).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("activetime"),Bytes.toBytes(record.get(4).toString))
//                        table.put(theput)
//                    }
//                    def close(errorOrNull:Throwable):Unit={
//                        println("************************* 关闭 ***********************")
//                        println(errorOrNull)
//                    }
//                }
//            )
            .queryName("UserHourlyUsage")
            .format("console")
            .start()
        appActiveTimeByDay.awaitTermination()
        println("=================================================================================")


    }

}
