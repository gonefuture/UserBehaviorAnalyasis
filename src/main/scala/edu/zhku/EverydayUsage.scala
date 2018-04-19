package edu.zhku

import java.time.Instant


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{explode, window}

/*
 *  @author : 钱伟健 gonefuture@qq.com
 *  @version    : 2018/3/14 17:24.
 *  说明：
 */

/**
  * <pre> </pre>
  */
object EverydayUsage {
    //System.setProperty("hadoop.home.dir", "/data/install/apache/hadoop-2.9.0")
    System.setProperty("hadoop.home.dir", "D:\\program\\hadoop")
    var zookeeperservers = "master:2181,slave1:2181,slave2:2181"
    var tablename = "everyday"

    //  hbase的相关配置
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum",zookeeperservers)
    //hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    //hbaseconf.set("zookeeper.znode.parent","hbase-unsecure")
    var connection: Connection = ConnectionFactory.createConnection(hbaseconf)
    var table:Table = _


    def everyday(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("EverydayUsage")
            .master("local[2]")
            .getOrCreate()

        import spark.implicits._
        println("========================= 计算每天每个应用的使用时间 ======================================")
        println("=========================    需要输入查找天数参数======================================")
        var day = "2018-03-14"
        val dayData = spark
            .readStream
            .schema(DataSchema.logSchema)
            .json("D:\\data\\behavior\\"+day+"*.log")




        val appActiveTimeByDay = dayData
            .withColumn("data", explode(dayData("data")))
            .select("day","data.*")
            .groupBy(
                window($"endtime", "60 minutes", "1 minutes"),
                $"day",
                $"packagename")
            .sum("activetime")
            .writeStream
            .outputMode("complete")
            .foreach(
                new ForeachWriter[Row]{
                    def open(partitionId:Long,version:Long):Boolean={
                        table = connection.getTable(TableName.valueOf(tablename))
                        true
                    }

                    def process(record:Row):Unit={
                        println(record.toString)
                        println("=========================开始储存==============================")
                        val theput= new Put(Bytes.toBytes(Instant.now().toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("day"),Bytes.toBytes(record.get(0).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("packagename"),Bytes.toBytes(record.get(1).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("activetime"),Bytes.toBytes(record.get(2).toString))
                        table.put(theput)
                    }
                    def close(errorOrNull:Throwable):Unit={
                        println("************************* 关闭 ***********************")
                        println(errorOrNull)
                    }
                }
                )
        .queryName("EverydayUsage")
            .format("foreach")
            .start()
        appActiveTimeByDay.awaitTermination()
        println("=================================================================================")


    }

}
