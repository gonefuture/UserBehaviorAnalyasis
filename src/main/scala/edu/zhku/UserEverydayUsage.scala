package edu.zhku


import java.util.Date


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
object UserEverydayUsage {
    //System.setProperty("hadoop.home.dir", "/data/install/apache/hadoop-2.9.0")
    System.setProperty("hadoop.home.dir", "D:\\program\\hadoop")
    var zookeeperservers = "192.168.90.12:2181"
    var tablename = "userEveryday"

    //  hbase的相关配置
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum",zookeeperservers)
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")

    var connection: Connection = ConnectionFactory.createConnection(hbaseconf)
    var table:Table = _

    //def main(args: Array[String]) {
    def userEveryday(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("EverydayUsage")
            .master("local[2]")
            .getOrCreate()



        import spark.implicits._
        println("========================= 计算每天每个应用的使用时间 ======================================")
        println("=========================    需要输入查找天数参数======================================")

        //  从HDFS中获取每小时的书的数据
        val dayData = spark
            .readStream
            .schema(DataSchema.logSchema)
            .json("D:\\data\\behavior\\"+"*.log")
            //.json("hdfs://master:9000/data")



        val appActiveTimeByDay = dayData
            // 拆开data中的应用使用时时长
            .withColumn("data", explode(dayData("data")))
            .select("endtime","userId","day","data.*")
            .groupBy(
                window($"endtime", "2 minutes", "1 minutes"),
                $"userId",
                $"day",
                $"package")
            .sum("activetime")
            .writeStream
            .outputMode("update")
            .foreach(
                new ForeachWriter[Row]{
                    def open(partitionId:Long,version:Long):Boolean={

                        table = connection.getTable(TableName.valueOf(tablename))
                        true
                    }

                    def process(record:Row):Unit={
                        println(record.toString)
                        println("=========================开始储存==============================")
                        // 以时间戳和和应用名称组合作为主键
                        val theput= new Put(Bytes.toBytes(String.valueOf(new Date().getTime)+"_"+record.get(3).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userId"),Bytes.toBytes(record.get(1).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("day"),Bytes.toBytes(record.get(2).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("packagename"),Bytes.toBytes(record.get(3).toString))
                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("activetime"),Bytes.toBytes(record.get(4).toString))
                        table.put(theput)
                    }
                    def close(errorOrNull:Throwable):Unit={
                        table.close()
                        println("************************* 关闭 ***********************")
                        println(errorOrNull)
                    }
                }
            )
            .queryName("UserEverydayUsage")
            .format("foreach")
            .start()
        appActiveTimeByDay.awaitTermination()
        println("=================================================================================")


    }

}
