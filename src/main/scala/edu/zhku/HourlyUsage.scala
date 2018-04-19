package edu.zhku

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
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
object HourlyUsage {
    System.setProperty("hadoop.home.dir", "D:\\program\\hadoop")
    var zookeeperservers = "master:2181,slave1:2181,slave2:2181"
    var tablename = "hourly"

    //  hbase的相关配置
    val hbaseconf: Configuration = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum",zookeeperservers)
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")

   // var connection: Connection = ConnectionFactory.createConnection(hbaseconf)

    def Hourly(args: Array[String]) {
    //def main(args:Array[String]){
        val spark = SparkSession
            .builder()
            .appName("HourlyUsage")
            .master("local[2]")
            .getOrCreate()



        import spark.implicits._

        println("========================= 计算每个小时实时的应用的使用时间 ==================================")
        println("=========================    需要输入查找天数参数======================================")
        val hourData = spark
            .readStream
            .schema(DataSchema.logSchema)
            .json("D:\\data\\behavior\\*.log")

        val appActiveTimeByHour = hourData
            .withColumn("data", explode($"data"))
            .select("day","endtime","data.*")
            .groupBy(
                window($"endtime", "5 minutes", "1 minutes"),
                $"package"
            )
            .sum("activetime")
            .writeStream
            .outputMode("complete")
            .foreach(
                new ForeachWriter[Row]{
                    def open(partitionId:Long,version:Long):Boolean={
                            true
                    }

                    def process(record:Row):Unit={
                        println(record.toString)
                        println("=========================开始储存==============================")
//                        val conf = HBaseConfiguration.create()
//                        val table = connection.getTable(TableName.valueOf(tablename))
//                        val theput= new Put(Bytes.toBytes(record.get(0).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("packagename"),Bytes.toBytes(record.get(1).toString))
//                        theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("activetime"),Bytes.toBytes(record.get(2).toString))
//                        table.put(theput)
                    }
                    def close(errorOrNull:Throwable):Unit={
                        println("************************* 关闭 ***********************")
                        println(errorOrNull)
                    }
                }
                )
            .queryName("HourlyUsage")
            .format("foreach")
            .start()
        appActiveTimeByHour.awaitTermination()
        println("=================================================================================")




    }

}
