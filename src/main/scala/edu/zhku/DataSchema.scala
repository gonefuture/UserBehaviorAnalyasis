package edu.zhku

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

/*
 *  @author : 钱伟健 gonefuture@qq.com
 *  @version    : 2018/4/6 21:27.
 *  说明：
 */
/**
  * <pre> <pre>
  */
object DataSchema {

    //    val schema = SocSchemaCollection.getSchemaBySourceName(sourceName) //从数据库加载json schema
    val logSchema = StructType(mutable.Seq(
        StructField("userId", DataTypes.StringType),
        StructField("day", DataTypes.StringType),
        StructField("begintime", DataTypes.TimestampType),
        StructField("endtime", DataTypes.TimestampType),
        StructField("data", DataTypes.createArrayType(
            StructType(
                mutable.Seq(
                    StructField("activetime", DataTypes.LongType),
                    StructField("package", DataTypes.StringType)
                )
            ) )
        ))
    )


    val kafkaSchema = StructType(mutable.Seq(
        StructField("key", DataTypes.BinaryType),
        StructField("value", logSchema),
        StructField("topic", DataTypes.StringType),
        StructField("partition", DataTypes.IntegerType),
        StructField("offset", DataTypes.LongType),
        StructField("timestamp", DataTypes.LongType),
        StructField("timestampType", DataTypes.IntegerType)
        )
    )

}
