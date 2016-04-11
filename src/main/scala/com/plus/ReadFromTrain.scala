package com.plus

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Eric on 2015/12/15.
 */
object ReadFromTrain {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("transport_test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val gd_train_data = sc.textFile("hdfs://node1:9000/hbase/gd_train_data.txt")
    val schemaString = "Use_city,Line_name,Terminal_id,Card_id,Create_city,Deal_time,Card_type"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = gd_train_data.map(_.split(",")).map(p => Row(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim,
      p(5).trim, p(6).trim))
    val gd_train_dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    gd_train_dataFrame.registerTempTable("gd_train_data")
    val results = sqlContext.sql("SELECT * FROM gd_train_data where Deal_time='2014112407'")
    results.printSchema()
    results.show()
    sc.stop()
  }
}
