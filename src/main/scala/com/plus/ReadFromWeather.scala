package com.plus

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Eric on 2015/12/15.
 */
object ReadFromWeather {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("transport_test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val gd_weather_data = sc.textFile("hdfs://node1:9000/hbase/gd_weather_report.txt")
    val schemaString = "Date_time,weather,Temperature,Wind_direction_force"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = gd_weather_data.map(_.split(",")).map(p => Row(p(0).trim, p(1).trim, p(2).trim, p(3).trim))
    val gd_weather_dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    gd_weather_dataFrame.registerTempTable("gd_weather_data")
    val results = sqlContext.sql("SELECT * FROM gd_weather_data where Date_time='2014/10/17'")
    results.printSchema()
    results.show()
    sc.stop()
  }
}
