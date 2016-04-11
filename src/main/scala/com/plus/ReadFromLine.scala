package com.plus

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

/**
 * Created by Eric on 2015/12/15.
 */

object ReadFromLine {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("transport_test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val gd_line_data = sc.textFile("hdfs://node1:9000/hbase/gd_line_desc.txt")
    val schemaString = "Line_name,Stop_cnt,Line_type"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = gd_line_data.map(_.split(",")).map(p => Row(p(0).trim, p(1).trim, p(2).trim))
    val gd_line_dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    gd_line_dataFrame.registerTempTable("gd_line_data")
    val results = sqlContext.sql("SELECT count(*) FROM gd_line_data")
    results.printSchema()
    results.show()
    sc.stop()
  }

}
