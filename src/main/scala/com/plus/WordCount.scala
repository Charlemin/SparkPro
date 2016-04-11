package com.plus

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Eric on 2015/12/15.
 */
object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("transport_test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val file = sc.textFile("hdfs://node1:9000/hbase/gd_line_desc.txt")
    file.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    println("i am eric")
  }
}