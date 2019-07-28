package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD

object CustomerSpending extends Serializable with SparkUtils {
  def main(args: Array[String]): Unit = {
    val logger = getLogger(this.getClass.getName)
    val sc = getSparkContext()
    val lines: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/customer-orders.csv")
    val rdd: RDD[Array[String]] = lines.map(row => row.split(","))
    val modifiedRdd: RDD[(String, Float)] = rdd.map(arr => (arr(0).toString,arr(2).toFloat))
    val sumRdd: RDD[(String, Float)] = modifiedRdd.reduceByKey(_+_)
    val sortedRdd: RDD[(Float, String)] = sumRdd.map(x => (x._2,x._1)).sortByKey()
    sortedRdd.collect().foreach(println)
  }


}
