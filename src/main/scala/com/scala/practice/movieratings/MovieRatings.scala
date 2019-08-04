package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD

object MovieRatings extends SparkUtils {
  def main(args: Array[String]): Unit = {
    val logger = getLogger(this.getClass.getName)
    val sc = getSparkContext()
    val line: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/ml-100k/u.data")
    val rdd = line.map(l => {
      val arr = l.split("\\t")
      (arr(1),1)
    })

    val sumrdd: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    sumrdd.collect().foreach(println)
    sc.broadcast()
  }

}
