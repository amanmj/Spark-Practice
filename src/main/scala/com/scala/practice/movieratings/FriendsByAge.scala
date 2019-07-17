package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD


object FriendsByAge extends SparkUtils {

    def main(args: Array[String]): Unit = {

      val logger = getLogger(this.getClass.getName)
      logger.info("test logger")
      val sc = getSparkContext()
      val line: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/fakefriends.csv")
      val newrdd: RDD[(String, String, String)] = line.map(row => {
        val x: Array[String] = row.split(",")
        (x(1),x(2),x(3))
      })
      newrdd.take(1)

    }




}

