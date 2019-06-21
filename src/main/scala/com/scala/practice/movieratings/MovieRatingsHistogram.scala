package com.scala.practice.movieratings

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MovieRatingsHistogram {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = LogManager.getLogger(this.getClass.getName)
    logger.info("Hello World " + SparkContext.getClass.toString)

    /*
      the * here suggests that we can use all of the cores inside our system to obtain parallelism
      The second argument would be the application name
     */
    val sc = new SparkContext("local[*]", "MovieRatingsCounter")


    val line: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/ml-100k/u.data")

    val ratings: RDD[String] = line.map(_.toString).map(_.split("\t")(2))
    val count: collection.Map[String, Long] = ratings.countByValue()
    /*
    Print each value of the Map
     */
    println(count.toSeq.sortBy(_._1))

  }

}
