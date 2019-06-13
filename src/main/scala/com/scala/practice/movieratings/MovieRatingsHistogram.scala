package com.scala.practice.movieratings

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext


object MovieRatingsHistogram {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = LogManager.getLogger(this.getClass.getName)
    logger.info("Hello World "+SparkContext.getClass.toString)


    val sc = new SparkContext("local[*]", "MovieRatingsCounter")

    val line = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/ml-100k/u.data")
    logger.info(line.count())


  }

}
