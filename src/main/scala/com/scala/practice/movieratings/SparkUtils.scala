package com.scala.practice.movieratings

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext

trait SparkUtils {

  def getSparkContext() : SparkContext = {
    new SparkContext("local[*]", "MovieRatingsCounter")
  }

  def getLogger(name : String) : Logger = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    LogManager.getLogger(name)
  }

}
