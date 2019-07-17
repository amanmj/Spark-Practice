package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD

import scala.util.Try

object WeatherData extends SparkUtils {
  def main(args: Array[String]): Unit = {
    val logger = getLogger(this.getClass.getName)
    val sc = getSparkContext()
    val lines: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/1800.csv")
    val newrdd: RDD[(String, String, Float)] = lines.map(line => {
      val x = line.split(",")
      val stationId = x(0)
      val entrytype  = x(2)
      val temperature = x(3).toFloat*0.1f*9/5+32
      //return a tuple of 3 values
      (stationId,entrytype,temperature)
    })
    newrdd.take(1).foreach(println)

    val filterrdd = newrdd.filter(_._2 == "TMIN").map(x => (x._1,x._3))
    filterrdd.take(1).foreach(println)

    val minimumByKey = filterrdd.reduceByKey((x,y) => Math.min(x,y))
    minimumByKey.collect().foreach(println)

  }
}
