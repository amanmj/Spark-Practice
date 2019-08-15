package com.scala.practice.movieratings

import org.apache.log4j.Logger
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object SuperheroDegreesOfFreedom extends SparkUtils {
  val logger: Logger = getLogger(this.getClass.getName)
  val sc: SparkContext = getSparkContext()
  val graphrdd: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/Marvel-graph.txt")
  val superheroname: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/Marvel-names.txt")
  val startCharacterId = 5306
  val targetCharacterId = 14

  private var hitCounter:Option[Accumulator[Int]] = None

  type bfsdata = (Array[Int],Int,String)
  type bfsnode = (Int,bfsdata)


  def convertToBfs(line : String) : bfsnode = {
    val field = line.split("\\s+")
    val id = field(0).toInt

    var connections : ArrayBuffer[Int] = ArrayBuffer()
    for(connection <- 1 to field.length-1)
      connections += field(connection).toInt

    var color = "WHITE"
    var dist = 100000000

    if(startCharacterId == id) {
      color = "GRAY"
      dist = 0
    }
    (id,(connections.toArray,dist,color))
  }
}
