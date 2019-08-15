package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD

object BookWordsCount extends SparkUtils {
  def main(args: Array[String]): Unit = {

    val sc = getSparkContext()
    val lines: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/book.txt")
    val maprdd = lines.flatMap(_.split(" "))

    val punctuatedRdd: RDD[String] = lines.map(_.toLowerCase()).flatMap(_.split("\\W+")).filter(!containsPunctuation(_))
    val reducedPuntuatedRdd = punctuatedRdd.map((_,1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey()

    reducedPuntuatedRdd.collect().foreach(println)


  }

  def containsPunctuation(str: String) : Boolean = {
    str.equals("a")
  }

}
