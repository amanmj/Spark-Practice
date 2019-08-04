package com.scala.practice.movieratings

import org.apache.spark.rdd.RDD

object SuperHeroPopularity extends SparkUtils {
  def main(args: Array[String]): Unit = {
    val logger = getLogger(this.getClass.getName)
    val sc = getSparkContext()
    val graphrdd: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/Marvel-graph.txt")
    val superheroname: RDD[String] = sc.textFile("/Users/ammahajan/Personal/Spark-Practice/src/main/resources/Marvel-names.txt")

    val superheroprocessedrdd: RDD[(Int, String)] = superheroname.flatMap(line => {
      val values = line.split("\"")
      if (values.length > 1) {
        Some(values(0).trim.toInt,values(1))
      }
      else {
        None
      }
    })

    val superherograph: RDD[(Int, Int)] = graphrdd.map(line => {
      val arr = line.split("\\s+")
      (arr(0).toInt,arr.length-1)
    })


    val totalrdd: RDD[(Int, Int)] = superherograph.reduceByKey(_+_)

    //totalrdd.collect().foreach(println)
    val mostPopular: Int = totalrdd.map(x => {(x._2,x._1)}).max()._2
    println(mostPopular)
    println(superheroprocessedrdd.lookup(mostPopular)(0))

  }


}
