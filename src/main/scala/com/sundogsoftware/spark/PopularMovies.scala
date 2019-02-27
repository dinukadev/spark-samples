package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PopularMovies {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Popular Movies")

    val input = sc.textFile("src/main/resources/u.data")

    val data = input.map(x=> (x.split("\t")(1),1))

    val movieCount = data.reduceByKey((x,y)=> x+y)

    val countMovie = movieCount.map(x=> (x._2,x._1))

    val movieCountSorted = countMovie.sortByKey()

    val result = movieCountSorted.collect()

    result.foreach(println)

  }
}
