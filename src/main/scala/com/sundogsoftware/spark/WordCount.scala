package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Word Count")

    val input = sc.textFile("src/main/resources/book.txt")

    val words = input.flatMap(x=> x.split(" "))

    val wordCount = words.countByValue()

    wordCount.foreach(println)
  }
}
