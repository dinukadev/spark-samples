package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local","Word Count")

    val input = sc.textFile("src/main/resources/book.txt")

    val words = input.flatMap(x=> x.split("\\W+"))

    val lowerCaseWords = words.map(x=> x.toLowerCase())

    val wordCount = lowerCaseWords.map(x=> (x,1)).reduceByKey((x,y)=> x+y)

    val wordsSorted = wordCount.map(x=> (x._2,x._1)).sortByKey()

    for(result <- wordsSorted){
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}
