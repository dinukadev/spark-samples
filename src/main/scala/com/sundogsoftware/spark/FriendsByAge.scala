package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numOfFriends = fields(3).toInt
    (age,numOfFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("prg").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","FriendsByAge");

    val lines = sc.textFile("src/main/resources/fakefriends.csv")

    val rdd = lines.map(parseLine);

    val totalByAge = rdd.mapValues(x=> (x,1)).reduceByKey((x,y)=> (x._1+y._1,x._2+y._2))

    val averageByAge = totalByAge.mapValues(x=> x._1/x._2);

    val results = averageByAge.collect();

    results.sorted.foreach(println)
  }
}
