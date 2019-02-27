package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrderSpend {

  def parseInput(lines: String) = {
    val fields = lines.split(",")
    (fields(0).toInt,fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","CustomerOrderSpend")
    val input = sc.textFile("src/main/resources/customer-orders.csv")

    val customerSpend = input.map(parseInput)

    val totalCustomerSpend = customerSpend.reduceByKey((x,y)=> x+y).sortByKey()

    val results = totalCustomerSpend.collect()

    results.foreach(println)
  }
}
