package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularSuperHero {

  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None
    }
  }

  def countCoOccurences(line: String) = {
    val elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperHero")

    val names = sc.textFile("src/main/resources/Marvel-names.txt")

    val namesRdd = names.flatMap(parseNames)

    var lines = sc.textFile("src/main/resources/Marvel-graph.txt")

    val pairings = lines.map(countCoOccurences)

    val totalFriendsByCharacter = pairings.reduceByKey((x,y)=>x+y)

    val flipped = totalFriendsByCharacter.map(x=> (x._2,x._1))

    val mostPopular = flipped.max()

    val mostPopularName = namesRdd.lookup(mostPopular._2)

    println(s"$mostPopularName is the most powerful superhero with ${mostPopular._1} co-appearances")
  }

}
