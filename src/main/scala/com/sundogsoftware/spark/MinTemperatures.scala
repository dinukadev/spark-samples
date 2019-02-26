package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.min
object MinTemperatures {

  def parseLines(line: String) = {
    val fields = line.split(",");
    val stationID = fields(0);
    val entryType = fields(2);
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature);
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Min temperatures")

    val lines = sc.textFile("src/main/resources/1800.csv")

    val parsedLines = lines.map(parseLines);

    val minTemps = parsedLines.filter(x => x._2=="TMIN")

    val stationTemps = minTemps.map(x=> (x._1,x._3.toFloat))

    val minTempByStation = stationTemps.reduceByKey((x,y)=> min(x,y))

    val results = minTempByStation.collect()

    for(res <- results){
      val station = res._1
      val temp = res._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}
