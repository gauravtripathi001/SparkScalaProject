package com.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long

object wbi {

//  /** Convert input data to (Urban populcation,Country) tuples */
//  def extractPopCountryPairs(line: String): (Long,String) = {
//    val fields = line.split(",")
//    val uPopulation=fields(10).replaceAll(",", "")
//    var uPopNum = 0L
//    if (uPopulation.length() > 0) uPopNum = uPopulation.toLong
//
//    (uPopNum, fields(0))
//  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "wbi")

    val data = sc.textFile("data/World_Bank_Indicators.csv")

//    val mappedInput=input.map(extractPopCountryPairs)
//
//    val result=mappedInput.sortByKey(false)
//
//    result.foreach(println)

    val result = data.map { line => {
      val fields = line.split(",")
//      val uPopulation = fields(10).replaceAll(",", "")
//      var uPopNum = 0L
//      if (uPopulation.length() > 0)
//        uPopNum = Long.parseLong(uPopulation)

      //(uPopNum, fields(0))
      (fields.length)
    }}
//      .sortByKey(false)
//      .take(1)

    result.foreach(println)

  }

}
