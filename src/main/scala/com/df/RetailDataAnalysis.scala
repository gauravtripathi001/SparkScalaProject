package com.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Calculate sales breakdown by product category across all of the stores. */
object RetailDataAnalysis {

  /** Convert input data to (Product-Cat,Sale-Value) tuples */
  def extractProductSalePairs(line: String): (String, Float) = {
    val fields = line.split("\t")
    (fields(3), fields(4).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "RetailDataAnalysis")


    val input = sc.textFile("data/Retail_Sample_Data_Set.txt")

    val mappedInput = input.map(extractProductSalePairs)

    val saleByProduct = mappedInput.reduceByKey((x, y) => x + y)


    val results = saleByProduct.collect()


    // Print the results.
    results.foreach(println)
  }

}
