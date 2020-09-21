package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object CustomerAmountExercise {
  def parseLine(line:String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toFloat
    (customerId,amount)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerAmountExercise")

    // Read each line of customer dataset into an RDD
    val input = sc.textFile("data/customer-orders.csv")

    // Break the amount for each customer
    val rdd = input.map(parseLine)

    // Add up the amount for each customer
    val totalAmountRdd = rdd.reduceByKey((x,y)=>(x+y))

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = totalAmountRdd.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)

  }
}
