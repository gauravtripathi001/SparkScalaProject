package com.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountDataFlair {


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of my book into an RDD
    val data = sc.textFile("data/book.txt")

    val result = data.flatMap { line => {
      line.split(" ")

      /*
       * DataFlair is the leading Training Provider of Big Data Technologies
       * DataFlair
       * is
       * the
       * leading
       * Training
       * Provider
       * of
       * Big
       * Data
       * Technologies
       */
    }
    }
      .map { words => (words, 1)
        /*
         * DataFlair, 1
         * is, 1
         * the, 1
         * leading, 1
         * Training, 1
         * Provider, 1
         * of, 1
         * Big, 1
         * Data, 1
         * Technologies, 1
         */

      }
      .reduceByKey(_ + _)
    /*
     * DataFlair	[1 1 1 1 1 1..........]		=>		DataFlair, 369
     * Training		[1 1 1 1 1 1..........]		=>		Training, 760
     */
    result.foreach(println)


  }

}
