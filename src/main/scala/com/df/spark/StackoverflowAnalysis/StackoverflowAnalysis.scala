package com.df.spark.StackoverflowAnalysis

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.xml.XML

object StackoverflowAnalysis {
  /*1. Count the total number of questions in the available data-set and collect the questions id of all the questions*/
  def kpi1(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result=input.filter{line=>line.trim().startsWith("<row")}.filter{line=>line.contains("PostTypeId=\"1\"")}

    result.foreach(println)

    println("Total count %d".format(result.count()))
  }
  /*2. Monthly questions count – provide the distribution of number of questions asked per month*/
  def kpi2(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val format1=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val format2=new SimpleDateFormat("yyyy-MM")
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .flatMap {line => {
        val xml = XML.loadString(line)
        xml.attribute("CreationDate")
      }
      }
      .map { line => {
        (format2.format(format1.parse(line.toString())).toString(), 1)
      }
      }
      .reduceByKey(_ + _)

    result.foreach { println }
  }
/*3.Provide the number of posts which are questions and contains specified words in their title (like data, science, nosql, hadoop, spark)*/
  def kpi3(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .flatMap {line => {
        val xml = XML.loadString(line)
        xml.attribute("Title")
      }
      }
      .filter { line => {
        line.mkString.toLowerCase().contains("hadoop")
      }
      }

    result.foreach { println }
    println ("Result Count: " + result.count())
  }
/*4. The trending questions which are viewed and scored highly by the user – Top 10 highest viewed questions with specific tags*/
  def kpi4(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        val xml = XML.loadString(line)
        (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()), line)
      }
      }
      .sortByKey(false)

    result.take(10).foreach(println)
  }
/*5. The questions that doesn’t have any answers – Number of questions with “0” number of answers*/
  def kpi5(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        val xml = XML.loadString(line)
        (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
      }}
        .filter(x=>{x._1==0})
    println(result.count())
}
  /*6. Number of questions with more than 2 answers*/
  def kpi6(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        val xml = XML.loadString(line)
        (Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString()), line)
      }}
      .filter(x=>{x._1>2})
    println(result.count())
  }
  /*7. Number of questions which are active for last 6 months*/
  def kpi7(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        val xml = XML.loadString(line)
        (xml.attribute("CreationDate").get,  xml.attribute("LastActivityDate").get, line)
        //			  data._1                              data._2                              data._3
      }
      }
      .map{ data => {
        val crDate = format.parse(data._1.text)
        val crTime = crDate.getTime;

        val edDate = format.parse(data._2.text)
        val edTime = edDate.getTime;

        val timeDiff : Long = edTime - crTime
        (crDate, edDate, timeDiff, data._3)
        //			 data._1 data._2 data._3 data._4
      }
      }
      .filter { data => { data._3 / (1000 * 60 * 60 * 24) > 30*6}
      }

    result.foreach { println }
    println(result.count())
  }
/*Number of question with specific tags (nosql, big data) which was asked in the specified time range (from 01-01-2015 to 31-12-2015)*/
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val format2 = new SimpleDateFormat("yyyy-MM")
    val format3 = new SimpleDateFormat("yyyy-MM-dd")

    val startTime = format3.parse("2015-01-01").getTime
    val endTime = format3.parse("2015-01-31").getTime

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackoverflowAnalysis")

    val input = sc.textFile("data/Posts.xml")

    val result = input.filter{line => {line.trim().startsWith("<row")}
    }
      .filter { line => {line.contains("PostTypeId=\"1\"")}
      }
      .map {line => {
        val xml = XML.loadString(line)
        val crDate = xml.attribute("CreationDate").get.toString()
        val tags = xml.attribute("Tags").get.toString()
        //			  (closeDate, line)
        (crDate, tags, line)
      }
      }
      .filter{ data => {
        var flag = false
        val crTime = format.parse(data._1.toString()).getTime
        if (crTime > startTime && crTime < endTime && (data._2.toLowerCase().contains("bigdata") ||
          data._2.toLowerCase().contains("hadoop") || data._2.toLowerCase().contains("spark")))
          flag = true
        flag
      }
      }

    result.foreach { println }
    println(result.count())
  }

}
