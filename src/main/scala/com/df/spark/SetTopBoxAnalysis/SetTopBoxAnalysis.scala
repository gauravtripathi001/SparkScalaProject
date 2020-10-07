package com.df.spark.SetTopBoxAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.xml.XML
import java.lang.Float

//The objective of the project is to analyze set top box data and generate insights.
//The data contains details about users’ activities like tuning a channel, duration, browsing for videos, purchase video using VOD (video on demand), etc.
// Process the data using Spark and solve the problem statement listed in KPIs sections
object SetTopBoxAnalysis {
  def extractListOfFields(line: String): (String, String, String, String, String, String, String) = {
    val fields = line.split("\\^")

    var serverUniqueId = ""
    if (fields.length > 0)
      serverUniqueId = fields(0)

    var requestType = ""
    if (fields.length > 1)
      requestType = fields(1)

    var eventId = ""
    if (fields.length > 2)
      eventId = fields(2)

    var timestamp = ""
    if (fields.length > 3)
      timestamp = fields(3)

    var xml = ""
    if (fields.length > 4)
      xml = fields(4)

    var deviceId = ""
    if (fields.length > 5)
      deviceId = fields(5)

    var secondaryTimestamp = ""
    if (fields.length > 6)
      secondaryTimestamp = fields(6)

    (serverUniqueId, requestType, eventId, timestamp, xml, deviceId, secondaryTimestamp)
    //(fields.length)
  }

  //  1. Filter all the record with event_id=100
  //  i. Get the top five devices with maximum duration
  def kpi1i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => rec._3.equals("100"))
      .map { rec => {
        var durationValue = 0
        var body = rec._5
        var dvId = rec._6
        val xml = XML.loadString(body)
        for (nv <- xml.child) {
          val oneNV = XML.loadString(nv.toString())
          val oneNVName = oneNV.attribute("n").fold("")(_.toString)
          val oneNVValue = oneNV.attribute("v")
          if (oneNVName == "Duration") durationValue = Integer.parseInt(oneNVValue.getOrElse(0).toString)
          //println(oneNVName, oneNVValue)
        }
        (dvId, durationValue)

      }
      }
      .groupByKey()
      .map(rec => (rec._1, rec._2.max))
      .sortBy(rec => (rec._2), false)
      .take(5)


    result.foreach(println)

  }

  //  1. Filter all the record with event_id=100
  //  ii. Get the top five Channels with maximum duration
  def kpi1ii(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => rec._3.equals("100"))
      .map { rec => {
        var durationValue = 0
        var channelNumber = 0
        var body = rec._5
        val xml = XML.loadString(body)
        for (nv <- xml.child) {
          val oneNV = XML.loadString(nv.toString())
          val oneNVName = oneNV.attribute("n").fold("")(_.toString)
          val oneNVValue = oneNV.attribute("v")
          if (oneNVName == "Duration") durationValue = Integer.parseInt(oneNVValue.getOrElse(0).toString)
          if (oneNVName == "ChannelNumber") channelNumber = Integer.parseInt(oneNVValue.getOrElse(0).toString)

          //println(oneNVName, oneNVValue)
        }
        (channelNumber, durationValue)

      }
      }
      .groupByKey()
      .map(rec => (rec._1, rec._2.max))
      .sortBy(rec => (rec._2), false)
      .take(5)
    result.foreach(println)


  }

  //  1. Filter all the record with event_id=100
  //iii. Total number of devices with ChannelType="LiveTVMediaChannel"
  def kpi1iii(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => (rec._3.equals("100") & rec._5.contains("LiveTVMediaChannel")))

    println(result.count())
  }

  //2. Filter all the record with event_id=101
  //i. Get the total number of devices with PowerState="On/Off"
  def kpi2i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => (rec._3.equals("101") & rec._5.contains("PowerState")))

    println(result.count())
  }

  //  3. Filter all the record with Event 102/113
  //  i. Get the maximum price group by offer_id
  def kpi3i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => (rec._3.equals("102") | rec._3.equals("113")))
      .map { rec => {
        var offerId = ""
        var price = 0f
        var channelNumber = 0
        var body = rec._5
        val xml = XML.loadString(body)
        for (nv <- xml.child) {
          val oneNV = XML.loadString(nv.toString())
          val oneNVName = oneNV.attribute("n").fold("")(_.toString)
          val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
          //println(oneNV.attribute("v"))
          if (oneNVName == "OfferId")
            offerId = oneNVValue
          if (oneNVName == "Price" & oneNVValue != "") {
            price = oneNVValue.toFloat
          }

          //println(oneNVName, oneNVValue)
        }
        (offerId, price)

      }
      }
      .groupByKey()
      .map(rec => (rec._1, rec._2.max))
      .sortBy(rec => (rec._2), false)
      .take(5)
    result.foreach(println)
  }

  //  4. Filter all the record with event_id=118
  //  i. Get the min and maximum duration
  def kpi4i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)

      .filter(rec => rec._3.equals("118"))
      .map { rec => {
        var durationValue = 0
        var body = rec._5

        val xml = XML.loadString(body)
        for (nv <- xml.child) {
          val oneNV = XML.loadString(nv.toString())
          val oneNVName = oneNV.attribute("n").fold("")(_.toString)
          val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
          //println("oneNVValue :"+oneNVValue)
          if (oneNVName == "DurationSecs" && oneNVValue != "") {
            durationValue = Integer.parseInt(oneNVValue)
          }

        }
        (durationValue)

      }
      }

    println(result.max)
    println(result.min)


  }

  //  5. Filter all the record with Event 0
  //  i. Calculate how many junk records are thier having BadBlocks in xml column
  def kpi5i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)
      .filter(rec => rec._3.equals("0"))
      .filter(rec=>rec._5.contains("BadBlocks"))

    println(result.count())
  }

//  6. Filter all the record with Event 107
//  i. group all the ButtonName with thier device_ids
def kpi6i(args: Array[String]): Unit = {
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

  val data = sc.textFile("data/Set_Top_Box_Data.txt")

  val result = data.map(extractListOfFields)

    .filter(rec => rec._3.equals("107"))
    .map { rec => {
      var deviceId=rec._6
      var buttonName = "NULL"
      var body = rec._5

      val xml = XML.loadString(body)
      for (nv <- xml.child) {
        val oneNV = XML.loadString(nv.toString())
        val oneNVName = oneNV.attribute("n").fold("")(_.toString)
        val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
        //println("oneNVValue :"+oneNVValue)
        if (oneNVName == "ButtonName") {
          //println(oneNVValue)
          buttonName = oneNVValue
        }

      }
      (buttonName,deviceId)

    }
    }
    .groupByKey()

  result.foreach(println)
}
//  7. Filter all the record with Event 115/118
//  i. Get the duration group by program_id
  def kpi7i(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

    val data = sc.textFile("data/Set_Top_Box_Data.txt")

    val result = data.map(extractListOfFields)

      .filter(rec => (rec._3.equals("115"))|(rec._3.equals("118")))
      .filter(rec=>rec._5.contains("<d>"))
      .map { rec => {
        var programId = "NULL"
        var body = rec._5
        var durationValue=0

        val xml = XML.loadString(body)
        for (nv <- xml.child) {
          val oneNV = XML.loadString(nv.toString())
          val oneNVName = oneNV.attribute("n").fold("")(_.toString)
          val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
          //println("oneNVValue :"+oneNVValue)
          if (oneNVName == "ProgramId") {
            //println(oneNVValue)
            programId = oneNVValue
          }
          if (oneNVName == "DurationSecs") {
            durationValue = Integer.parseInt(oneNVValue)
          }

        }
        (programId,durationValue)

      }
      }
      .groupByKey()

    result.foreach(println)
  }
//  7. Filter all the record with Event 115/118
//    ii. Total number of devices with frequency="Once"
def main(args: Array[String]): Unit = {
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "SetTopBoxAnalysis")

  val data = sc.textFile("data/Set_Top_Box_Data.txt")

  val result = data.map(extractListOfFields)

    .filter(rec => (rec._3.equals("115"))|(rec._3.equals("118")))
    .filter(rec=>rec._5.contains("<d>"))
    .map { rec => {
      var deviceId = rec._6
      var body = rec._5
      var frequencyValue="NULL"

      val xml = XML.loadString(body)
      for (nv <- xml.child) {
        val oneNV = XML.loadString(nv.toString())
        val oneNVName = oneNV.attribute("n").fold("")(_.toString)
        val oneNVValue = oneNV.attribute("v").fold("")(_.toString)

        if (oneNVName == "Frequency") {
          frequencyValue = oneNVValue
        }

      }
      (deviceId,frequencyValue)

    }
    }
    .filter(x => (x._2 =="Once"))
    .map( rec => (rec._1, 1))
    .reduceByKey(_+_)
    .sortBy(rec => (rec._2), false)
  //.take(5)

  result.foreach(println)
  println("Total number of devices with frequency=Once : " + result.count())
}
}
