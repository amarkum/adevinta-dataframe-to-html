package com.adevinta.htmlconverter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, trim}

import scala.collection.mutable
import scala.io.Source

object HTMLConverter {

  /**
   * This function accepts the metafile location and converts into Map of column - Start location and length of value
   * @param fileName
   * @return
   */
  def getFileSchema(fileName: String): mutable.LinkedHashMap[String, List[Int]] = {
    var mapBuffer: mutable.LinkedHashMap[String, List[Int]] = mutable.LinkedHashMap()
    try {
      for (line <- Source.fromFile(fileName).getLines()) {
        val splitLine = line.split(",").map(_.trim).toList
        mapBuffer = mapBuffer ++ Map(splitLine.head -> splitLine.tail.map(_.toInt))
      }
    } catch {
      case ex: Exception => println(ex)
    }
    mapBuffer
  }

  /**
   * This method accepts dataframe object and sparkSession and converts the dataframe into HTML page.
   * @param dataframe
   * @param sparkSession
   */
  def convertDataFrameToHTML(dataframe : DataFrame, sparkSession: SparkSession) ={
    var headerHTML = "<html><table><tr>"
    dataframe.columns.foreach(col=>{headerHTML += "<th>"+ col+ "</th>"})
    headerHTML += "</tr>"

    val headers = sparkSession.sparkContext.parallelize(Seq(headerHTML))
    val footers = sparkSession.sparkContext.parallelize(Seq("</table></html>"))

    val htmlDataframe = dataframe.select(concat(lit("<tr><td>"), concat_ws("</td><td>", dataframe.columns.map(c => col(c)): _*), lit("</td></tr>")).alias(headerHTML))

    val htmlFile = headers ++ htmlDataframe.rdd.map(x => x.mkString("")) ++ footers
    htmlFile.coalesce(1).saveAsTextFile("src/main/resources/output/html")

  }
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Adevinta Assignment")
      .getOrCreate()

    // Parse PRN file to Dataframe object
    var dataframe = sparkSession.read.text("src/main/resources/data/prn/*")
    val header = dataframe.first()
    dataframe = dataframe.filter(row => row != header)

    // get PRM Meta Map
    val maps = getFileSchema("src/main/resources/prnMeta")

    // Extract value from the start Index and length
    maps.foreach { KV => dataframe = dataframe.withColumn(KV._1, dataframe.col("value").substr(KV._2.head,KV._2.last)) }

    // Remove whitespace at start and end  from the column
    dataframe.schema.fields.foreach(f=>{
      dataframe=dataframe.withColumn(f.name,trim(col(f.name)))
    })
    dataframe = dataframe.drop(dataframe("value"))

    // Comment below to use CSV location to convert into Dataframe object
    // var dataframe = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("src/main/resources/data/csv/*")

    // View the sample Dataframe
    dataframe.show(10)

    // Convert dataframe into HTML
    convertDataFrameToHTML(dataframe, sparkSession)
  }

}
