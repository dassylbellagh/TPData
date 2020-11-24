package services

import java.util.logging.{Level, Logger}

import config.ConfigParser
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}
import reader.{ConfigReader, SparkReaderWriter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

object Service1 {

  def implemService1(df : DataFrame) : DataFrame= {
    println("Start Service 1 Enlever le client 292494523 ")

    var resService1 = df.filter(col("ClientID") =!= 292494523)
    println("nombre des clients avant suppression")
    println(df.count())

    println("nombre des clients apres suppression")
    println(resService1.count())

    return resService1
  }

}
