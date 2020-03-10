package com.test.spark.wiki.extracts
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object  CreatDataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.testing.memory", "2147480000")

      .appName("MyFirstApp")
      .getOrCreate()


    val df = spark.read.option("header", "true").csv("src/main/resources/bank.csv")
    df.show()
  }
}



