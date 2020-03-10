package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import com.test.spark.wiki.extracts.Implicits._

object CreateDataSet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MyFirstApp")
      .master("local[*]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val BankE = spark.read.option("header", "true").csv("src/main/resources/bank.csv").map(elt => bank(elt.getString(0).toInt,elt.getString(1),
      elt.getString(2), elt.getString(3),
      elt.getString(4), elt.getString(5).toInt, elt.getString(6),
      elt.getString(7), elt.getString(8), elt.getString(9).toInt,
      elt.getString(10), elt.getString(11).toInt, elt.getString(12).toInt,elt.getString(13).toInt,
      elt.getString(14).toInt,elt.getString(15),elt.getString(16)))

   //BankE.show()

    BankE.createOrReplaceTempView("bank")
    spark.sql("""
                 |
                 |SELECT age, marital
                 |FROM bank
                 |WHERE marital = 'single'
                 |ORDER BY age DESC
                 |""".stripMargin
    )//.show(5)


    BankE.createOrReplaceTempView("bank1")
    spark.sql("""
                 |
                 |SELECT *
                 |FROM bank1
                 |
                 |
                 |""".stripMargin).show(5)
  }

}
