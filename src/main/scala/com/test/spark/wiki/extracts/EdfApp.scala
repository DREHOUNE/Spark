package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.domain.{Agency, DomainEncoders, Installation, Region, Team}
import com.test.spark.wiki.extracts.services.ServiceEDF
import org.apache.spark.sql.{Dataset, SparkSession}



case class A(name:String)
class B(name:String)
object  EdfApp {

  def main(args: Array[String]): Unit = {
    import DomainEncoders._

    implicit val spark : SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp.edf")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val a = A("sqsqs")
    val b = new B("sqsqs")


  }


}
