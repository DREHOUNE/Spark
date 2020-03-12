package com.test.spark.wiki.extracts.services

import com.test.spark.wiki.extracts.domain.DomainEncoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe._

object ServiceEDF {


  def apply(name:String): Unit ={
    println(s"Apply Called ${name}")
  }

  def readDataSet[T: TypeTag](inputfile: String)(implicit spark: SparkSession): Dataset[T] = {
    implicit val encoder = ExpressionEncoder[T]
    spark.read.json(inputfile).flatMap(_.copy(): Option[T])
  }

}

