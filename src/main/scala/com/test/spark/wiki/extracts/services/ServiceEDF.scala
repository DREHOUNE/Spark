package com.test.spark.wiki.extracts.services

import com.test.spark.wiki.extracts.domain.{Agency, DomainEncoders, Edf, Installation, Region, Team}
import org.apache.spark
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe._
import DomainEncoders._

object ServiceEDF {


  def filterDataSet[T: TypeTag](inputfile: String, toExclude: String)(implicit spark: SparkSession): Dataset[T] = {

    implicit val encoder = ExpressionEncoder[T]

    spark.read.json(inputfile).flatMap(_.copy(): Option[T])

  }

}

