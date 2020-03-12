package com.test.spark.wiki.extracts.services

import com.test.spark.wiki.extracts.domain.{Agency, DomainEncoders, Edf, Installation, Region, Team}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec


class ServiceEDFTest extends FlatSpec {
   implicit val spark = SparkSession.builder()
     .master("local[*]")
     .config("spark.testing.memory", "2147480000")
     .getOrCreate()

}
