package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.Implicits._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object DataSetExemple {
  def getCulomnSum1(input : Dataset[bank]): Dataset[Banka1]= {
    input.withColumn("Sum", col("duration") + col("balance")).as[Banka1]
  }

  def getCulomnSum(input: Dataset[bank]): Dataset[Banka1] = {
    input.withColumn("Sum",col("duration") + col("balance")).as[Banka1]

  }

}
