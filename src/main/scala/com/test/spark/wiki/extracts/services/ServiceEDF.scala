package com.test.spark.wiki.extracts.services

import com.test.spark.wiki.extracts.domain.{Agency, DomainEncoders, Edf, Installation, Region, Team}
import org.apache.spark.sql.{Dataset, SparkSession}

object ServiceEDF {


  def filterDataSet(inputfile: String, aType: String, toExclude: String)(implicit spark: SparkSession): Dataset[_ <: Edf] = {
    import DomainEncoders._

    aType match {
      case "AGENCY" => {
        spark.read.json(inputfile)
          .map(_.copy(): Agency)
          .filter(t => !toExclude.equals(t.name))
      }
      case "TEAM" => {
        spark.read.json(inputfile)
          .map(_.copy(): Team)
          .filter(t => !toExclude.equals(t.name))
      }
      case "INSTALLATION" => {
        spark.read.json(inputfile)
          .map(_.copy(): Installation)
          .filter(t => !toExclude.equals(t.name))
      }

      case "REGION" => {
        spark.read.json(inputfile)
          .map(_.copy(): Region)
          .filter(t => !toExclude.equals(t.name))
      }

  }

}

}

