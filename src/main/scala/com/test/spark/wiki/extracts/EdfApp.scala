package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.domain.{DomainEncoders, Installation, Team}
import com.test.spark.wiki.extracts.services.ServiceEDF
import org.apache.spark.sql.{Dataset, SparkSession}

object  EdfApp {

  def main(args: Array[String]): Unit = {
    import DomainEncoders._

    implicit val spark : SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp.edf")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val path = "src/main/resources/installations.json"

    val installationDS:Dataset[Installation] = ServiceEDF.filterDataSet[Installation](path,"NUMERO INSTALLATION")
    installationDS.show()
    val count = installationDS.count()
    println(s"Count = ${count}")

  }


}
