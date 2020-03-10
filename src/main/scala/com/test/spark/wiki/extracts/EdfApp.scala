package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.domain.{DomainEncoders, Team}
import com.test.spark.wiki.extracts.services.ServiceEDF
import org.apache.spark.sql.SparkSession

object  EdfApp {

  def main(args: Array[String]): Unit = {
    import DomainEncoders._

    implicit val spark : SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp.edf")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val pathTeam = "C:\\Users\\dreho\\Projects\\formation\\src\\main\\resources\\equipes.json"

    val path = "src/main/resources/installations.json"

    val installationDS = ServiceEDF.filterDataSet(path,"INSTALLATION","NUMERO INSTALLATION")
    installationDS.show()

  }


}
