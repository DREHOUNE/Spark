package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.domain.{DomainEncoders, Installation, Team}
import com.test.spark.wiki.extracts.services.ServiceEDF
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.spark.sql.{Dataset, SparkSession}

object EdfApp {

  def main(args: Array[String]): Unit = {
    import DomainEncoders._

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MyApp.edf")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val pathTeam = "C:\\Users\\dreho\\Projects\\formation\\src\\main\\resources\\equipes.json"

    val pathI = "src/main/resources/installations.json"
    val pathR = "src/main/resources/regions.json"
    val pathE = "src/main/resources/equipes.json"
    val pathA = "src/main/resources/agences.json"

    val installationDS = ServiceEDF.filterDataSet(pathI, "INSTALLATION", "NUMERO INSTALLATION")
    val regionDS = ServiceEDF.filterDataSet(pathR, "REGION", "NOM REGION")
    val teamDS = ServiceEDF.filterDataSet(pathE, "TEAM", "NOM EQUIPE")
    val agencyDS = ServiceEDF.filterDataSet(pathA, "AGENCY", "NOM AGENCE")

    println("Installation ===>  ")


    val httpClient: CloseableHttpClient = HttpClients.createDefault()

    installationDS.map(ins => {

      val request: HttpGet = new HttpGet(s"https://www.google.com/search?q=${ins.name()}")
      httpClient.execute(request)


      ins.asInstanceOf[Installation]
    })


  }


}
