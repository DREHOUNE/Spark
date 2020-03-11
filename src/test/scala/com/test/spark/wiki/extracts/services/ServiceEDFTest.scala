package com.test.spark.wiki.extracts.services

import com.test.spark.wiki.extracts.domain.{Agency, DomainEncoders, Edf, Installation, Region, Team}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec


class ServiceEDFTest extends FlatSpec {
   implicit val spark = SparkSession.builder()
     .master("local[*]")
     .config("spark.testing.memory", "2147480000")
     .getOrCreate()

  import DomainEncoders._

  "get dataset from Json equipes " should "filter the first wrong line" in {

    //Given
    val inputfile = "src/test/resources/equipes.json"

    val expected = spark.createDataset(Seq(
      Team("EAR", "JM3", "Eq GIE STADE DE FRANCE"),
      Team("HPH", "HPS", "EPEX ARCELOR - IBM - OLYMPE"),
      Team("HPH", "HNA", "EPEX ARCHIVES GROUPE")))

    //when
    val result= ServiceEDF.filterDataSet(inputfile, "TEAM", "NOM EQUIPE")

    //Then
    assert(expected.collect().sameElements(result.collect()) )

  }

  "get dataset from Json agences" should "filter the first wrong line" in {

    //Given
    val inputfile = "src/test/resources/agences.json"

    val expected = spark.createDataset(Seq(
      Agency("EAR", "1A0", "GIE Multiservices Stade de France"),
      Agency("HPH", "1A0", "ESEIS GEDOC")))

    //when
    val result= ServiceEDF.filterDataSet(inputfile, "AGENCY", "NOM AGENCE")
    result.show()

    //Then
    assert(expected.collect().sameElements(result.collect()) )

  }

  "get dataset from Json installation" should "filter the first wrong line" in {

    //Given
    val inputfile = "src/test/resources/installations.json"

    val expected = spark.createDataset(Seq(
      Installation("JM3",199675,"N","STADEFRANCE","5460901001")
    ))


    //when
    val result= ServiceEDF.filterDataSet(inputfile, "INSTALLATION", "NUMERO INSTALLATION")
    result.show()

    //Then
    assert(expected.collect().sameElements(result.collect()) )

  }


  "get dataset from Json Region " should "filter the first wrong line" in {

    //Given
    val inputfile = "src/test/resources/regions.json"

    val expected = spark.createDataset(Seq(
      Region("1A0","COFELY Services Facilities Solutions")
    ))


    //when
    val result= ServiceEDF.filterDataSet(inputfile, "REGION", "NOM REGION")
    result.show()

    //Then
    assert(expected.collect().sameElements(result.collect()) )

  }

  "get join installation and equipe" should "ok" in {

    //Given
    val inputfileEquipe = "src/test/resources/installations.json"
    val inputfileInstallation = "src/test/resources/equipes.json"
    val installationDS = ServiceEDF.filterDataSet(inputfileInstallation, "INSTALLATION", "NUMERO INSTALLATION")
    val equipeDS = ServiceEDF.filterDataSet(inputfileEquipe, "TEAM", "NOM EQUIPE")

    val expected =

    //When

    //Then




  }


}
