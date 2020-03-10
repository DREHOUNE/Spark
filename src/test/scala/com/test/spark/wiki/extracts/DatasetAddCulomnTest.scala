package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Dataset, SparkSession}
import com.test.spark.wiki.extracts.Implicits._
import org.scalatest.FlatSpec

class DatasetAddCulomnTest extends FlatSpec {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()


  "get column Sum" should "ok" in {

    //Given

    val inputData = spark.createDataset(Seq(bank(59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes")
      , bank(56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes")))

    val expected = spark.createDataset(Seq(Banka1(59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes",3385),
      Banka1(56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes",1512)))

    //When
    val result = DataSetExemple.getCulomnSum1(inputData)

    //Then
    assert(expected.collectAsList()==result.collectAsList())

  }

}
