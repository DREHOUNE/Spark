package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec

class FormationTest extends FlatSpec {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.testing.memory", "2147480000")
      .appName("MyFirstApp")
      .getOrCreate()

  import spark.implicits._


  "get the Top5 of married" should "ok" in  {
    //Given
    val input: DataFrame = Seq(
      (59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes"),
      (56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes"),
      (41,"technician","married","secondary","no",1270,"yes","no","unknown",5,"may",1389,1,-1,0,"unknown","yes"),
      (55,"services","married","secondary","no",2476,"yes","no","unknown",5,"may",579,1,-1,0,"unknown","yes"),
      (54,"admin.","married","tertiary","no",184,"no","no","unknown",5,"may",673,2,-1,0,"unknown","yes"),
      (42,"management","single","tertiary","no",0,"yes","yes","unknown",5,"may",562,2,-1,0,"unknown","yes")
    )
      .toDF("age", "job","marital", "education","default",
        "balance","housing", "loan","contact", "day","month", "duration",
        "campaign", "pdays","previous", "poutcome","deposit")

    val expected: DataFrame = Seq(
      (59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes"),
      (56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes"),
      (55,"services","married","secondary","no",2476,"yes","no","unknown",5,"may",579,1,-1,0,"unknown","yes"),
      (54,"admin.","married","tertiary","no",184,"no","no","unknown",5,"may",673,2,-1,0,"unknown","yes"),
      (41,"technician","married","secondary","no",1270,"yes","no","unknown",5,"may",1389,1,-1,0,"unknown","yes")
    )
      .toDF("age", "job","marital", "education","default",
        "balance","housing", "loan","contact", "day","month", "duration",
        "campaign", "pdays","previous", "poutcome","deposit")

    //When
    val result: DataFrame = Formation.getTopClientMarried(input)
    result.show()


    //Then
    assert(expected.collectAsList() == result.collectAsList())
  }
}


