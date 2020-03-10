package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FlatSpec
import com.test.spark.wiki.extracts.Implicits._

class DataSetExempleTest extends FlatSpec {

  val spark : SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()

  import spark.implicits._

  "get columns Sum(duration + balance) " should "ok" in {
    //Given
    val input : Dataset[bank]= Seq (
      (59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes"),
      (56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes"),
      (41,"technician","married","secondary","no",1270,"yes","no","unknown",5,"may",1389,1,-1,0,"unknown","yes"),
      (55,"services","married","secondary","no",2476,"yes","no","unknown",5,"may",579,1,-1,0,"unknown","yes"),
      (54,"admin.","married","tertiary","no",184,"no","no","unknown",5,"may",673,2,-1,0,"unknown","yes"),
      (42,"management","single","tertiary","no",0,"yes","yes","unknown",5,"may",562,2,-1,0,"unknown","yes")

    ).toDF("age","job","marital","education","f","balance","housing","loan","contact","day","month",
      "duration","campaign","pdays","previous","poutcome","deposit").as[bank]

    val expected: Dataset[Banka1] = Seq(
      (59,"admin.","married","secondary","no",2343,"yes","no","unknown",5,"may",1042,1,-1,0,"unknown","yes",3385),
      (56,"admin.","married","secondary","no",45,"no","no","unknown",5,"may",1467,1,-1,0,"unknown","yes",1512),
      (41,"technician","married","secondary","no",1270,"yes","no","unknown",5,"may",1389,1,-1,0,"unknown","yes",2659),
      (55,"services","married","secondary","no",2476,"yes","no","unknown",5,"may",579,1,-1,0,"unknown","yes",3055),
      (54,"admin.","married","tertiary","no",184,"no","no","unknown",5,"may",673,2,-1,0,"unknown","yes",857),
      (42,"management","single","tertiary","no",0,"yes","yes","unknown",5,"may",562,2,-1,0,"unknown","yes",562)

    ).toDF("age","job","marital","education","f","balance","housing","loan","contact","day","month",
      "duration","campaign","pdays","previous","poutcome","deposit","Sum").as[Banka1]

    //When
    val result: Dataset[Banka1] = DataSetExemple.getCulomnSum(input)
        result.show()

    //Then
    assert(expected.collectAsList()==result.collectAsList())
  }


}
