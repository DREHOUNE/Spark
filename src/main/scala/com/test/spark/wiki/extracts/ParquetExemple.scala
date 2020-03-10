package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import com.test.spark.wiki.extracts.Implicits._

object ParquetExemple {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExemples.com")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

//    val data = Seq(("James ","","Smith","36636","M",3000),
//      ("Michael ","Rose","","40288","M",4000),
//      ("Robert ","","Williams","42114","M",4000),
//      ("Maria ","Anne","Jones","39192","F",4000),
//      ("Jen","Mary","Brown","","F",-1)
//    )
//
//    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
//
//    import spark.sqlContext.implicits._

//    val df = data.toDF(columns:_*)
//    df.show()
//    df.printSchema()

    //df.write.parquet("src/main/resources/people.parquet")

    //val parqDF = spark.read.parquet("src/main/resources/people.parquet")
    //parqDF.show()

    //df.write.mode("append").parquet("src/main/resources/people.parquet")

    spark.read.parquet("src/main/resources/zipcodes.parquet").as[zip].show()
  }

}
