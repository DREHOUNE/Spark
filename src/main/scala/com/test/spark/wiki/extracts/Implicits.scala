package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Encoder, Encoders}

object Implicits {
 implicit val encoder1 = Encoders.product[bank]
 implicit val enco2= Encoders.product[zip]
 implicit val enco3= Encoders.product[Test]
 implicit val enco4= Encoders.product[Banka1]



}
