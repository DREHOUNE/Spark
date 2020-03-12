package com.test.spark.wiki.extracts.domain

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Encoders, Row}
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.util.Try
import scala.reflect.runtime.universe._
import scala.util.parsing.json.JSONObject


sealed trait  Edf{
  def name():String
}

//case class Team(codeAgence: String, code: String,  override val name: String) extends Edf
//case class Agency(code: String, codeRegion: String, override val    name: String) extends Edf
//case class Region(code: String, override val  name: String) extends Edf

case class Installation(@JsonProperty("code_equipe") codeEquipe: String,
                        @JsonProperty("code_installation")  code: Int,
                        @JsonProperty("cofely_vision") cofelyVision: String,
                        @JsonProperty("libelle_installation") libelle: String,
                        @JsonProperty("nom_installation") name: String) extends Edf

case class Team(@JsonProperty("code_agence") codeAgence: String,
                @JsonProperty("code_equipe")  code: String,
                @JsonProperty("nom_equipe") name: String) extends Edf

case class Agency(@JsonProperty("code_agence") code: String,
                  @JsonProperty("code_region")  codeRegion: String,
                  @JsonProperty("nom_agence") name: String) extends Edf

case class Region(@JsonProperty("code_region") code: String,
                  @JsonProperty("nom_region")  name: String) extends Edf

object DomainEncoders {
  implicit val encdTeam = Encoders.product[Team]
  implicit val encdAgency = Encoders.product[Agency]
  implicit val encodInstallation = Encoders.product[Installation]
  implicit val encodRegion = Encoders.product[Region]

  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  implicit def rowToGeneric[T: TypeTag](r: Row): Option[T] = {
    val mirror = ScalaReflection.mirror
    val tpe = typeTag[T].in(mirror).tpe
    val cls = mirror.runtimeClass(tpe)
    val json = convertRowToJSON(r)

    Try(mapper.readValue(json, cls.asInstanceOf[Class[T]])).toOption

  }

  implicit class filterDataset[T](ds: Dataset[T]) {
         def filterWithCodeBegin(s: String) = ds.filter(elt => true)
     }

}