package com.test.spark.wiki.extracts.domain

import org.apache.spark.sql.{Encoders, Row}

import scala.util.Try

sealed trait  Edf {
  def name():String
}
case class Team(codeAgence: String, code: String, override val name: String) extends Edf
case class Agency(code: String, codeRegion: String, override val name: String) extends Edf
case class Region(code: String, name: String) extends Edf
case class Installation( codeEquipe: String, code: Int, cofelyVision: String, libelle: String, name: String) extends Edf
case class JointInstallEquipe(codeAgence: String, code: String, override val name: String, codeEquipe: String, code: Int, cofelyVision: String, libelle: String, name: String) extends Edf

object DomainEncoders {
  implicit val encdTeam = Encoders.product[Team]
  implicit val encdAgency = Encoders.product[Agency]
  implicit val encodInstallation = Encoders.product[Installation]
  implicit val encodRegion = Encoders.product[Region]

  implicit def rowToTeam (r: Row): Team = {
    Team(r.getString(0), r.getString(1), r.getString(2))
  }

  implicit def rowToAgency (r: Row): Agency = {
    Agency(r.getString(0), r.getString(1), r.getString(2))
  }
  implicit def rowToRegion (r: Row): Region = {
    Region(r.getString(0), r.getString(1))
  }

  implicit def rowToInstallation (r: Row): Installation = {

     val codeInstall = Try(r.getString(1).toInt).getOrElse(Int.MinValue)
     Installation(r.getString(0), codeInstall , r.getString(2), r.getString(3), r.getString(4))

  }


}