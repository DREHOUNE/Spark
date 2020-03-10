package com.test.spark.wiki.extracts

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object Formation {
  def getTopClientMarried(input: DataFrame): DataFrame = {
    input.filter("marital == 'married'").sort(desc("age"))
  }


}
