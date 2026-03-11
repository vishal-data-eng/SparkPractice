package utils

import org.apache.spark.sql.SparkSession

object sparkSessionBuilder{

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Spark practice")
    .master("local[*]")
    .getOrCreate()

}