package utils

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object sparkSessionBuilder{

  def getSparkSession(): SparkSession = {

    val spark = SparkSession.builder()
      .appName("Spark practice")
      .master("local[*]")
      //Hive support
      .config("spark.sql.warehouse.dir", "/src/main/resources/spark-warehouse")
      .enableHiveSupport()
      // Iceberg catalog
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.type", "hadoop")
      .config("spark.sql.catalog.iceberg.warehouse", "/src/main/resources/iceberg-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}


