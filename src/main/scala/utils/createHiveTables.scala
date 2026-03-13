package utils

import org.apache.spark.sql.SparkSession
import utils.sparkSessionBuilder.getSparkSession


object createHiveTables {

  def createCafeSalesTable(spark: SparkSession): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS Demo")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS Demo.tbl_CafeSales (
        Transaction_ID String,
        Item String,
        Quantity Int,
        Price_Per_Unit Double,
        Total_Spent Double,
        Payment_Method String,
        Location String,
        Transaction_Date String
      ) STORED AS PARQUET
    """)
}
  //LOCATION 'src/main/resources/spark-warehouse/cafeSales.parquet'

}