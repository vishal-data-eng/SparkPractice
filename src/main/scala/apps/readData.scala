package apps

import utils.sparkSessionBuilder.spark
import org.apache.spark.sql.functions._

object readData {

  def main(args: Array[String]): Unit = {

    //Reading CSV data
    val custDf = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/customers.csv")

    println("CSV Data:")
    custDf.show()

    //Selecting specific columns
    val custDf1 = custDf.select("customer_id", "gender", "age", "city")
    custDf1.show()

    // Filtering customers older than 40
    val seniorCust = custDf1.filter(col("age") > 40)
    seniorCust.show()

    //Adding new column
    val dfWithAgeGroup = custDf1.withColumn("age_group",
        when(col("age") < 30, "Young")
       .when(col("age") < 50, "Adult")
       .otherwise("Senior"))
    dfWithAgeGroup.show()

    //Aggregations
    custDf1.groupBy("gender").agg(avg("age").cast("int").alias("avg_age")).show()

    // Oldest customers first
    custDf1.orderBy(col("age").desc).show()

    //Reading parquet data
    val cityDf = spark.read.parquet("src/main/resources/cities.parquet")

    println("Parquet Data:")
    cityDf.show()

    println("Spark job finished. Spark UI is still alive.")
    //Thread.sleep(10 * 60 * 1000)

    spark.stop()
  }

}