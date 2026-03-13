package pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.sparkSessionBuilder.getSparkSession
import org.apache.spark.sql.functions._
import utils.createHiveTables.createCafeSalesTable



object cafeDataCleaning {

  def getSourceData(spark: SparkSession): Map[String, DataFrame] = {

    val cafeMenu: DataFrame = spark.read.option("header", "true")
      .csv("src/main/resources/cafe_menu.csv")

    val cafeSales: DataFrame = spark.read.option("header", "true")
      .csv("src/main/resources/dirty_cafe_sales.csv")

    Map("cafeMenu"-> cafeMenu, "cafeSales"-> cafeSales)
  }


  def transformData(sourceData: Map[String, DataFrame]): DataFrame = {

    val cafeMenu = sourceData("cafeMenu")
    val cafeSales = sourceData("cafeSales")

    cafeSales.show()
    println(s"Total records: ${cafeSales.count()}")

    //Check for items which are not part of menu
    cafeSales.join(cafeMenu, "Item", "left_anti").select(col("Item")).distinct().show

    //Check for non-integer values in quantity column
    cafeSales.filter(
      col("Quantity").isNotNull &&
        col("Quantity").cast("int").isNull
    ).select("Quantity").distinct().show()

    //Check for price per unit which is not as per menu
    cafeSales.join(
      cafeMenu,
      cafeSales("Price Per Unit") === cafeMenu("Price($)"),
      "left_anti"
    ).select(col("Price Per Unit")).distinct().show()

    //Check for the different payment methods
    cafeSales.select(col("Payment Method")).distinct().show()


    //Checking for missing/dirty values
    val dirtyValues = Seq("UNKNOWN", "ERROR")

    cafeSales.columns.foreach { colName =>
      val dirtyCount = cafeSales.filter(
        col(colName).isNull ||
          col(colName) === "" ||
          col(colName).isin(dirtyValues: _*)
      ).count()

      println(s"Column $colName has $dirtyCount null/empty values")
    }


    //Filling missing Price Per Unit from the menu
    val filledPriceDF = cafeSales.join(cafeMenu, "Item", "left")
      .withColumn("Price Per Unit",
        coalesce(when(col("Price Per Unit").isin(dirtyValues: _*), lit(null)).otherwise(col("Price Per Unit")), col("Price($)")))
      .drop("Price($)")

    //Filling missing Quantity with mean value
    val avgQuantityDf = filledPriceDF.agg(mean("Quantity").alias("Avg_Quantity")).first().getDouble(0).round.toInt
    val filledQuantityDF = filledPriceDF
      .withColumn("Quantity", when(col("Quantity").isin(dirtyValues: _*), lit(null)).otherwise(col("Quantity")))
      .na.fill(Map("Quantity" -> avgQuantityDf))

    //Filling missing Total Spent value by calculating Quantity * Price Per Unit
    val filledTotalSpentDf = filledQuantityDF
      .withColumn("Total Spent", when(col("Total Spent").isin(dirtyValues: _*) || col("Total Spent").isNull, null).otherwise(col("Total Spent")))
      .withColumn("Total Spent", when(col("Total Spent").isNull, col("Quantity") * col("Price Per Unit")).otherwise(col("Total Spent")))

    //Filling missing Payment Method as Cash
    val CleanedDf = filledTotalSpentDf
      .withColumn("Payment Method", when(col("Payment Method").isin(dirtyValues: _*) || col("Payment Method").isNull, null).otherwise(col("Payment Method")))
      .na.fill(Map("Payment Method" -> "Cash"))

    CleanedDf
  }


  def loadData(transformedData: DataFrame): Unit = {

    /*createCafeSalesTable(spark)

    val loadDF = transformedData.select(
      col("Transaction ID").alias("Transaction_ID"),
      col("Item").alias("Item"),
      col("Quantity").alias("Quantity").cast("Integer"),
      col("Price Per Unit").alias("Price_Per_Unit").cast("Double"),
      col("Total Spent").alias("Total_Spent").cast("Double"),
      col("Payment Method").alias("Payment_Method"),
      col("Location").alias("Location"),
      col("Transaction Date").alias("Transaction_Date")
    ) */

    transformedData.write.mode("overwrite").parquet("target/outputData/CafeSales")
  }


  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()

    val sourceData = getSourceData(spark)

    val transformedData = transformData(sourceData)

    loadData(transformedData)

    spark.stop()
  }

}