package pipelines

import org.apache.spark.sql.functions._
import utils.sparkSessionBuilder.getSparkSession

object dataPartitioning {

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()
    import spark.implicits._

    val data = (1 to 1000000).map(i => (i % 10, i))
    val df = data.toDF("key", "value")

    df.groupBy("key").count().show()

    println(s"Number of partitions: ${df.rdd.getNumPartitions}")

    //println(df.groupBy("key").count().explain(true))

    val repartitionedDF = df.repartition(20)
    repartitionedDF.groupBy("key").count().show()

    println(s"Number of partitions: ${repartitionedDF.rdd.getNumPartitions}")

    val coalesceDF = repartitionedDF.coalesce(5)
    coalesceDF.groupBy("key").count().show()

    println(s"Number of partitions: ${coalesceDF.rdd.getNumPartitions}")

    //Skewness
    val skewedDF = (1 to 1000000).map { i => if (i < 900000) (0, i) else (i % 10, i)}.toDF("key", "value")

    skewedDF.groupBy("key").count().show()

    val saltedDF = skewedDF.withColumn("saltedKey", (rand() * 100).cast("int"))

    val saltedAgg = saltedDF.groupBy(col("key"), col("saltedKey"))
                            .count()
                            .groupBy("key")
                            .sum("count")

    saltedAgg.show()
  }


}