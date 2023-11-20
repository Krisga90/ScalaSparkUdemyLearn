package excercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object ComplexTypesExercise extends App{

  val spark = SparkSession.builder()
    .appName("Complex Date Types Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  /**
   * Exercise
   * 1. How do we deal with multiple date formats
   * -parse DF multiple times, then union the small DFs
   * 2. Read the stock DF and parse the dates
   */

  val stockDF = spark.read
    .option("inferSchema", "true")
    .option("sep", ",")
    .option("header", "true")
    .option("nullValue", "") // empty string is understand as nullValue
    .csv("src/main/resources/data/stocks.csv")

  stockDF.show()

  val stockWithDateDF = stockDF.select(
    col("symbol"),
    to_date(col("date"), "MMM dd yyyy").as("Actual Date")
  )
//
  stockWithDateDF.show()
  stockWithDateDF.printSchema()

}
