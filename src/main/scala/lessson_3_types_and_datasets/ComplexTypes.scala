package lessson_3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App{

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

//  spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  /**
   * Dates
   */

  val moviesWithReleaseDatesDF = moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")
  ).withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"),col("Actual_Release"))/365)
    .drop("Today")

  moviesWithReleaseDatesDF.select("*").where(col("Actual_Release").isNull)
//    .show()


  /**
   * Structures:
   * 1. with column operators
   * 2. with expression string
   */


  // 1.
  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  )
    .select(
      col("Title"),
      col("Profit"),
      col("Profit").getField("US_Gross").as("US_Profit"))
//    .show()

  // 2.
    moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as  Profit")
      .selectExpr("Title", "Profit.US_Gross")

  /**
   * Arrays
   */

  val moviesWithWordsDF =
    moviesDF.select(
      col("Title"),
      split(col("Title"), " |,").as("Title_Words")
    ) // Array of strings

  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love").as("Love")
  ).where(col("Love"))
    .drop(col("Love"))
    .show()


}
