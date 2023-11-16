package excercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, countDistinct, stddev, sum}
object AggregationExercise extends App {

  /**
   * 1 Sum up All the profits of All the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standart deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  val spark = SparkSession.builder()
    .appName("Aggregation Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF
    .selectExpr("sum(US_Gross + Worldwide_Gross + US_DVD_Sales) as Total_Income")
    .show()

  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Income"))
    .select(sum("Total_Income"))
    .show()

  val distinctDirectories = moviesDF
    .select(countDistinct(col("Director")))
  distinctDirectories.show()

  val avgAndDevUSGross = moviesDF
    .select(
      avg("US_Gross").as("Average"),
      stddev("US_Gross").as("stddev")
    )
  avgAndDevUSGross.show()

  val directorStats = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Average_IMDB_Rating"),
      avg("US_Gross").as("Average_US_Gross"),
      sum("US_Gross").as("Sum_US_Gross"),
    )
    .orderBy(col("Average_IMDB_Rating").desc_nulls_last)

  directorStats.show()








}
