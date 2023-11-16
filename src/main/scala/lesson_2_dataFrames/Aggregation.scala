package lesson_2_dataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}
object Aggregation extends App{

  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))  // all values except null
  moviesDF.selectExpr("count(Major_Genre)")
  // count all
  moviesDF.select(count("*")).show()  // count all includes null

  // count distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // aproximate count
  moviesDF.select(approx_count_distinct("Major_Genre"))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)").show()

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)").show()

  // average
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))  // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  val averageByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("Rotten_Tomatoes_Rating")

  averageByGenreDF.show()

  val aggregation = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("Rotten_Tomatoes_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")
  aggregation.show()
}
