package lessson_3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  /**
   * select the first non-null value
   */

  val moviesNoNullsDF = moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Rating")
  )

  moviesNoNullsDF.show()
  //Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  //nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove nulls
  moviesDF.select("Title", "IMDB_Rating")
    .na.drop() //remove rows containing nulls

  //replace nulls with 0 from columns "IMDB_Rating", "Rotten_Tomatoes_Rating"
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  /**
   * //replace nulls with
   * 0 for "IMDB_Rating",
   * 10 for "Rotten_Tomatoes_Rating"
   * "Unknown" for "Director"
   */

  moviesDF.na.fill(Map(
  "IMDB_Rating" -> 0,
  "Rotten_Tomatoes_Rating" -> 10,
  "Director" -> "Unknown"
  ))

  //complex operation
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", //same as upper line
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",  //returns null if two values ar EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)" // if (first != null) second else third
  ).show()


}
