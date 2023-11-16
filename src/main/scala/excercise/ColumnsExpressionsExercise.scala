package excercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object ColumnsExpressionsExercise extends App{

  val spark = SparkSession.builder()
    .appName("Columns Expressions Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  moviesDF.printSchema()

//  val newDF = moviesDF.select(
//    col("Title"),
//    col("Director"),
//    col("Rotten_Tomatoes_Rating")
//  )
  val newDF = moviesDF.select("Title","Director","Rotten_Tomatoes_Rating")

  newDF.show()

  val moviesWithNewColumnDF =  moviesDF.withColumn("Overall_gross",
    col("US_Gross") + col("Worldwide_Gross"))

  moviesWithNewColumnDF.show()

  val goodComedies = moviesDF.select("Title", "Release_Date", "IMDB_Rating")
    .filter("IMDB_Rating >= 6.0")
    .filter("Major_Genre = 'Comedy'")

  val goodComedies_2 = moviesDF.select("Title", "Release_Date", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  val goodComedies_3 = moviesDF.select("Title", "Release_Date", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  goodComedies.show()
  goodComedies_2.show()
  goodComedies_3.show()

}
