package lessson_3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{
  val spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRaitingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRaitingFilter
  moviesDF.select("Title").where(preferredFilter).show()

  val moviesWithGoodnessFlagDF = moviesDF
    .select(col("Title"), preferredFilter.as("good_movie"))

  //filer on a boolean column
  moviesWithGoodnessFlagDF.where("good_movie").show() //where(col("good_movie") === "true")

  //negation
  moviesWithGoodnessFlagDF.where(not(col("good_movie"))).show()

  //Numbers
  val moviesAvgRatingDF = moviesDF.select(col("Title"),
    ((col("Rotten_Tomatoes_RAting")/ 10 + col("IMDB_Rating"))/2).as("Avg_Rating"))

  println(moviesDF.stat.corr("Rotten_Tomatoes_RAting", "IMDB_Rating"))  //corr is an action (run imidietly)

//  moviesAvgRatingDF.show()

  // String
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  /**
   * capitalization
   * initcap
   * lower
   * upper
   */

  carsDF.select(initcap(col("Name"))).show()

  /**
   * contains
   */
  carsDF.select("*").where(col("Name").contains("volkswagen")).show()

  /**
   * regex
   */
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()

  /**
   * Exercise
   * Filter the cars DF by a list of car names obtained by an API call
   */

  def getCarNames: List[String] = List("Volkswagen", "Vw", "Mercedes-Benz", "Ford")

  // version 1 regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("complex_extract")
  ).where(col("complex_extract") =!= "")
    .drop("complex_extract")
    .show()

  //version 2
  var carNameFilters = getCarNames.map(_.toLowerCase())
    .map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()


}
