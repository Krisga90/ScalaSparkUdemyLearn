package excercise
import lesson_2_dataFrames.DataSources.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
object DataSourcesExercise extends App{
  /**
   * read the movies DF, then rewrite it as
   * - tab- separeted values file
   * - snappy Parquet
   * - table "public.movies in the Postgress DB
   */

  val spark = SparkSession.builder()
    .appName("Data Sources Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("path", "src/main/resources/data/movies.csv")
    .option("sep", "\t")
    .save()

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")


  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()



}
