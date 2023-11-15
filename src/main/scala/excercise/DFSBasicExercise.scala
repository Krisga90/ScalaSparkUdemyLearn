package excercise
import lesson_2_dataFrames.DataFrameBasics.spark
import org.apache.spark.sql.{Row, SparkSession}

object DFSBasicExercise extends App{

  /**
   * 1. Create a manual DF describing smartphones
   *
   * 2. Read file from the data movies.json
   *  - print schema
   *  - count the number of rows, "count()"
   *
   */

  // create new sesion
  val spark =SparkSession.builder()
    .appName("DFSBasicExercise")
    .config("spark.master", "local")
    .getOrCreate()

  val smartphones =  Seq(
    ("Samsung", "GalaxyS8", 11, 12),
    ("Iphone", "S8", 10, 15),
    ("Xiaomi", "Mi-9SE", 9, 10)
  )
  import spark.implicits._

  val smartphonesDF = smartphones.toDF(
    "make", "model", "screen dimension", "camera Mpx"
  )

  smartphonesDF.show()
  smartphonesDF.printSchema()


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())


}
