package lesson_2_dataFrames

import lesson_2_dataFrames.DataFrameBasics.{carsDFSchema, spark}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
object DataSources extends App{

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )
  /**
   * Reading a DF:
   * - format
   * - schema or inferSchema = true
   * - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") //dropMalformed (ignore), permissive(default), failFast (exception)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()

//  val carstDF = spark.read
//    .format("json")
//    .option("inferSchema", "true") //Dont Use in production "inferSchema"
//    .load("src/main/resources/data/cars.json")

  val carstDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast", //dropMalformed (ignore), permissive(default), failFast (exception)
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()


  /**
   * Writing DF !!!!!
   * - format
   * - save mode = overwrite, append, ignore, errorIfExists
   * - path
   * - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_duplicate.json")
    .save()

//  carsDF.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars_duplicate.json")

  spark.read
//    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") //couple with schema; if spark fail parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")  // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars_duplicate.json")

  /**
   * CSV flags
   * -
   */
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
//    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignore first line in file as it is header
    .option("sep", ",") // separator
    .option("nullValue", "") // empty string is understand as nullValue
    .csv("src/main/resources/data/stocks.csv")

  /**
   * Parquet
   */
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars_duplicate.parquet")
    //.save("src/main/resources/data/cars_duplicate.parquet") //parquete is default save format


  /**
   * textFile
   */
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt").show()

  /**
   * Reading from a remote DB
   */
  val employees = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employees.show()

}
