package lessson_3_types_and_datasets

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.sql.Date

object DataSets extends App{
  val spark = SparkSession.builder()
    .appName("Data Sets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  //convert a DF to a Dataset
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]



  // dataset of a complex type
  //1. define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  def readDF(filename: String) =
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")

  // 2. read the DF from the file
  val carsDF = readDF("cars.json")

  // 3 -  define an encoder (importing the implicities)
  import spark.implicits._
  //  implicit val carEncoder = Encoders.product[Car]
  // 4. convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

}
