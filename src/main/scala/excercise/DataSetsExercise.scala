package excercise

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSetsExercise extends App {
  /**
   * cont how many cars we have
   * count how many powerful cars we have (HP > 140)
   * average HP
   */

  val spark = SparkSession.builder()
    .appName("Data Sets Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  def getDF(fileName: String):DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }

  val carsDF = getDF("cars.json")
  import spark.implicits._

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

  val carsDS = carsDF.as[Car]

  // 1.
  val carsCount = carsDS.count()
  println(carsCount)
  // 2.
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println(carsDS.filter(_.Horsepower.exists(_ > 140)).count()) //becaus option
  // 3.
  val validHorsepowerCars = carsDS.filter(_.Horsepower.isDefined)
  val totalHorsepower = validHorsepowerCars.map(_.Horsepower.get).reduce(_ + _)
  println(totalHorsepower / validHorsepowerCars.count())
  println(carsDS.select(avg(col("Horsepower"))))





}
