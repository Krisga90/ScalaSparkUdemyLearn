package lesson_2_dataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
object ColumnsExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select method
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    expr("Year"),
    expr("Horsepower"),
    expr("Origin"),
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightDF.show()

  //selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2"
  )

  // adding a column
  val carsWithKg_3 =  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs")/ 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
//  carsWithColumnRenamed.show()

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")
  carsWithColumnRenamed.show()

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filter with expression string
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") >  150)
  val americanPowerfulCarsDF2  = carsDF.filter(col("Origin") === "USA" and col("Horsepower") >  150)
  val americanPowerfulCarsDF3  = carsDF.filter("Origin = 'USA' and Horsepower >  150")
  americanPowerfulCarsDF3.show()

  //unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) //works if DF have same schema

  // distant values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
}
