package lesson_2_dataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
object Joins extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Joins")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("infermSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .option("infermSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("infermSchema", "true")
    .json("src/main/resources/data/bands.json")


  // inner joins
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristDF
    .join(bandsDF, joinCondition, "inner")

  guitaristBandsDF.show()

  /**
   * outer joins
   * left_outer = everything in the inner jooin + all the rows in the LEFT table,
   * with nulls in where the data is missing
   */

  guitaristDF.join(bandsDF, joinCondition, "left_outer").show()

  /**
   * outer joins
   * right_outer = everything in the inner jooin + all the rows in the RIGHT table,
   * with nulls in where the data is missing
   */

  guitaristDF.join(bandsDF, joinCondition, "right_outer").show()

  /**
   * outer joins
   * outer = everything in the inner jooin + all the rows in the Both table,
   * with nulls in where the data is missing
   */
  guitaristDF.join(bandsDF, joinCondition, "outer").show()


  /**
   * semi-join
   * will be shown date from left table only but only the one that woul;d be shown in inner_join
   */

  guitaristDF.join(bandsDF, joinCondition, "left_semi").show()


  /**
   * anti-join
   * will be shown date from left table only but only the one that wouldn't be shown in inner_join
   */

  guitaristDF.join(bandsDF, joinCondition, "left_anti").show()

  /**
   * Now we have two columns named id in our guitaristBandsDF.
   * If we try to call column by "id" guitaristBandsDF.secet("id", ..).show()
   * program will crash
   * to fix it we need to raneme 1 column
   */

  // 1. rename the column on which we are joining
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // 2. drop the dupe column
  guitaristBandsDF.drop(bandsDF.col("id"))

  // 3. rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristDF.join(guitaristDF.withColumnRenamed("id",
    "guitarId"),
    expr("array_contains(guitars, guitarId)"))



}
