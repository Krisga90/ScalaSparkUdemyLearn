package excercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object JoinExercise extends App{
  /**
   * - show all employees and their max salary
   * - show all employees who were never managers
   * - find job titles of the best paid 10 employees in the company
   */
  
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Join Exercise")
    .getOrCreate()

  private val driver = "org.postgresql.Driver"
  private val url = "jdbc:postgresql://localhost:5432/rtjvm"
  private val user = "docker"
  private val password = "docker"
  /**
   * Reading from a remote DB
   */


  def readTable(tableName: String) =  spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val managersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

//  employeesDF.show()
//  salariesDF.show()


  val maxSalaryPerEmployeeNumber = salariesDF.groupBy("emp_no").max("salary")
  val employeeBestSalaryDF = employeesDF.join(maxSalaryPerEmployeeNumber, "emp_no")


  employeeBestSalaryDF.show()

  val neverManagerDF = employeesDF.join(managersDF,
    employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti")
  neverManagerDF.show()

  val highestPaidJobTitles = employeeBestSalaryDF
    .join(titlesDF, "emp_no")
    .select("title", "max(salary)")
    .orderBy(col("max(salary)").desc_nulls_last)

    highestPaidJobTitles.show(10)
}
