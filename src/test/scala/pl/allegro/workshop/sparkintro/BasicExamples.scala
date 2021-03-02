package pl.allegro.workshop.sparkintro

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.avg

class BasicExamples extends SparkTest {

  test("should map and filter RDD") {

    val data = spark.sparkContext.parallelize(Seq("apple", "orange", "lemon", "lime"))

    val wordLengths: RDD[(String, Long)] = data
      .flatMap(word => Array(word, word * 2))
      .filter(word => word.contains("a"))
      .map(word => (word, word.length))

    println(wordLengths.collect().toList)

  }

  test("should map and filter Datasets") {

    import spark.implicits._

    val data = spark.createDataset(Seq("apple", "orange", "lemon", "lime"))

    val wordLengths: Dataset[(String, Long)] = data
      .flatMap(word => Array(word, word * 2))
      .filter(word => word.contains("a"))
      .map(word => (word, word.length))

    wordLengths.show(10)

  }

  test("should join datasets") {
    import spark.implicits._
    val locations = spark.createDataset(setupLocations())
    val occupations = spark.createDataset(setupOccupations())

    locations
      .joinWith(occupations, 'id === 'userId)
      .show(10)

  }

  test("should aggregate dataset") {
    import spark.implicits._

    val salaries = spark.createDataset(setupSalaries())

    salaries
      .agg(avg('salary))
      .select($"avg(salary)" alias "avgSalary")
      .show()
  }

  test("should repartition data") {
    import spark.implicits._

    val salaries = spark.createDataset(setupSalaries())
    salaries.repartition(numPartitions = 10, partitionExprs = $"salary")
  }

  def setupLocations(): Array[UserLocation] = {
    Array(
      UserLocation("1", "Poznan"),
      UserLocation("1", "Torun"),
      UserLocation("2", "Warsaw"),
      UserLocation("3", "Ney York"),
    )
  }

  def setupOccupations(): Array[UserOccupation] = {
    Array(
      UserOccupation("1", "Software Engineer"),
      UserOccupation("2", "Physiotherapist"),
      UserOccupation("4", "Teacher"),
    )
  }

  def setupSalaries(): Array[UserSalary] = {
    Array(
      UserSalary("1", 10000),
      UserSalary("2", 14000),
      UserSalary("4", 50000),
      UserSalary("5", 2000),
      UserSalary("5", 8000),
    )
  }
}

case class UserLocation(id: String, city: String)
case class UserOccupation(userId: String, occupation: String)
case class UserSalary(userId: String, salary: Int)


