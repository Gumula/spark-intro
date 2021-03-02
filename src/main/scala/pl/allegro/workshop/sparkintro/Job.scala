package pl.allegro.workshop.sparkintro

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.util.Random

object Job extends App {
  val spark: SparkSession = createSession

  import spark.implicits._

  val locations = createUserLocations(Range(1, 2000000), spark)
  val salaries = createUserSalaries(Range(500000, 2000000), spark)

  val notFromParis: Dataset[UserLocation] = locations
    .map(userLoc => userLoc.copy(location = userLoc.location.toUpperCase))
    .filter(_.location != "PARIS").cache

  notFromParis
    .joinWith(salaries, 'userId === 'id, "left")
    .map { case (userLoc, userSalary) => toUserInfo(userLoc, userSalary)}
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .csv("output/")


  private def toUserInfo(userLoc: UserLocation, userSalary: UserSalary) = {
    Thread.sleep(10000000L)
    UserInfo(userLoc.userId, userLoc.location, Option(userSalary).map(_.salary).getOrElse(0))
  }

  private def createSession = {
    val conf = new SparkConf()
      .setAppName(ConfigFactory.load().getString("spark.app.name"))
      .setMaster(ConfigFactory.load().getString("spark.master"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.password", "IMPORTANT, SHOULD NOT BE VIEWED!!!")

    SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }

  def createUserLocations(range: Range, spark: SparkSession): Dataset[UserLocation] = {
    val cities = List("new york", "warsaw", "paris", "poznan", "krakow", "san francisco")
    val random = new Random

    import spark.implicits._

    val userLocs: Array[UserLocation] = range.map(i => UserLocation(i.toString, cities(random.nextInt(cities.length)))).toArray

    spark.createDataset(userLocs)
  }

  def createUserSalaries(range: Range, spark: SparkSession): Dataset[UserSalary] = {
    val random = new Random

    import spark.implicits._

    val userSalaries: Array[UserSalary] = range.map(i => UserSalary(i.toString, random.nextInt(20000))).toArray

    spark.createDataset(userSalaries)
  }

}

case class UserLocation(userId: String, location: String)

case class UserSalary(id: String, salary: Int)

case class UserInfo(id: String, location: String, salary: Int)
