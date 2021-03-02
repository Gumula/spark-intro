package pl.allegro.workshop.sparkintro

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkTest extends AnyFunSuite with BeforeAndAfter {
  val spark: SparkSession = createSession

  private def createSession = {
    val conf = new SparkConf()
      .setAppName(ConfigFactory.load().getString("spark.app.name"))
      .setMaster(ConfigFactory.load().getString("spark.master"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }

}


