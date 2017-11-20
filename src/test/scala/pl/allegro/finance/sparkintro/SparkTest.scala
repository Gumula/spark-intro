package pl.allegro.finance.sparkintro

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._

class SparkTest extends FunSuite with BeforeAndAfter {
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


  after {
    spark.sparkContext.stop()
  }

}

object Util {
  val dateTimeFormat: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("EEE MMM d HH:mm:ss ")
    .appendOffset("+HHMM", "+0000")
    .appendLiteral(" ")
    .appendText(ChronoField.YEAR)
    .toFormatter

  def toWordsList(text: String): Array[String] = {
    text.replaceAll("\\<[^>]*>", "")
      .replaceAllLiterally("!", "")
      .replaceAllLiterally(",", "")
      .replaceAllLiterally(".", "")
      .replaceAllLiterally("\"", "")
      .replaceAllLiterally("&amp;", "")
      .toLowerCase
      .split(" ")
      .filter(str => str.length > 2)
      .filter(str => !Stopwords.stopwords.contains(str.toLowerCase))
  }

  def extractDevice(source: String): String = {
    ">(.+?)<".r.findFirstIn(source).getOrElse("").replaceAll("<", "").replaceAll(">", "")
  }
}
