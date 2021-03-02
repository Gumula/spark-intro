package pl.allegro.workshop.sparkintro

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.Locale

object Util {
  val dateTimeFormat: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("EEE MMM d HH:mm:ss ")
    .appendOffset("+HHMM", "+0000")
    .appendLiteral(" ")
    .appendText(ChronoField.YEAR)
    .toFormatter
    .withLocale(Locale.US)

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
