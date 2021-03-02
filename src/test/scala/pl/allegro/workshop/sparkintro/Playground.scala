package pl.allegro.workshop.sparkintro

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{YearMonth, ZonedDateTime}
import java.util.Locale

import org.scalatest.funsuite.AnyFunSuite

class Playground extends AnyFunSuite {
  test("playground") {
    //Locale.setDefault(Locale.)
    val dateTimeFormat: DateTimeFormatter = new DateTimeFormatterBuilder()
      .parseLenient()
      .appendPattern("EEE MMM d HH:mm:ss ")
      .appendOffset("+HHMM", "+0000")
      .appendLiteral(" ")
      .appendText(ChronoField.YEAR)
      .toFormatter
      .withLocale(Locale.US)

    print(dateTimeFormat.toString)

    val parsed = YearMonth.from(ZonedDateTime.parse("Sun Jan 01 06:49:49 +0000 2017", dateTimeFormat)).toString
    print(parsed)
    assert(true)
  }

}
