package pl.allegro.finance.sparkintro

import org.apache.spark.sql.Dataset
import org.scalatest.Matchers._

class WordCount extends SparkTest {
  //hint: group and count
  test("should count words correctly") {

    import spark.implicits._

    val data = spark.createDataset(Seq("Amelinium", "Amelinium", "Amelinium", "Yunkai", "Apollo"))

    val wordCount: Dataset[(String, Long)] = ???

    wordCount.collect.toMap should contain allOf("Amelinium" -> 3, "Yunkai" -> 1, "Apollo" -> 1)
  }

}
