package pl.allegro.workshop.sparkintro

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers._

class WordCount extends SparkTest {

  test("should count words correctly") {

    import spark.implicits._

    val data = spark.createDataset(Seq("apple apple", "apple orange lemon"))

    val wordCount: Dataset[(String, Long)] = data
      .flatMap(line => line.split(" "))
      .groupByKey(word => word)
      .count()

    wordCount.show()

    wordCount.collect.toMap should contain allOf("apple" -> 3, "orange" -> 1, "lemon" -> 1)
  }

}
