package pl.allegro.workshop.sparkintro

import org.apache.spark.storage.StorageLevel
import org.scalatest.matchers.should.Matchers._

class DoubleComputation extends SparkTest {

  test("should run computation twice") {
    import spark.implicits._

    val acc = spark.sparkContext.longAccumulator

    val numbers = spark.sparkContext.parallelize(Array(1)).toDS()
    val mapped = numbers.map ( x => { acc.add(1); x } )

    mapped.collect()
    mapped.collect()

    acc.value should be(2)
  }

  test("should run computation once") {
    import spark.implicits._

    val acc = spark.sparkContext.longAccumulator

    val numbers = spark.sparkContext.parallelize(Array(1)).toDS()
    val mapped = numbers.map ( x => { acc.add(1); x } ).cache()

    mapped.collect()
    mapped.collect()

    acc.value should be(1)
  }
}
