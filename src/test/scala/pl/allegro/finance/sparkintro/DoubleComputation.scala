package pl.allegro.finance.sparkintro
import org.scalatest.Matchers._

class DoubleComputation extends SparkTest {

  test("should run computation once") {
    import spark.implicits._

    val acc = spark.sparkContext.collectionAccumulator[Int]

    val numbers = spark.sparkContext.parallelize(1 to 5).toDS()
    val plusOne = numbers.map ( x => { acc.add(x); x + 1 } )

    plusOne.map ( x => x + 2 ).count()
    plusOne.map ( x => x + 10 ).count()

    acc.value.size() should be(5)

  }
}
