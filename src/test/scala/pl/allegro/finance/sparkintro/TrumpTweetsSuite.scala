package pl.allegro.finance.sparkintro

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.scalalang.typed
import org.scalatest.Matchers._

class TrumpTweetsSuite extends SparkTest {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  private val tweets = spark.read.json("src/main/resources/trump.json").as[Tweet].cache()

  //hint: sort and map
  //you can access the column like this: $"retweet_count"
  test("should get the id, created_at, retweet_count, text of the 5 most retweeted tweets") {

    val mostRetweeted: Array[(Long, String, Long, String)] = ???

    mostRetweeted should contain theSameElementsInOrderAs List(
      (795954831718498305L, "Tue Nov 08 11:43:14 +0000 2016", 348986L, "TODAY WE MAKE AMERICA GREAT AGAIN!"),
      (796315640307060738L, "Wed Nov 09 11:36:58 +0000 2016", 224084L, "Such a beautiful and important evening! The forgotten man and woman will never be forgotten again. We will all come together as never before"),
      (741007091947556864L, "Thu Jun 09 20:40:32 +0000 2016", 167284L, "How long did it take your staff of 823 people to think that up--and where are your 33,000 emails that you deleted? https://t.co/gECLNtQizQ"),
      (815185071317676033L, "Sat Dec 31 13:17:21 +0000 2016", 130715L, "Happy New Year to all, including to my many enemies and those who have fought me and lost so badly they just don't know what to do. Love!"),
      (755788382618390529L, "Wed Jul 20 15:36:06 +0000 2016", 119732L, "The media is spending more time doing a forensic analysis of Melania's speech than the FBI spent on Hillary's emails.")
    )

  }

  //hint: use created_at field of Tweet
  //parse the date using Util.dateTimeFormat
  test("should count tweets by month") {

    val grouped: Dataset[(String, Long)] = ???

    grouped.collect() should contain allOf(
      ("2016-02", 44L),
      ("2016-12", 137L),
      ("2016-09", 296L),
      ("2016-03", 441L),
      ("2016-10", 531L),
      ("2016-05", 350L),
      ("2016-04", 283L),
      ("2016-11", 193L),
      ("2016-07", 358L),
      ("2016-06", 303L),
      ("2016-08", 283L),
      ("2017-01", 6L)
    )
  }

  //hint: first group tweets by source
  //then count how many tweets there are per device
  //then find all tweets that contain the word 'crooked' (flatMapValues can help)
  //then count them (typed.sum can help)
  //then join the 2 datasets
  //key for 1st table can be accessed by $"_1.value", similarly for second table
  //then compute the ratio by dividing featured words count by total device count and multiplying the result by 100 to get percent values
  //extract device name with Util.extractDeviceName(source)
  // sort by second column $"_2" desc
  test("should compute ratio of 'crooked' word per device sorted desc") {
    val grouped = ???

    val counts = ???

    val crookedRatio: Dataset[(String, Double)] = ???


    crookedRatio.collect() should contain theSameElementsInOrderAs List(
      ("Twitter for Android", 12.114537444933921),
      ("Twitter for iPad", 9.090909090909092),
      ("Twitter Web Client", 7.971014492753623),
      ("Twitter for iPhone", 5.384615384615385),
      ("Media Studio", 0.0),
      ("Instagram", 0.0),
      ("Periscope", 0.0),
      ("Twitter Ads", 0.0)
    )

  }
//hint: implement mostPopular
  test("should get most popular words from android and iphone") {
    val android = tweets.filter(tweet => tweet.source == "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>")
    val iphone = tweets.filter(tweet => tweet.source == "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>")

    val mostPopularAndroid = mostPopular(android).take(20)
    val mostPopularIphone = mostPopular(iphone).take(20)

    println(s"iPhone: ${mostPopularIphone.mkString("\n")}")
    println(s"\nAndroid: ${mostPopularAndroid.mkString("\n")}")

    mostPopularAndroid.head should equal(("hillary",241))
    mostPopularIphone.head should equal(("thank",402))
  }

  //hint: extract words list from text with Util.toWordsList
  // flatMap will be useful
  // group and aggregate
  // count the words with typed.count[String]
  //you should rename the aggregated column to make it sortable
  def mostPopular(dataset: Dataset[Tweet]): Dataset[(String, Long)] = {
    ???
  }

}
