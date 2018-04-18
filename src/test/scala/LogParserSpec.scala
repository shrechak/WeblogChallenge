package parser

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

case class TestCase(timestamp: String, clientPort: String, request: String)

class LogParserSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Serializable{
  var spark: SparkSession = _
  lazy val sqlContext = spark.sqlContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.appName("test").master("local").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  "LogParser" should "validate the UDFs" in {
    val input = List(TestCase("2015-07-22T09:00:27.894580Z", "10.0.4.150:80", "GET https://paytm.com HTTP/1.1"))
    getTestInput(input)
      .select("dateMillis", "clientIp", "url")
      .collect()
      .map{
        r: Row => (r.getLong(0), r.getString(1), r.getString(2))
      } should be (Array((1437555627894L,"10.0.4.150", "https://paytm.com")))
  }

  "LogParser" should "generate new session ids and backfill session ids for when duration remains the same" in {
    val input = List(TestCase("2015-07-22T09:00:27.894580Z", "10.0.4.150:80", "GET https://paytm.com HTTP/1.1"),
      TestCase("2015-07-22T09:03:27.894580Z", "10.0.4.150:80", "GET https://paytm.com HTTP/1.1"),
      TestCase("2015-07-22T09:30:27.894580Z", "10.0.4.150:80", "GET https://paytm.com HTTP/1.1"))

    val testDataset = getTestInput(input)
    LogParser.addSessionId(testDataset, 15 * 60 * 1000)
      .select("sessionId")
      .distinct()
      .collect()
      .size should be (2)
  }

  private def getTestInput(input: List[TestCase]) = {
    sqlContext.createDataFrame(input)
      .withColumn("dateMillis", LogParser.dateToMillis(col("timestamp")))
      .withColumn("clientIp", LogParser.clientIP(col("clientPort")))
      .withColumn("url", LogParser.urlFromRequest(col("request")))
  }
}
