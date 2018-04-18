package parser

import com.twitter.scalding.Args
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.format.ISODateTimeFormat

trait DateTimeParser {
  val parser = ISODateTimeFormat.dateTimeParser

  def getMillis(date: String) = {
    val dateTimeHere = parser.parseDateTime(date)
    dateTimeHere.getMillis
  }
}

object LogParser extends App with DateTimeParser {
  val cmdArgs = Args(args)
  val input = cmdArgs.getOrElse("input", "data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
  val output = cmdArgs.getOrElse("output", "/tmp/output-spark/")
  val timeWindow = cmdArgs.getOrElse("window-minutes", "15")

  //define spark session
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("WebLogParser")
    .getOrCreate()

  import spark.implicits._

  val dateToMillis = udf { date: String => getMillis(date) }
  val clientIP = udf { ipPortString: String => ipPortString.split(":").head }
  val urlFromRequest = udf { request: String => request.split(" ")(1) }

  val logs = spark.read
    .option("delimiter", " ")
    .csv(input)
    .toDF("timestamp", "elb", "client:port", "backend:port", "request_processing_time", "backend_processing_time",
      "response_processing_time", "elb_status_code", "backend_status_code", "received_bytes", "sent_bytes",
      "request", "user_agent", "ssl_cipher", "ssl_protocol")
    .withColumn("dateMillis", dateToMillis($"timestamp"))
    .withColumn("clientIp", clientIP($"client:port"))
    .withColumn("url", urlFromRequest($"request"))

  val duration = timeWindow.toInt*60*1000.0

  //1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  val byClientIp = Window.partitionBy($"clientIp").orderBy("dateMillis")

  val sessions = logs
    .withColumn("oldDateMillis", lag($"dateMillis", 1, 0).over(byClientIp))
    //detecting when there is a new session in a client ip partition
    .withColumn("newSession",
      when(($"dateMillis" - $"oldDateMillis") > duration, floor(rand()*1000)))
    ///backfilling session ids for cases when the session didnt change
    .withColumn("filledSessionIds",
      last($"newSession", true).over(Window.partitionBy($"clientIp").orderBy("dateMillis")))
    //generating final session Id
    .withColumn("sessionId", concat_ws("_", $"clientIp", lit("SESS"), $"filledSessionIds"))
    .cache()

  sessions
    .select("sessionId", "timestamp", "elb", "client:port", "backend:port", "request_processing_time", "backend_processing_time",
      "response_processing_time", "elb_status_code", "backend_status_code", "received_bytes", "sent_bytes",
      "request", "user_agent", "ssl_cipher", "ssl_protocol")
    .write.csv(output+"/1/")

  //2. Determine the average session time

  val bySessionId = Window.partitionBy($"sessionId").orderBy("dateMillis")
  def getSessionSize  = udf { millisList: Seq[Long] => millisList.max - millisList.min}

  val withSessionSize = sessions
    .groupBy("sessionId")
    .agg(collect_list("dateMillis").as("all_dateMillis"))
    .withColumn("sessionSize", getSessionSize($"all_dateMillis"))

  withSessionSize
    .agg(avg("sessionSize"))
    .write.csv(output+"/2/")

  //3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

  sessions
    .groupBy("sessionId")
    .agg(countDistinct("url"))
    .write.csv(output+"/3/")

  //4. Find the most engaged users, ie the IPs with the longest session times

  withSessionSize
    .orderBy(desc("sessionSize"))
    .write.csv(output+"/4/")
}