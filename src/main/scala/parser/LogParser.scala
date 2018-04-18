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
  val startTime = cmdArgs.getOrElse("start-time", "")
  val timeWindow = cmdArgs.getOrElse("window", "15m")
  val output = cmdArgs.getOrElse("output", "/tmp/output-spark/")
  val ip = cmdArgs.getOrElse("ip", "112.196.25.164")


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
    .csv("/Users/shreya/Desktop/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    .toDF("timestamp", "elb", "client:port", "backend:port", "request_processing_time", "backend_processing_time",
      "response_processing_time", "elb_status_code", "backend_status_code", "received_bytes", "sent_bytes",
      "request", "user_agent", "ssl_cipher", "ssl_protocol")
    .withColumn("dateMillis", dateToMillis($"timestamp"))
    .withColumn("fullProcessingTime", $"request_processing_time" + $"backend_processing_time" + $"response_processing_time")
    .withColumn("client_ip", clientIP($"client:port"))
    .withColumn("url", urlFromRequest($"request"))

  val startMillis = getMillis("2015-07-22T09:00:28.491670Z")
  val endMillis = getMillis("2015-07-23T09:00:28.491670Z")

  //1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
  val sessions = logs
    .filter(s"dateMillis > $startMillis and dateMillis < $endMillis")
    .groupBy("client_ip")

  sessions.count().orderBy(desc("count")).write.csv(output+"/1/")

  //2. Determine the average session time
  val sessionSize = udf { listMillis: Seq[String] =>
    val times = listMillis.map(_.toLong)
    times.max - times.min
  }

  sessions
    .agg(collect_list("datemillis").alias("times"))
    .withColumn("session_size", sessionSize($"times"))
    .agg(avg($"session_size").as("session_size_avg"))
    .write.csv(output+"/2/")

  //3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
  sessions
    .agg(countDistinct("url"))
    .write.csv(output+"/3/")

  //4. Find the most engaged users, ie the IPs with the longest session times
  sessions
    .agg(sum($"fullProcessingTime").alias("fullTime"))
    .orderBy("fullTime")
    .write.csv(output+"/4/")
}