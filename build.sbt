name := "WeblogChallenge"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies +=  "com.twitter" %% "scalding-args" % "0.15.0"

// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

//Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % Test
libraryDependencies += "org.mockito" % "mockito-all" % "1.10.19" % Test

