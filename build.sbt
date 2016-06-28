name := "movie_recommendation"

version := "1.0"

val spark = "org.apache.spark" % "spark-core_2.10" % "1.6.0"
val graphX = "org.apache.spark" % "spark-graphx_2.10" % "1.6.0"
val mlLib = "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"

lazy val commonSettings = Seq(
  organization := "com.knoldus.spark",
  version := "0.1.0",
  scalaVersion := "2.10.5"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "movie_recommendation",
    libraryDependencies ++= Seq(spark, graphX, mlLib)
  )