name := "Machine Learning Algorithm Examples"

version := "1.0"

val scalaVersionUsed = "2.11"
val sparkVersion = "2.1.0"

val spark = "org.apache.spark" % s"spark-core_$scalaVersionUsed" % s"$sparkVersion"
val mlLib = "org.apache.spark" % s"spark-mllib_$scalaVersionUsed" % s"$sparkVersion"

lazy val commonSettings = Seq(
  organization := "com.knoldus",
  version := "0.2.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Machine Learning Algorithm Examples",
    libraryDependencies ++= Seq(spark, mlLib)
  )