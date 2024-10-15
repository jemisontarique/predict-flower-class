ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-mllib" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",

  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2",
  "joda-time" % "joda-time" % "2.9.9",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "org.slf4j" % "slf4j-simple" % "1.7.22"
)


lazy val root = (project in file("."))
  .settings(
    name := "IrisDataset"
  )
