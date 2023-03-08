# ScalaDBSCAN
Simple clustering algorithm

Add this to sbt 

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"
ThisBuild / useCoursier := false

lazy val root = (project in file(".")) .settings( name := "untitled1" )
libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % "3.2.2",
    "org.apache.spark" %% "spark-sql" % "3.2.2",
    "org.apache.spark" %% "spark-graphx" % "3.3.2"
  )
