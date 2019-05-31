import sbt._
import sbtassembly.{MergeStrategy, PathList}

object Dependencies {

  val clusterLibraryDependencies = Seq(
    "junit" % "junit" % "4.12" % Test,
    "org.apache.spark" %% "spark-core" % "2.4.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided",
    "org.apache.spark" %% "spark-hive" % "2.4.2" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.3" % Test,
    "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "provided",
    "org.apache.hadoop" % "hadoop-aws" % "2.6.5" % "provided",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"
  )

  val localLibraryDependencies = Seq(
    "junit" % "junit" % "4.12" % Test,
    "org.apache.spark" %% "spark-core" % "2.4.2",
    "org.apache.spark" %% "spark-sql" % "2.4.2",
    "org.apache.spark" %% "spark-hive" % "2.4.2",
    "org.scalatest" %% "scalatest" % "3.0.3" % Test,
    "org.apache.hadoop" % "hadoop-common" % "2.6.5",
    "org.apache.hadoop" % "hadoop-aws" % "2.6.5",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4"
  )

}