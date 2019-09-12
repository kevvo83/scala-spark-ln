import sbt._
import sbtassembly.AssemblyPlugin.autoImport
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyMergeStrategy}
import sbtassembly.PathList
import sbtassembly.AssemblyKeys._

object Dependencies {

  val clusterLibraryDependencies = Seq (
    "junit" % "junit" % "4.12" % Test,
    "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-avro" % "2.4.3" % "provided",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.3" % Test,
    "org.apache.hadoop" % "hadoop-aws" % "2.6.5" % "provided",
    "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "provided",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided",
    "io.spray" %%  "spray-json" % "1.3.5"
  )

  /*val localLibraryDependencies = Seq(
    "junit" % "junit" % "4.12" % Test,
    "org.apache.spark" %% "spark-core" % "2.4.2",
    "org.apache.spark" %% "spark-sql" % "2.4.2",
    "org.apache.spark" %% "spark-hive" % "2.4.2",
    "org.scalatest" %% "scalatest" % "3.0.3" % Test,
    "org.apache.hadoop" % "hadoop-common" % "2.6.5",
    "org.apache.hadoop" % "hadoop-aws" % "2.6.5",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4"
  )*/

}