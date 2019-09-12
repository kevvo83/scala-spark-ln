import sbt._
import Dependencies._

name := "scala-spark-ln-1"
version := "0.2"
scalaVersion := "2.11.12"

val libDependencyOpt = Option(System.getProperty("libDependencyOpt")).getOrElse("NONE")

lazy val root = (project in file("."))//.aggregate(week3)

lazy val week3 = (project in file("coursera-hmiller-week3")).
  settings(
    version := "0.1.0-SNAPSHOT"
  ).
  settings(
    libraryDependencies ++= clusterLibraryDependencies
  ).
  settings(
    assemblyJarName in assembly := name.value + "-assembly-" + version.value + ".jar"
  ).
  settings(
    assemblyMergeStrategy in assembly := {
      case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("org","objenesis", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  ).
  settings(
    test in assembly := {}
  )


lazy val week2 = (project in file("coursera-hmiller-week2")).
  settings(
    version := "0.2.0-SNAPSHOT"
  ).
  settings(
    libraryDependencies ++= clusterLibraryDependencies
  ).
  settings(
    test in assembly := {}
  ).
  settings(
    assemblyJarName in assembly := name.value + "-assembly-" + version.value + ".jar"
  ).
  settings(
    assemblyMergeStrategy in assembly := {
      case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("org","objenesis", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )


lazy val structstreamapps = (project in file("struct-stream-apps")).
  settings(
    resolvers += "confluent" at "https://packages.confluent.io/maven/"
  ).
  settings(
    version := "0.2.0-SNAPSHOT"
  ).
  settings(
    libraryDependencies ++= clusterLibraryDependencies,
    libraryDependencies ++= Seq(
      "io.confluent" % "kafka-schema-registry-client" % "3.3.0" withSources() withJavadoc(),
      "io.confluent" % "kafka-avro-serializer" % "3.3.0" withSources() withJavadoc(),
      "org.apache.kafka" % "kafka-streams" % "2.3.0",
      "org.apache.kafka" % "kafka-streams-test-utils" % "2.3.0" % Test
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1" % "provided",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1" % "provided"
    )
  ).
  settings(
    test in assembly := {}
  ).
  settings(
    assemblyJarName in assembly := name.value + "-assembly-" + version.value + ".jar"
  ).
  settings(
    assemblyMergeStrategy in assembly := {
      case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("org","objenesis", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )