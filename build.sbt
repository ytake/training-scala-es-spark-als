name := "training-scala-es-spark-als"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in assembly := Some("net.jp.ytake.SparkRecommenderApp")

logLevel := Level.Error

resolvers ++= Seq(
  "clojars" at "https://clojars.org/repo",
  "conjars" at "http://conjars.org/repo",
  "plugins" at "http://repo.spring.io/plugins-release",
  "sonatype" at "http://oss.sonatype.org/content/groups/public/",
  "mvn" at "https://mvnrepository.com/artifact"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.json4s" %% "json4s-core" % "3.2.11",
  "org.json4s" %% "json4s-ast" % "3.2.11",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.4.3",
  "junit" % "junit" % "4.12" % Test,
  "org.assertj" % "assertj-core" % "3.9.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "rootdoc.txt" => MergeStrategy.discard
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName in assembly := { s"${name.value}-${version.value}.jar" }
