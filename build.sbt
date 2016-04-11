name := "SparkPro"

version := "1.0"

scalaVersion := "2.11.7"

lazy val sparkVersion ="1.5.2"

libraryDependencies +={
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
  "org.apache.spark" %% "spark-hive" % sparkVersion % "compile"
}