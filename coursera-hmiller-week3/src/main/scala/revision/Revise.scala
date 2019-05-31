package revision

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object Revise extends App {

  def fsPath(resourcefilename: String): String = {
    Paths.get(getClass.getResource(resourcefilename).toURI).toString
  }

  def defineSchema(firstline: String): StructType = {
    val splitup: List[String] = firstline.split(",").toList

    val init: Array[StructField] = Array(StructField(splitup.head, StringType , false))

    def worker(in: List[String], acc: Array[StructField]): Array[StructField] = in match {
      case Nil => acc
      case (h :: t) => worker(t, acc :+ StructField(h, DoubleType, false))
    }

    StructType(worker(splitup.tail, init))
  }


  val spark: SparkSession = SparkSession.builder().appName("revise-sparkapp").config("spark.master", "local").getOrCreate()

  val rdd: RDD[String] = spark.sparkContext.textFile(fsPath("/resources/timeusage/atussum.csv"))
  val schema: StructType = defineSchema(rdd.take(1).toString)



  //


}
