package ScalaSpells
import org.apache.spark
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.io.Source




object dataframe_spark_SQL extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  override def main(args: Array[String]) {
    val path = "spells_thread12.txt"
    val textFile=Source.fromFile(path).getLines
    var dataset=textFile.flatMap(line => "[\""+line.split(";")(0)+"\",\""+line.split(";")(1)+"\",\""+line.split(";")(2)+"\","+line.split(";")(3)+"]")
    dataset.foreach(line =>print (line))

  }
}
//  "[\"Name\",\"level\",\"composant\",\"spellresist\""