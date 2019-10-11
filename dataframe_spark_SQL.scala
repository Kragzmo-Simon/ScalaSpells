package ScalaSpells
import java.io.File

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
    var dataset=textFile.flatMap(line => "{\"name\":\""+line.split(";")(0)+"\",\"level\": \""+line.split(";")(1)+"\",\"composant\":\""+line.split(";")(2)+"\",\"spellresist\":"+line.split(";")(3)+"}"+"\n")
    val fileName = "spells_thread.json"

    val file_to_check = new File(fileName)
    val exist = file_to_check.exists()
    if (exist) {
      file_to_check.delete()
    }


    dataset.foreach(line =>print (line))

  }
}
//  "[\"Name\",\"level\",\"composant\",\"spellresist\""