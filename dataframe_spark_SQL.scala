package ScalaSpells
import java.io.{File, FileWriter}

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

  import spark.implicits._

  override def main(args: Array[String]) {

    val json_fileName = "spells.json"
    val json_file = new File(json_fileName)
    val exist = json_file.exists()
    if (exist) {
      json_file.delete()
    }

    for (i <- 0 to 30) {
      val path = "spells_thread" + i + ".txt"
      val file_to_check = new File(path)

      if (file_to_check.exists()) {
        val textFile = Source.fromFile(path).getLines
        val dataset = textFile.flatMap(line => {
          val spell_information = line.split(";")
          "{\"name\":\"" + spell_information(0) + "\",\"level\": \"" + spell_information(1) + "\",\"composant\":\"" + spell_information(2) + "\",\"spellresist\":" + spell_information(3) + "}" + "\n"
        })

        var dataset_lines = ""
        while (dataset.hasNext) {
          dataset_lines += dataset.next()
        }
        val fw = new FileWriter(json_fileName, true)
        try {
          fw.write(dataset_lines)
        }
        finally fw.close()

      }
    }

    //val dang = "C:\\Users\\ferys\\IdeaProjects\\untitled\\" + json_fileName
    val dang = "C:/Users/ferys/IdeaProjects/untitled/" + json_fileName
    println(dang)
    val spellsSQL = spark.read.json(dang)

    //spellsSQL.printSchema()
  }
}
