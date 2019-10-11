import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class Spell(tc :  String, lc : String, cc : String, sc : Boolean) {
  var title : String = tc
  var levels : String = lc
  var components : String = cc
  var spellResistance : Boolean = sc

  def selfPrint(): Unit = {
    println("ScalaSpells.Spell ",title,", levels ",levels,", components ",components,", spell resistance ", spellResistance)
  }
}

object Part1 {

  def collect_spells(filename : String, spell_collection : ArrayBuffer[Spell]): Unit = {
    for (line <- Source.fromFile(filename).getLines) {
      val spell_infos =  line.split(";")
      val new_spell = new Spell(spell_infos(0), spell_infos(1), spell_infos(2), spell_infos(3).toBoolean)
      spell_collection.append(new_spell)
    }
  }

  def main(args: Array[String]): Unit = {

    //var spell_collection = new Array[Spell](2000)
    var spell_collection = new ArrayBuffer[Spell]()

    for (i <- 0 to 30) {
      val fileName = "spells_thread" + i + ".txt"
      val file_to_check = new File(fileName)
      val exist = file_to_check.exists()

      if (exist) {
        collect_spells(fileName, spell_collection)
      }
    }

    /*
    for (spell <- spell_collection) {
      spell.selfPrint()
    }
    */

    val conf = new SparkConf()
      .setAppName("Spells")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spellsRDD = sc.makeRDD(spell_collection)

    println(spellsRDD.getClass().getName())
    println(spellsRDD.count())

  }
}


