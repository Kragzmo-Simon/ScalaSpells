package ScalaSpells

import com.gaocegege.scrala.core.common.response.impl.HttpResponse
import com.gaocegege.scrala.core.spider.impl.DefaultSpider

import scala.collection.mutable.ArrayBuffer

class Spell(tc :  String, lc : String, cc : String, sc : Boolean) {
  var title : String = tc
  var levels : String = lc
  var components : String = cc
  var spellResistance : Boolean = sc

  def selfPrint(): Unit = {
    println("ScalaSpells.Spell ",title,", levels ",levels,", components ",components,", spell resistance ",spellResistance)
  }
}

class TestSpider extends DefaultSpider {
  var startUrl = List[String]("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=1")
  var spellCollection = new ArrayBuffer[Spell]()

  def parse(response: HttpResponse): Unit = {
    val content = response.getContentParser()
    //println(content)
    //println(content.getClass().toString())
    val spellInformation = content.select("div[class=SpellDiv]")
    val SpDet = spellInformation.select("p[class=SpDet]")

    val title = spellInformation.select("div[class=heading]").first.text()
    val levels = SpDet.first.text()
    val components = SpDet.get(2).text()
    val spellResist = SpDet.get(6).text().split(";").last.trim().substring(17)

    val spellTitle = title
    val spellLevels = levels.split(";").last.trim().substring(6)
    var spellComponents = ""
    var spellResistance = true

    for (c <- components.substring(11)) {
      if (c.isUpper) {
        spellComponents += c
      }
    }

    if (spellResistance.equals("no")) {
      spellResistance = false
    }

    val newSpell = new Spell(spellTitle,spellLevels,spellComponents,spellResistance)
    println("Acces à collection")
    spellCollection += newSpell
    println("Ajout de spell")

    //println("HEEEEEEEY")
    //newSpell.selfPrint()
  }

  def printIt(response: HttpResponse): Unit = {
    println((response.getContentParser).title)
  }
}

object Main {
  def main(args: Array[String]) {
    val crawler = new TestSpider

    for (i <- 2 to 5) {
      val newUrl = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i.toString()
      val newUrlCollection = newUrl :: crawler.startUrl
      crawler.startUrl = newUrlCollection
    }

      crawler begin

    Thread.sleep(10000)
    println("On en a : ", crawler.spellCollection.length)
    for(element <- crawler.spellCollection){
      element.selfPrint()
    }
  }
}