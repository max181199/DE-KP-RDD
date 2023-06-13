package max.work


import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import java.util.Calendar
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class DocOpen (
                         key: Long,
                         time: Long,
                         docName: String,
                         doc: Int
                       )

case class Session (
                     quickSearch: mutable.HashMap[Long, mutable.HashSet[Int]],
                     cardSearch: mutable.HashMap[Long, mutable.HashSet[Int]],
                     docOpen: List[DocOpen]
                   )

sealed trait ParseStates
  case object START extends ParseStates
  case object END extends ParseStates
  case object SESSION_START extends ParseStates
  case object SESSION_END extends ParseStates
  case object SEARCH_RESULT extends ParseStates
  case object SEARCH_PARAMS extends ParseStates
  case object QUICK_SEARCH extends ParseStates
  case object CARD_SEARCH_START extends ParseStates
  case object CARD_SEARCH_END extends ParseStates
  case object DOC_OPEN extends ParseStates
  case object ERROR extends ParseStates

case class StatisticsMapKey(docHash: Long, date: Long)

object App {

  private def lineToDocOpen (fileName: String, str: String): DocOpen = {
    val param: ArrayBuffer[String] = ArrayBuffer[String]("","","")
    val keyPattern = "^\\d+$".r
    val keyPatternMinus = "^-\\d+$".r
    val docPattern = "^\\w+_\\d+$".r
    str.split(" ").tail.foreach(word => {
      if (keyPattern.findFirstIn(word).isDefined || keyPatternMinus.findFirstIn(word).isDefined ){
        param(0) = word
      } else if (docPattern.findFirstIn(word).isDefined) {
        param(1) = word
      } else {
        param(2) = word
      }
    })


    val datetime: Long = if (param(2) != "") {
      try {
        val date = new java.text.SimpleDateFormat("dd.MM.yyyy_H:m:s").parse(param(2))
        val calendar = Calendar.getInstance
        calendar.setTime(date)
        calendar.set(Calendar.MILLISECOND, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.HOUR, 0)
        calendar.getTimeInMillis
      } catch {
        case _: Throwable =>
          println(s"ERROR => file: $fileName; can't parse date;")
          -1
      }
    } else 0

    val key: Long = if (param(0) != "")  {
      try {
        param(0).toLong
      } catch {
        case _: Throwable =>
          println(s"ERROR => file: $fileName; can't parse key;")
          -1
      }
    } else 0

    val docHash: Int = if (param(1) != "") {
      param(1).hashCode()
    } else 0

    DocOpen(
      key = key,
      time = datetime,
      docName = param(1),
      doc = docHash
    )

  }

  private def lineHead(line: String): String = {
    line.take(20).split(" ").head
  }

  private def parseText(fileName:String, str: String): Session = {
    val lines = str
      .split("\n")
    //println(s"PARSING ${fileName}")
    val qs_hm = mutable.HashMap[Long, mutable.HashSet[Int]]()
    val cs_hm = mutable.HashMap[Long, mutable.HashSet[Int]]()
    var dop = List[DocOpen]()
    var i = 0
    var state : ParseStates = START
    while (state != END && state != ERROR){
      state match {
        case START =>
          //println("START")
          lineHead(lines(i)) match {
            case "SESSION_START" =>
              state = SESSION_START
              i += 1
            case _ => state = ERROR
          }
        case SESSION_START =>
          //println("SESSION_START")
          lineHead(lines(i)) match {
            case "SESSION_END" =>
              state = SESSION_END
            case "CARD_SEARCH_START" =>
              i += 1
              state = CARD_SEARCH_START
            case "QS" =>
              i += 1
              state = QUICK_SEARCH
            case _ => state = ERROR
          }
        case SESSION_END =>
          //println("SESSION_END")
          state = END
        case CARD_SEARCH_START =>
          //println("CARD_SEARCH_START")
          lineHead(lines(i)) match {
            case "CARD_SEARCH_END" =>
              state = CARD_SEARCH_END
              i += 1
            case _ =>
              state = SEARCH_PARAMS
              i += 1
          }
        case SEARCH_PARAMS =>
          //println("SEARCH_PARAMS")
          lineHead(lines(i)) match {
            case "CARD_SEARCH_END" =>
              state = CARD_SEARCH_END
              i += 1
            case _ =>
              state = SEARCH_PARAMS
              i += 1
          }
        case CARD_SEARCH_END =>
          //println("CARD_SEARCH_END")
          val words = lines(i).split(" ")
          try {
            val key = words(0).toLong
            if (cs_hm.exists(_._1 == key)){
              cs_hm(key) ++ words.tail.map(s => s.hashCode())
            } else {
              cs_hm(key) = mutable.HashSet() ++ words.tail.map(s => s.hashCode())
            }
          } catch {
            case _: Throwable =>
          }
          i += 1
          state = SEARCH_RESULT
        case QUICK_SEARCH =>
          //println("QUICK_SEARCH")
          val words = lines(i).split(" ")
          try {
            val key = words(0).toLong
            if (qs_hm.exists(_._1 == key)) {
              qs_hm(key) ++ words.tail.map(s => s.hashCode())
            } else {
              qs_hm(key) = mutable.HashSet() ++ words.tail.map(s => s.hashCode())
            }
          } catch {
            case _: Throwable =>
          }
          i += 1
          state = SEARCH_RESULT
        case SEARCH_RESULT =>
          //println("SEARCH_RESULT")
          lineHead(lines(i)) match {
            case "SESSION_END" =>
              state = SESSION_END
            case "CARD_SEARCH_START" =>
              state = CARD_SEARCH_START
              i += 1
            case "QS" =>
              state = QUICK_SEARCH
              i += 1
            case "DOC_OPEN" =>
              state = DOC_OPEN
            case _ => state = ERROR
          }
        case DOC_OPEN =>
          //println("DOC_OPEN")
          dop = lineToDocOpen(fileName, lines(i)) :: dop
          i += 1
          lineHead(lines(i)) match {
            case "SESSION_END" =>
              state = SESSION_END
            case "CARD_SEARCH_START" =>
              state = CARD_SEARCH_START
              i += 1
            case "QS" =>
              state = QUICK_SEARCH
              i += 1
            case "DOC_OPEN" =>
              state = DOC_OPEN
            case _ => state = ERROR
          }
        case _ => state = ERROR
      }
    }

    if (state == ERROR) println(s"ERROR: file=$fileName line=$i")
    //dop.foreach(println)
    //cs_hm.foreach(e => println(s"CardSearch => \n\t key: ${e._1}; \n\t set: ${e._2}"))
    //qs_hm.foreach(e => println(s"QuickSearch => \n\t key: ${e._1}; \n\t set: ${e._2}"))
    Session(cardSearch = cs_hm, quickSearch = qs_hm, docOpen = dop)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DE_Task")
    val sc = new SparkContext(conf)
    val sessions =
      sc
        .wholeTextFiles("./data")
        .map(file => parseText(file._1, file._2))
    val docHash = "ACC_45616".hashCode()
    val docCount = sc.longAccumulator("ACC_45616 count")
    sessions.foreach(session => session.cardSearch.foreach(cs => if (cs._2.contains(docHash)) docCount.add(1)))
    val statistics = sessions
      .map(session => {
        val map = mutable.Map[StatisticsMapKey, Long]()
          session.docOpen.foreach(doc => {
            if (session.quickSearch.contains(doc.key) && session.quickSearch(doc.key).contains(doc.doc)){
              if (map.contains(StatisticsMapKey(doc.doc, doc.time))) {
                map(StatisticsMapKey(doc.doc,  doc.time)) += 1
              } else {
                map += StatisticsMapKey(doc.doc, doc.time) -> 1
              }
            }
          })
        map
      })
      .collect()
      .reduce(
        (mapA, mapB) => mapA ++ mapB.map { case (k, v) => k -> (v + mapA.getOrElse(k, 0L)) }
      )

    new PrintWriter("result") {
      write(s"Count: ${docCount.value};\n")
      statistics.foreach(m => write(s"${m._1.docHash}\t${m._1.date}\t${m._2}\n"))
      close()
    }

    sc.stop()
  }
}