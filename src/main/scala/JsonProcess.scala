
import scala.io.Source
import upickle.default._
import upickle.default.{macroRW, ReadWriter => RW}

import scala.util.Try

case class Vacation(place: String, year: Int)
object Vacation {
  implicit val rw: RW[Vacation] = macroRW
}

case class Person(
                   name: String,
                   salary: Int = -1,
                   foods: List[String] = List.empty,
                   vacations: List[Vacation] = List.empty)
object Person {
  implicit val rw: RW[Person] = macroRW
}

case class DataRecord(event_id: Int, data: Person)
object DataRecord {
  implicit val rw: RW[DataRecord] = macroRW
}

object JsonProcess extends App {

  // read resources file line oriented
  private val sourceLines = Source.fromResource("data.json").getLines()

  // deserialize json lines to case class
  private val records = sourceLines
    .map(line => Try(read[DataRecord](line)))
    .filter(_.isSuccess)
    .map(_.get)
    .toList

  private val salaryMap = records
    .filter(_.data.salary > 0)
    .sortBy(_.event_id)
    .map(dr => (dr.data.name, dr.data.salary))
    // ease the pain: later keys will override earlier keys
    .toMap

  println(salaryMap.values.sum.toDouble / salaryMap.values.size)
}
