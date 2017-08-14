import org.apache.spark.sql.{SparkSession, Dataset}
case class Person(name: String, age: Int)
case class ClinDev(people: Dataset[Person]) 
object Main extends App {

  val spark = SparkSession.builder.master("local[*]").getOrCreate
  import spark.implicits._
  val person = Person("john", 24)
  Seq(person).toDS.write.saveAsTable("people")

  import TablesCaseClass._

  /*
  val person = Person("john", 24)
  assert {
    toDB(person) == Map("name" -> "john", "age" -> 24)
  }
  */

  val map = Map("name" -> "bob", "age" -> 22)
  val input = fromDB[ClinDev](spark, "clin_dev") // == Person("bob", 22)
  input.people.show
  assert {
    true
  }
}
