import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

import org.apache.spark.sql.{SparkSession, Dataset}

trait TablesCaseClass[T <: Product] {
  def toDB(dbName: String, t: T): Unit
  def fromDB(spark: SparkSession, dbName: String): T
}

object TablesCaseClass {
  //def toDB[T <: Product](t: T)(implicit converter: TablesCaseClass[T]) = converter.toDB(t)
  def toDB[T <: Product](dbName: String, t: T)(implicit converter: TablesCaseClass[T]) = converter.fromDB(dbName, dbName)

  def fromDB[T <: Product](spark: SparkSession, dbName: String)(implicit converter: TablesCaseClass[T]) = converter.fromDB(spark, dbName)

  implicit def materializeTablesCaseClass[T <: Product]: TablesCaseClass[T] = macro materializeTablesCaseClassImpl[T]

  p
  def materializeTablesCaseClassImpl[T <: Product : c.WeakTypeTag](c: Context): c.Expr[TablesCaseClass[T]] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val companion = tpe.typeSymbol.companion

    val fields = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor ⇒ m
    }.get.paramLists.head

    val loadFromDb = fields.map { field ⇒
      val name = field.name.toTermName
      val decoded = name.decodedName.toString

      q"spark.table($decoded).as" //(spark.implicits.newProductEncoder)"
    }
    val writeToDbStatements = fields.map { field ⇒
      val name = field.name.toTermName
      val decoded = name.decodedName.toString

          //spark.sql("USE " + dbName)
        //def toDB(t: $tpe): Map[String, Any] = Map(..$toMapParams)
    c.Expr[TablesCaseClass[T]] { q"""
      new TablesCaseClass[$tpe] {
        def toDB(t: T): Unit = ..$writeToDBStatements
        def fromDB(spark: SparkSession, dbName: String): $tpe = {
          //import spark.implicits._
          $companion(..$loadFromDb)
        }
      }
    """ }
  }
}
