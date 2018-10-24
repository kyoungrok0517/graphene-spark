import java.util

import org.lambda3.graphene.core.Graphene
import org.apache.spark.sql.{Row, SparkSession, types}


import scala.collection.JavaConverters._

object Main {
  def main(args: Array[String]): Unit = {
    // Check arguments
    if (args.length < 1) {
      System.err.println("Usage: Main <file_path>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Exobrain Graphene Processer")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val file_path = args(0)

    val schema = types.StructType(
      types.StructField("file", types.DataTypes.StringType, true) ::
        types.StructField("content", types.DataTypes.StringType, true) :: Nil
    )
    val rdd = spark.read.schema(schema).parquet("data/wikipedia-train/**")
    rdd.show()

    import spark.implicits._
    val results = rdd.mapPartitions(rows => {
      new util.ArrayList[String]().iterator.asScala
    }).collect()
  }

  //  def test(): Unit = {
  //    val graphene = new Graphene()
  //
  //    val testString = "In which year were TV licences introduced in the UK?"
  //    val result = graphene.doRelationExtraction(testString, false, false).serializeToJSON()
  //
  //    println(result)
  //  }
}

object GrapheneProcessor {
  def process(rows: Iterator[Row]): Iterator[String] = {
    val results = new util.ArrayList[String]
    if (rows.isEmpty) {
      return results.iterator.asScala
    }

    rows.map(row => {
      val file = row(0).toString
      val content = row(1).toString

      results.add(file + "\t" + content)
    })

    return results.iterator.asScala
  }

  //  def process(rows: Iterator[Row]): Iterator[String] = {
  //    val graphene = new Graphene()
  //    val results = new util.ArrayList[String]
  //
  //    rows.map(row => {
  //      val file = row(0).toString
  //      val content = row(1).toString
  //      val result = graphene.doRelationExtraction(content, true, false).serializeToJSON()
  //
  //      results.add(file + "\t" + result)
  //    })
  //
  //
  //    return results.iterator.asScala
  //  }
}