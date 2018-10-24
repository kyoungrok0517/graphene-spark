import java.util

import org.lambda3.graphene.core.Graphene
import org.apache.spark.sql.{Row, SparkSession, types}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Main {
  val schema = types.StructType(
    types.StructField("file", types.DataTypes.StringType, true) ::
      types.StructField("content", types.DataTypes.StringType, true) :: Nil
  )

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

    val df = spark.read.schema(schema).parquet(file_path)

    import spark.implicits._
    df.rdd.mapPartitions(rows => {
      val graphene = new Graphene()

      val results = rows.map(row => {
        val file = row(0).toString
        val content = row(1).toString
        val lines = content.split("\n")
        val results_ = lines.map(line => {
          val res_json = graphene.doRelationExtraction(line, true, false).serializeToJSON()
          (file, res_json)
        })
        results_
      })
      results.flatten
    }).toDF("file", "graphene").write.option("compression", "snappy").parquet("./output")
  }
}
