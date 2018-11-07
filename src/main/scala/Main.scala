import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types.{StringType, StructField}
import org.lambda3.graphene.core.Graphene
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

case class Record(file: String, content: String)

case class Result(file: String, sentence: String, graphene: String)

object Main {

  def main(args: Array[String]): Unit = {
    // Check arguments
    if (args.length < 3) {
      System.err.println("Usage: Main <data_path> <out_path> <n_partitions>")
      System.exit(1)
    }

    // get args
    val data_path = args(0)
    val out_dir = args(1)
    val n_partitions = args(2).toInt
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss")
    val timestamp = formatter.format(now)
    val out_path = s"$out_dir/$timestamp"

    val spark = SparkSession
      .builder()
      .appName("Exobrain Graphene Processer")
      .master(s"local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    val schema = types.StructType(
      StructField("file", StringType, true) ::
        StructField("content", StringType, true) :: Nil
    )
    val df = spark.read.schema(schema).parquet(data_path)
    //    df.show()

    // 처리 시작
    val totalCount = sc.broadcast(df.count())

    println("Total count: " + totalCount.value)

    import spark.implicits._

    // get (file, sentence) pairs
    val sentences = df.as[Record].flatMap(row => {
      val file = Option(row.file.toString).getOrElse("")
      val content = Option(row.content.toString).getOrElse("")
      content.split("\n").map((s: String) => (file: String, s: String))
    }).filter(t => (!t._1.trim.isEmpty && !t._2.trim.isEmpty))

    // Run Graphene
    val results = sentences.repartition(n_partitions).mapPartitions(file_and_sentence_tuples => {
      val graphene = new Graphene()

      val graphene_results = file_and_sentence_tuples.map(file_and_sent => {
        val file = file_and_sent._1
        val sentence = file_and_sent._2
        var res_json: String = "{}"
        try {
          res_json = graphene.doRelationExtraction(sentence, true, false).serializeToJSON()
          (file, sentence, res_json)
        } catch {
          case _: Throwable => {
            res_json = "{}"
            (file, sentence, res_json)
          }
        }
      })

      // return
      graphene_results
    })

    // Save
    val df_results = results.toDF("file", "sentence", "graphene")
    df_results.write.option("compression", "snappy").parquet(out_path)
  }
}