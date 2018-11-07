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
      .master(s"local[2]")
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
    val finishedFilesCounter = sc.longAccumulator("finishedFilesCounter")

    println("Total count: " + totalCount.value)

    import spark.implicits._
    val sentences = df.as[Record].flatMap(row => {
      val file = Option(row.file.toString).getOrElse("")
      val content = Option(row.content.toString).getOrElse("")
      content.split("\n").map((s: String) => (file: String, s: String))
    }).filter(t => !t._2.trim.isEmpty)

//    sentences.foreach(tuple => println(tuple))
  }
}

//     val results = df.repartition(n_partitions).as[Record].mapPartitions(rows => {
     

//       val sentences = rows.flatMap(row => {
//         val file = Option(row.file.toString).getOrElse("")
//         val content = Option(row.content.toString).getOrElse("")
//         content.split("\n")
//       })

//       results_rows
//     }).toDF("file", "sentence", "graphene")

//     // save
//     results.write.option("compression", "snappy").parquet(out_path)
//   }
// }


//  val graphene = new Graphene()
// val results_ = sentences.map(sentence => {
//           var res_json: String = "{}"
//           try {
//             res_json = graphene.doRelationExtraction(sentence, true, false).serializeToJSON()
//             (file, sentence, res_json)
//           } catch {
//             case unknown: Throwable => { 
//               res_json = "{}"
//               (file, sentence, res_json)
//             }
//           }
//         })

//         // update the counter
//         finishedFilesCounter.add(1)

//         // return
//         results_