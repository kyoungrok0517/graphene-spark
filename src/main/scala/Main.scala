import org.lambda3.graphene.core.Graphene
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object Main {
  val schema = types.StructType(
    types.StructField("file", types.DataTypes.StringType, true) ::
      types.StructField("content", types.DataTypes.StringType, true) :: Nil
  )

  def main(args: Array[String]): Unit = {
    // Check arguments
    if (args.length < 2) {
      System.err.println("Usage: Main <data_path> <out_path>")
      System.exit(1)
    }

    // get args
    val data_path = args(0)
    val out_path = args(1)

    val spark = SparkSession
      .builder()
      .appName("Exobrain Graphene Processer")
      .master(s"local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read.schema(schema).parquet(data_path).toDF()

    // 처리 시작
    val totalCount = sc.broadcast(df.count())
    val finishedFilesCounter = sc.longAccumulator("finishedFilesCounter")

    import spark.implicits._
    df.mapPartitions(rows => {
      val graphene = new Graphene()

      val results = rows.map(row => {
        val file = row(0).toString
        val lines = row(1).toString.split("\n")
        val results_ = lines.map(line => {
          val res_json = graphene.doRelationExtraction(line, true, false).serializeToJSON()

          (file, res_json)
        })

        // update the counter
        finishedFilesCounter.add(1)

        // return
        results_
      })
      results.flatten
    }).toDF("file", "graphene").write.option("compression", "snappy").parquet(out_path)

    println("finishedFilesCounter: " + finishedFilesCounter)
  }
}
