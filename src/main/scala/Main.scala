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
      System.err.println("Usage: Main <data_path> <n_threads>")
      System.exit(1)
    }

    // get args
    val data_path = args(0)
//    val target_partition = args(1).toInt
//    val total_partition = args(2).toInt
    val n_threads = args(1).toInt

    val spark = SparkSession
      .builder()
      .appName("Exobrain Graphene Processer")
      .master(s"local[$n_threads]")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read.schema(schema).parquet(data_path).toDF()

    // 여러 컴퓨터에 분산하기 위한 코드
//    var df_partitions = Seq[DataFrame]()
//    val frac = 1.0 / total_partition
//    val seed = 42  // subset 결과가 항상 동일하도록
//    for (_ <- 1 to total_partition) {
//      val df_ = df.sample(false,0.25, seed = seed).toDF()
//      df_partitions = df_partitions :+ df_
//    }
//    // 처리할 파티션 선택
//    val target_df = df_partitions(target_partition)

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
    }).toDF("file", "graphene").write.option("compression", "snappy").parquet("./output")
  }
}
