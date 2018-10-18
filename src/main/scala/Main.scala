import org.lambda3.graphene.core.Graphene

object Main {
  def main(args: Array[String]): Unit = {
    val graphene = new Graphene()

    val testString = "This is a test string."
    val result = graphene.doRelationExtraction(testString, true, false)

    println(result)
  }
}