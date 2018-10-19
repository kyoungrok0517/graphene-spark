import org.lambda3.graphene.core.Graphene

object Main {
  def main(args: Array[String]): Unit = {
    val graphene = new Graphene()

    val testString = "In which year were TV licences introduced in the UK?"
    val result = graphene.doRelationExtraction(testString, false, false).serializeToJSON()

    println(result)
  }
}