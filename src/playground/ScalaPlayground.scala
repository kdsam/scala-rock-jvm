package playground

object ScalaPlayground extends App {

  println("Hello, Scala!")
  for {
    i <- 1 to 1
  } {
    print(i)

    val s = s"""
            |SELECT * FROM growingtree.member M""".stripMargin
  }
}
