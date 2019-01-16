package lectures.part1basics

object ValuesVariablesType extends App {

  val x: Int = 42

  println(x)

  // VAL ARE IMMUTABLE

  // COMPILER can infer types

  val aString: String = "hello"
  val anotherString = "goodbye"

  val aBoolean: Boolean = false
  val aChar: Char = 'a'
  val anInt: Int = x
  val short: Short = 4613
  val aLong: Long = 5273434343434L
  val aFloat: Float = 2.0f
  val aDouble: Double = 3.14

}
