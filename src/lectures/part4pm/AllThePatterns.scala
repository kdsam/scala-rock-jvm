package lectures.part4pm

import exercises.{Cons, Empty, MyList}

object AllThePatterns extends App {

  // 1 - constants
  val x: Any = "Scala"

  val constants = x match {
    case 1 => "a number"
    case "Scala" => "THE Scala"
    case true => "The Truth"
    case AllThePatterns => "A singleton object"
  }

  // 2 - match anything
  // 2.1 wildcard

  val matchAnything = x match {
    case _ =>
  }

  // 2.2 variable
  val matchAVariable = x match {
    case something => s"I've found $something"
  }

  // 3 - tuples
  val aTuple = (1,2)
  val matchATuple = aTuple match {
    case (1, 1) =>
    case (something, 2) => s"I've found $something"
  }

  val nestedTuple = (1, (2, 3))
  val matchANestedTuple = nestedTuple match {
    case (_, (2, v)) =>
  }
  // PMs can be NESTED!

  // 4 - case classes - constructor pattern
  // PMs can be nested with CCs as well
  val aList: MyList[Int] = Cons(1, Cons(2, Empty))
  val matchAList = aList match {
    case Empty =>
    case Cons(head, Cons(subhead, subtail)) =>
  }

  // 5 - list patterns
  val aStandardList = List(1,2,3,42)
  val standardListMatching = aStandardList match {
    case List(1, _, _, _) => "a"// extractor - advanced
    case List(1, _*) => "b"// list of arbitrary length - advanced
    case 1 :: List(2,3,42) => "c"// infix pattern
    case List(1,2,3) :+ 42 => "d"// infix pattern
    case _ => "e"
  }

  // 6 - type specifiers
  val unknown: Any = 2
  val unknownMatch = unknown match {
    case list: List[Int] => // explicit type scpecifier
    case _ =>
  }

  // 7 - name binding
  val nameBindingMatch = aList match {
    case nonEmptyList @ Cons(_, _) => nonEmptyList.h// name binding => use the name later(here)
    case Cons(1, rest @ Cons(2, _)) => // name binding inside nested patterns
  }

  println(nameBindingMatch)

  // 8 - multi-patterns
  val multipattern = aList match {
    case Empty | Cons(0, _) => // compound patter (multi-pattern)
    case _ =>
  }

  // 9 - if guards
  val secondElementSpecial = aList match {
    case Cons(_, Cons(specialElement, _)) if specialElement % 2 == 0 =>
  }

  // ALL.

    /*
      Question.
     */

  val numbers = List(1,2,3)
  val numbersMatch = numbers match {
    case listOfStrings: List[String] => "a list of strings" // Trick => type-erasure
    case licstOfNumbers: List[Int] => "a list of numbers"
    case _ => ""
  }

  println(numbersMatch)
  // JVM trick question
}
