package lesson_1_scala_recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App{

//  values
  val aBoolean: Boolean = false

//  expresions

  val anIfExpression = if(2>3) "bigger" else "smaller"

//  instructions
  def myFunction(x: Int) = {42}

//  OOP
  class Animal
  class Cat extends Animal
  trait Carnivore{
    def eat(animal: Animal): Unit
  }

  class Croc extends Animal with Carnivore{
    override def eat(animal: Animal): Unit = println("moch moch")
  }

//  singleton pattern
  object MySingleton
//companions
  object Carnivore

//  generic
  trait MyList[A]

//  method notaion
  val x = 1 +2
  val y = 1.+(2)

//  functional programing
  val incrementor: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(v1: Int): Int = v1 + 1
  }
  val incrementor2: Int => Int = x => x + 1

  val processedList = List(1,2,3).map(incrementor2)
  println(processedList)

//  pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "Some returned value"
    case _ => "sth else"
  }

//  Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation running on another thread
    42
  }

    aFuture.onComplete {
      case Success(meaningOfLife) => println(s"I,ve found $meaningOfLife")
      case Failure(ex) => println(s"I have failed $ex")
    }

  // Partial function
  val aPartialFunction = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  val aPartialFunction2 : PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

//  Implicits
//  auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  println(methodWithImplicitArgument) // <- it is same like methodWithImplicitArgument(67)

  case class Person(name: String){
    def greet(): Unit = println(s"Hi my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "bob".greet() // fromStringToPerson("Bob").greet()

//  implicit conversiuon - implicit classes
  implicit class Dog(name: String) {
  def bark(): Unit = println("Wrrrr")
  }
  "Jecki".bark()

  /*
  local scope
  imported scope
  companion objects of the types in the method call
   */

}

