package part1recap

import part1recap.ScalaRecap.anIfExpression

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  //values and variables
  private val aBoolean: Boolean = false

  //expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"
  println(anIfExpression)

  //instructions vs expressions
  val theUnit: Unit = println("Hello, Scala") //Printing something to the console returns a `Unit` type

  //functions
  def myFunction(x: Int): Int = 42

  //OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Croc extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crunch")
  }

  //Singleton pattern
  object MySingleton
  //companions

  object Carnivore

  //generics
  trait MyList[A]

  //method notation
  val x = 1 + 2
  val y = 1.+(2)

  //functional programming
  val incremeneter: Int => Int = x => x + 1
  val incremeneted = incremeneter(42)

  //map, flatMap, filter
  val processedList = List(1, 2, 3).map(incremeneter)

  //pattern matching
  private val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  //try-catch
  try {
    //some code
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned value"
    case _ => "something else"
  }

  //Future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    //some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I found the $meaningOfLife")
    case Failure(ex) => println(s"I have failed: $ex")
  }

  //partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  //implicits
  //auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implictInt = 67

  val implicitCall = methodWithImplicitArgument

  //implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hello, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet //fromStringToPerson("Bob").greet

  //implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("bark!")
  }

  "Lassie".bark

  /*
   - local scope
   - imported scope
   - companion objects of the types involved in the method call
   */
  List(1, 2, 3).sorted

}

