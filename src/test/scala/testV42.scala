package scala

/**
  * Created by 428900 on 2017/5/18.
  */
object testV42 {
  def main(args: Array[String]): Unit = {
    val v = 42
    Some(42) match {
      case Some(`v`) => println("这是42")

      case _ => println("不是42")

    }
  }

}
