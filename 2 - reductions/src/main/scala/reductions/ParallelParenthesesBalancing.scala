package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    var parens: Int = 0

    chars foreach {c => {
      if (parens < 0) false
      else if (c == '(') parens += 1
      else if (c == ')') parens -= 1
    }}
    parens == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, leftParens: Int, rightParens: Int): (Int, Int) = {
      var i: Int = idx
      var left = 0
      var right = 0
      while (i < until) {
        if (chars(i) == '(') left += 1
        else if (chars(i) == ')' && left > 0) left -= 1
        else if (chars(i) == ')' && left <= 0) right += 1
        i += 1
      }
      (right, left)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val elementsCount: Int = until - from
      if (elementsCount <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid: Int = from + math.ceil( elementsCount / 2.0).toInt
        val ((i, j), (k, l)) = parallel(reduce(from, mid), reduce(mid, until))
        if (j > k) (l + j - k, i)
        else (l, i + k - j)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
