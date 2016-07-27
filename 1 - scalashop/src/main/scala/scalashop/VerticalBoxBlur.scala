package scalashop

import org.scalameter._
import common._

import scala.collection.immutable.IndexedSeq

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur {

  /** Blurs the columns of the source image `src` into the destination image
    * `dst`, starting with `from` and ending with `end` (non-inclusive).
    *
    * Within each column, `blur` traverses the pixels by going from top to
    * bottom.
    */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    // TODO implement this method using the `boxBlurKernel` method
    //VERSION 1
    var column = from
    while (column < end) {
      var yy: Int = 0
      while (yy < src.height) {
        dst.update(column, yy, boxBlurKernel(src, column, yy, radius))
        yy += 1
      }
      column += 1
    }
  }

  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
    *
    * Parallelization is done by stripping the source image `src` into
    * `numTasks` separate strips, where each strip is composed of some number of
    * columns.
    */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    type stripe = (Int, Int)
    val numColinStripe: Int = math.ceil(src.width / numTasks.toDouble).toInt
    val stripes: IndexedSeq[stripe] = (0 until src.width) map { s =>
      if (s + numColinStripe >= src.width) (s, src.width)
      else (s, s + numColinStripe)
    }
    val tasks = stripes.map(s => (task {blur(src, dst, s._1, s._2, radius)}))
    tasks.foreach(_.join)
  }
}
