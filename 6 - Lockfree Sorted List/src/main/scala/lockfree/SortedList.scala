package lockfree

import scala.annotation.tailrec

class SortedList extends AbstractSortedList {

  // The sentinel node at the head.
  private val _head = createNode(0, None, isHead = true)

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.next

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Node, Option[Node]) = {
    @tailrec def iter(prev: Node, cur: Option[Node]): (Node, Option[Node]) = (prev, cur) match {
      case (before, Some(n)) =>
        if (n.deleted) {
          before.atomicState compareAndSet ((cur, false), (n.next, false))
          findNodeWithPrev(pred)
        }
        else if (pred(n.value)) (prev, cur)
        else iter(n, n.next)
      case (last, None) => (last, None)
    }
    iter(_head, firstNode)
  }

  // Insert an element in the list.
  def insert(e: Int): Unit = {
    val (prev, succ) = findNodeWithPrev(_ >= e)
    val newNode: Node = createNode(e, succ)
    if (!prev.atomicState.compareAndSet((succ, false), (Some(newNode), false))) insert(e)
  }

  // Checks if the list contains an element.
  def contains(e: Int): Boolean = findNodeWithPrev(_ == e)._2 match {
    case None => false
    case Some(n) => !n.deleted
  }

  // Delete an element from the list.
  // Should only delete one element when multiple occurences are present.
  def delete(e: Int): Boolean = findNodeWithPrev(_ == e)._2 match {
    case Some(toDelete) => if (toDelete.mark) true else delete(e)
    case None => false
  }
}
