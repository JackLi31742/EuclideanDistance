package test

import scala.Ordering

object listTest {
  def main(args: Array[String]) {
    val s = List("a", "d", "F", "B", "e")
    val n = List(3, 7, 2, 1, 5)
    /* sort alphabetical and ignoring case */
    def compfn1(e1: String, e2: String) = (e1 compareToIgnoreCase e2) < 0
    val sorted = s.sortWith(compfn1)
//    sorted.foreach(println)
    
    
    val points1 = List(Array(("a", 9.0)), Array(("e", 7.0)), Array(("b", 5.6)), Array(("d", 6.3)))
    val points1sort=points1.sortBy(f⇒(f(0)._2))(Ordering.Double.reverse)
    points1sort.foreach(f⇒f.foreach(println))
    
    def com(arr1:Array[(String, Int)],arr2:Array[(String, Int)]):Int={
    		var int1= arr1.apply(0)._2
    				var int2= arr2.apply(0)._2
    				if (int1 > int2) {
    					return -1;
    				} else if (int1 == int2) {
    					return 0;
    				} else {
    					return 0;
    				}
    }
  }
}