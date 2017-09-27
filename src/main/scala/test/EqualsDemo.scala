package test

import java.util.Date
import java.text.SimpleDateFormat


object EqualsDemo {
  def test={
    val a="dfsaf"
    val b="dfsaf"
    println(a.equals(b))
  }
  def main(args: Array[String]): Unit = {
println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }
  val now = new Date()  
  def getCurrent_time():Long = {  
        val a = now  
        var str = a+""  
        
        str.toLong  
  }
}