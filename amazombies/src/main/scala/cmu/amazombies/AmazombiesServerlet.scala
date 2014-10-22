package cmu.amazombies

import org.scalatra._
import scalate.ScalateSupport
import scala.math.BigInt
import java.text.SimpleDateFormat
import java.util.Calendar

import java.sql.{Connection, DriverManager, ResultSet};
import scalacache._
import memcached._
import memoization._
import scala.concurrent.duration._


// object MyCache{
//   var cache:Map[String,String] = HashMap()
//   def getCache() ={
//     cache
//   }
// }

object MySQLFactory{

  var mysqlConn:Connection = null
  def getMySQLConn() ={
    if(mysqlConn == null){
      val mysql_url="54.172.202.199"
      val mysql_db="tweet"
      val mysql_user="root"
      val mysql_password="db15319root"
      val driver="com.mysql.jdbc.Driver"
      try {
          // make the connection
          Class.forName(driver)
          val jdbc_url = "jdbc:mysql://" + mysql_url + ":3306/"+mysql_db+"?useUnicode=true&characterEncoding=UTF-8"
          mysqlConn = DriverManager.getConnection(jdbc_url, mysql_user, mysql_password)
          // println("Initialize first time driver")
      }catch {
        case e: Exception => e.printStackTrace()
      }
    }
    mysqlConn
  }

}


// object Cache{
//   implicit val scalaCache = ScalaCache(MemcachedCache("localhost:11211"))
//   def getNumberString(key:String):String = {
//     scalaCache.cache.get(key)
//   }
//   def setNumberString(key:String, numberString:String) = {
//     scalaCache.cache.put(key, numberString, ttl=Duration(1, "days"))
//   }
// }

class AmazombiesServerlet extends AmazombiesStack {
  implicit val scalaCache = ScalaCache(MemcachedCache("localhost:11211"))


  // def getNumberString(key_str:String):String = memoize {
  //     val key:BigInt = BigInt.apply(key_str)
  //     val publickey:BigInt = BigInt.apply("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153")
  //     val number:BigInt = key/publickey
  //     val number_str:String = number.toString()
  //     val today = Calendar.getInstance().getTime()
  //     val timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss")

  //     number_str + "\nAmazombies,jiajunwa,chiz2,sanchuah\n" + timeFormat.format(today)
  // }

  // val memcache_url= "localhost:11211"

  get("/q1") {
    val key_str:String = params("key")
    val number_str:String = scalacache.get(key_str)

    // var number_str:String = Cache.getNumberString(key_str)
    if(number_str == None){
      val key:BigInt = BigInt.apply(key_str)
      val publickey:BigInt = BigInt.apply("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153")
      val number = key/publickey
      val number_str = number.toString()
      // Cache.setNumberString(key_str)(number_str)
      scalacache.put(key_str, number_str)
    }

    // val today = Calendar.getInstance().getTime()
    // val timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss")

    number_str + "\n" + "Amazombies,jiajunwa,chiz2,sanchuah\n" + timeFormat.format(today)
    // getNumberString(key_str)
  }

  get("/sql/q2"){
      val userid:String = params("userid")
      val tweet_time:String = params("tweet_time").replace(' ', '+')
      val row_key:String = userid+"_"+tweet_time

      // there's probably a better way to do this
      var content = ""

      try {
        // make the connection
        val conn = MySQLFactory.getMySQLConn()

        // create the statement, and run the select query
        val statement = conn.createStatement()
        val sql_query = "select tweetId, sentimentScore, censoredText from tweets_phase1 where userIdtime='"+row_key+"'"
        val resultSet = statement.executeQuery(sql_query)
        while ( resultSet.next() ) {
          val tweetId = resultSet.getString("tweetId")
          val sentimentScore = resultSet.getInt("sentimentScore")
          val censoredText = resultSet.getString("censoredText")
          content += (tweetId +":"+sentimentScore+":"+censoredText+";")
        }
      } catch {
        case e: Exception => println("exception caught in q2: " + e)
      }
      "Amazombies,jiajunwa,chiz2,sanchuah\n"+ content
  }

  get("/hbase/q2"){
    val userid:String = params("userid")
    val tweet_time:String = params("tweet_time").replace(' ', '+')
    val row_key:String = userid+"_"+tweet_time

    var content = ""

    try {
      // make the connection
      val conn = MySQLFactory.getMySQLConn()

      // create the statement, and run the select query
      val statement = conn.createStatement()
      val sql_query = "select tweetId, sentimentScore, censoredText from tweet.tweets_phase1 where userIdtime='"+row_key+"'"
      val resultSet = statement.executeQuery(sql_query)
      while ( resultSet.next() ) {
        val tweetId = resultSet.getString("tweetId")
        val sentimentScore = resultSet.getInt("sentimentScore")
        val censoredText = resultSet.getString("censoredText")
        content += (tweetId +":"+sentimentScore+":"+censoredText+";")
      }
    } catch {
      case e: Exception => println("exception caught in q2: " + e)
    }
    "Amazombies,jiajunwa,chiz2,sanchuah\n"+ content
  }



  get("/") {
    "Mom. I'm alive"
  }

}
