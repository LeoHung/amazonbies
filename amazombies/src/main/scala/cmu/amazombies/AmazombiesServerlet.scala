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
import scala.concurrent.Future
import scala.concurrent.Await
import scala.collection.concurrent.TrieMap

object MySQLFactory{
  var mysqlConn:Connection = null
  def getMySQLConn() ={
    if(mysqlConn == null){
      val mysql_url="54.173.36.37"
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



object WarmUp{
  val publickey:BigInt = BigInt("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153")
  def warmUpQ1Map(q1Map :TrieMap[String, String], k :Int){
    var i =0;
    for(i <- 0 to k){
      val number_str = (BigInt(i) * publickey).toString()
      q1Map.put(number_str, BigInt(i).toString())
    }
  }
}


class AmazombiesServerlet extends AmazombiesStack  {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val scalaCache = ScalaCache(MemcachedCache("localhost:11211"))
  val q1Map:TrieMap[String, String] = new TrieMap[String, String]
  val publickey:BigInt = BigInt("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153")

  def warmUpQ1Map(){
    WarmUp.warmUpQ1Map(q1Map, 1000000)
  }

  // q1?key=20630300497055296189489132603428150008912572451445788755351067609550255501160184017902946173672156459
  get("/q1") {
    val key_str:String = params("key")
    val today = Calendar.getInstance().getTime()
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss")

    val number_str = q1Map.get(key_str) match{
      case Some(x) => x
      case None => {
        val key:BigInt = BigInt.apply(key_str)
        val number = key/publickey
        q1Map.put(key_str, number.toString())
        number.toString()
      }
    }

    number_str + "\n" + "Amazombies,jiajunwa,chiz2,sanchuah\n" + timeFormat.format(today)
  }

  get("/sql/q2"){
    val userid:String = params("userid")
    val tweet_time:String = params("tweet_time").replace(' ', '+')
    val row_key:String = userid+"_"+tweet_time

    Await.result(scalacache.get[String](row_key), 1.minute) getOrElse {
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
      val page = "Amazombies,jiajunwa,chiz2,sanchuah\n"+ content
      scalacache.put(row_key)(page, Some(1.hour))
      page
    }
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
