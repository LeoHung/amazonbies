package com.mastertheboss.undertow;

import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.*;
import io.undertow.server.handlers.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.math.BigInteger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.concurrent.*;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

class SQLConnection{
    static Connection mysqlConn=null;

    public static Connection getSQLConnection(){

        if(SQLConnection.mysqlConn == null){

            String mysql_url="54.173.36.37";
            String mysql_db="tweet";
            String mysql_user="root";
            String mysql_password="db15319root";

            String driver="com.mysql.jdbc.Driver";
            try {
                // make the connection
                Class.forName(driver);
                String jdbc_url = "jdbc:mysql://" + mysql_url + ":3306/"+mysql_db+"?useUnicode=true&characterEncoding=UTF-8";
                SQLConnection.mysqlConn = DriverManager.getConnection(jdbc_url, mysql_user, mysql_password);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        return SQLConnection.mysqlConn;
    }
}

class HBaseConnection{
    public static HTable getQ2Table(){

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(System.getenv("HBASE_SETTING_FILE"));
        HTable table = null;
        try{
            table = new HTable(conf, "tweets");
        }catch( Exception e ){
            e.printStackTrace();
        }
        return table;
    }

    public static HTable getQ3Table(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","54.164.123.142");
        HTable table = null;
        try{
            table = new HTable(conf, "tweets_q3");
        }catch(Exception e){
            e.printStackTrace();
        }
        return table;
    }
}

/**
 * Hello world!
 *
 */
public class App {

    final static BigInteger publicKey= new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");

    public static void warmUpQ1(ConcurrentMap<String,String> q1Cache, BigInteger k){
        BigInteger key = new BigInteger("0");
        BigInteger number = new BigInteger("0");
        while(k.compareTo(number) > 0){
            q1Cache.put(key.toString(), number.toString());
            System.out.println("key: "+ key +" Number: " + number);
            number = number.add(new BigInteger("1"));
            key =  key.add(publicKey);
        }
    }

    public static void main(final String[] args) {

        // HeartBeat
        HttpHandler helloworld = new HttpHandler() {
                    public void handleRequest(final HttpServerExchange exchange)
                            throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                                "text/plain");
                        exchange.getResponseSender().send("Mom. I'm alive.");
                        }
                    };

        // Q1
        final ConcurrentMap<String,String> q1Cache = new ConcurrentHashMap<String,String>();
        warmUpQ1(q1Cache, new BigInteger("1000"));
        final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        HttpHandler q1Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String key_str = exchange.getQueryParameters().get("key").getFirst();
                String numberStr = q1Cache.get(key_str);

                if(numberStr == null){
                    BigInteger key = new BigInteger(key_str);
                    BigInteger number = key.divide(publicKey);
                    numberStr = number.toString();
                    q1Cache.put(key_str, numberStr);
                }
                String output = String.format(
                    "%s\nAmazombies,jiajunwa,chiz2,sanchuah\n%s",
                    numberStr,
                    timeFormat.format(Calendar.getInstance().getTime())
                );

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.getResponseSender().send(output);
            }
        };

        // Q2
        final ConcurrentMap<String,String> sqlCache = new ConcurrentHashMap<String,String>();
        final Connection sqlConn = SQLConnection.getSQLConnection();
        HttpHandler q2SQLHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String tweet_time = exchange.getQueryParameters().get("tweet_time").getFirst().replace(" ", "+");
                String row_key = userid+"_"+tweet_time;

                String cachePage = sqlCache.get(row_key);
                String page =null;
                if(cachePage == null){
                    try{
                        Statement statement = sqlConn.createStatement();
                        String sql_query = "select tweetId, sentimentScore, censoredText from tweets_phase1 where userIdtime='"+row_key+"'";
                        ResultSet resultSet = statement.executeQuery(sql_query);
                        String content = "";
                        while ( resultSet.next() ) {
                            String tweetId = resultSet.getString("tweetId");
                            Integer sentimentScore = resultSet.getInt("sentimentScore");
                            String censoredText = resultSet.getString("censoredText");
                            content += (tweetId +":"+sentimentScore+":"+censoredText+";");
                        }
                        page = "Amazombies,jiajunwa,chiz2,sanchuah\n"+ content;
                        sqlCache.put(row_key, page);
                    }catch(Exception e ){
                        e.printStackTrace();
                    }
                }else{
                    page = cachePage;
                }

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                exchange.getResponseSender().send(page);
            }
        };

        // Q3
        // /q3?userid=2495192362
        final ConcurrentMap<String,String> q3Cache = new ConcurrentHashMap<String,String>();
        final HTable q3Table = HBaseConnection.getQ3Table();
        HttpHandler q3Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {

                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String page = q3Cache.get(userid);
                if(page ==null){
                    Get g = new Get(Bytes.toBytes(userid));
                    Result r = q3Table.get(g);
                    byte [] value = r.getValue(
                            Bytes.toBytes("cfmain"),
                            Bytes.toBytes("retweetids")
                    );
                    String valueStr = Bytes.toString(value);

                    page = valueStr.replace(",","\n");
                    q3Cache.put(userid, page);
                }

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                exchange.getResponseSender().send(page);
            }
        };


        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);
        pathhandler.addPrefixPath("/sql/q2", q2SQLHandler);
        pathhandler.addPrefixPath("/q3", q3Handler);

        pathhandler.addPrefixPath("/", helloworld);

        int port = 8080;
        if(System.getenv("PORT") != null){
            port = Integer.parseInt(System.getenv("PORT"));
            System.out.println("Run On: "+ port);
        }

        Undertow server = Undertow.builder().addHttpListener(port, "localhost")
                .setHandler(pathhandler).build();
        server.start();
    }
}
