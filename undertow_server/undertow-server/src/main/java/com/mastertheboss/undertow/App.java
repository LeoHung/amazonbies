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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;

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
        // Configuration conf = HBaseConfiguration.create();
        // conf.addResource(System.getenv("HBASE_SETTING_FILE"));
        // HTable table = null;
        // try{
        //     table = new HTable(conf, "tweets");
        // }catch( Exception e ){
        //     e.printStackTrace();
        // }
        // return table;
        return null;
    }

    public static HConnection getHBConnection(String hbaseIp){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",hbaseIp);

        HConnection connection = null;
        try{
            connection = HConnectionManager.createConnection(conf);
        }catch(Exception e){
            e.printStackTrace();
        }
        return connection;
    }
}

/**
 * Hello world!
 *
 */
public class App {

    final static String teamLine = "Amazombies,jiajunwa,chiz2,sanchuah";

    final static BigInteger publicKey= new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");

    public static void warmUpQ1(ConcurrentMap<String,String> q1Cache, BigInteger k){
        BigInteger key = new BigInteger("0");
        BigInteger number = new BigInteger("0");
        while(k.compareTo(number) > 0){
            q1Cache.put(key.toString(), number.toString());
            //System.out.println("key: "+ key +" Number: " + number);
            number = number.add(new BigInteger("1"));
            key =  key.add(publicKey);
        }
	System.out.println("total: "+ number);
    }

    public static void warmUpQ3(ConcurrentMap<String,String> q3Cache, String q3File){
        try{
            BufferedReader bf = new BufferedReader(new FileReader(q3File));
            String line = null;
            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String userid = tmp[0];
                String retweetids = tmp[1];
                String text = teamLine + "\n" + retweetids.replace(",", "\n");
                q3Cache.put(userid, text);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {

        //parameters
        String warmUpQ1Num = System.getenv("WARMUPQ1NUM");
        int port = 8080;
        if(System.getenv("PORT") != null){
            port = Integer.parseInt(System.getenv("PORT"));
            System.out.println("Run On: "+ port);
        }
        String hbaseIp = System.getenv("HBASEIP");
        String q3WarmUpFile = System.getenv("WARMUPQ3FILE");


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
        // q1?key=20630300497055296189489132603428150008912572451445788755351067609550255501160184017902946173672156459
        final ConcurrentMap<String,String> q1Cache = new ConcurrentHashMap<String,String>();
        warmUpQ1(q1Cache, new BigInteger(warmUpQ1Num));
        final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        HttpHandler q1Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String key_str = exchange.getQueryParameters().get("key").getFirst();
                String numberStr = q1Cache.get(key_str);
        		boolean isCached = (numberStr != null);
                if(numberStr == null){
                    BigInteger key = new BigInteger(key_str);
                    BigInteger number = key.divide(publicKey);
                    numberStr = number.toString();
                }

                String output = String.format(
                    "%s\nAmazombies,jiajunwa,chiz2,sanchuah\n%s",
                    numberStr,
                    timeFormat.format(Calendar.getInstance().getTime())
                );

                exchange.getResponseSender().send(output);

        		if(!isCached){
                    q1Cache.put(key_str, numberStr);
        		}
            }
        };

        HttpHandler q1SaveHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange) throws Exception{
                String timeStr = timeFormat.format(Calendar.getInstance().getTime());
                String filename = "/tmp/q1Query_"+ timeStr +".txt";

                PrintStream ps = new PrintStream(filename);
                for(String key_str: q1Cache.keySet()){
                    ps.println(key_str + "\t" + q1Cache.get(key_str));
                }
                ps.close();
                exchange.getResponseSender().send("Finished..." + filename);
            }
        };

        // Q2 sql
    	System.out.println("Q2 SQL...start");
        final ConcurrentMap<String,String> sqlCache = new ConcurrentHashMap<String,String>();
        //final Connection sqlConn = SQLConnection.getSQLConnection();
        final Connection sqlConn = null;
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
                        page = teamLine + "\n" + content;
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

        // Q2
    	System.out.println("Q2 hbse: start");
        final ConcurrentMap<String,String> q2HbaseCache = new ConcurrentHashMap<String,String>();
        //final HConnection q2hbaseConnection = HBaseConnection.getHBConnection(hbaseIp);
        final HConnection q2hbaseConnection = null;
        HttpHandler q2HbaseHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String tweet_time = exchange.getQueryParameters().get("tweet_time").getFirst().replace(" ", "+");
                String row_key = userid + "_" + tweet_time;

                String cachePage = q2HbaseCache.get(row_key);
                String page =null;
                if(cachePage == null){
                    HTableInterface q2HbaseTable = q2hbaseConnection.getTable("tweets");

                    Get g = new Get(Bytes.toBytes(row_key));
                    Result r = q2HbaseTable.get(g);

                    page = teamLine +"\n";
                    if(!r.isEmpty()){
                        byte [] tweetidByte = r.getValue(
                            Bytes.toBytes("cfmain"),
                            Bytes.toBytes("tweetId")
                        );
                        String tweetidStr = Bytes.toString(tweetidByte);

                        byte [] scoreByte = r.getValue(
                            Bytes.toBytes("cfmain"),
                            Bytes.toBytes("sentimentScore")
                        );
                        String scoreInt = Bytes.toString(scoreByte);

                        byte [] jsonByte = r.getValue(
                                Bytes.toBytes("cfmain"),
                                Bytes.toBytes("censoredText")
                        );
                        String jsonStr = Bytes.toString(jsonByte);
                        JSONObject textObj = new JSONObject(jsonStr);
                        String textStr = textObj.getString("ct");
                        page += tweetidStr + ":" + scoreInt + ":" + textStr +"\n";
                    }else{
                        page += "";
                    }
                    q2HbaseCache.put(userid, page);
                    q2HbaseTable.close();
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
    	System.out.println("Q3: start");
        final ConcurrentMap<String,String> q3Cache = new ConcurrentHashMap<String,String>();
        warmUpQ3(q3Cache, q3WarmUpFile);
        //final HConnection q3connection = HBaseConnection.getHBConnection(hbaseIp);
        final HConnection q3connection = null;
        HttpHandler q3Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String page = q3Cache.get(userid);
                if(page ==null){
                    HTableInterface q3Table = q3connection.getTable("tweetsq3");

                    Get g = new Get(Bytes.toBytes(userid));
                    Result r = q3Table.get(g);

                    page = teamLine +"\n";
                    if(!r.isEmpty()){
                        byte [] value = r.getValue(
                                Bytes.toBytes("cfmain"),
                                Bytes.toBytes("retweetids")
                        );
                        String valueStr = Bytes.toString(value);

                        page += (valueStr.replace(",","\n")+"\n");
                    }else{
                        page += "";
                    }

                    q3Cache.put(userid, page);
                    q3Table.close();
                }

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                exchange.getResponseSender().send(page);
            }
        };

        // Q4
        // q4?date=2013-01-01&location=Pittsburgh&m=1&n=3
        // final ConcurrentMap<String,String> q4Cache = new ConcurrentHashMap<String,String>();
        // final HConnection q4connection = HBaseConnection.getHBConnection();
        // HttpHandler q4Handler = new HttpHandler(){
        //     public void handleRequest(final HttpServerExchange exchange)
        //             throws Exception {
        //         String date = exchange.getQueryParameters().get("date").getFirst();
        //         String location = exchange.getQueryParameters().get("location").getFirst();
        //         String m = exchange.getQueryParameters().get("m").getFirst();
        //         String n = exchange.getQueryParameters().get("n").getFirst();

        //         String page = q4Cache.get(date+"_"+location);
        //         if(page ==null){
        //             HTableInterface q4Table = q4connection.getTable("tweetsq4");
        //             Get g = new Get(Bytes.toBytes(userid));
        //             Result r = q3Table.get(g);

        //             page = teamLine +"\n";
        //             if(!r.isEmpty()){
        //                 byte [] value = r.getValue(
        //                         Bytes.toBytes("cfmain"),
        //                         Bytes.toBytes("retweetids")
        //                 );
        //                 String valueStr = Bytes.toString(value);

        //                 page += valueStr.replace(",","\n");
        //             }else{
        //                 page += "";
        //             }
        //             q4Cache.put(userid, page);
        //             q4Table.close();
        //         }

        //         exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
        //                 "text/plain");
        //         exchange.getResponseSender().send(page);
        //     }
        // };


        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);
        pathhandler.addPrefixPath("/q1Save", q1SaveHandler);
        pathhandler.addPrefixPath("/q2", q2HbaseHandler);
        pathhandler.addPrefixPath("/sql/q2", q2SQLHandler);
        pathhandler.addPrefixPath("/q3", q3Handler);



        pathhandler.addPrefixPath("/", helloworld);


        Undertow server = Undertow.builder().addHttpListener(port, "0.0.0.0")
                .setHandler(pathhandler).build();
        server.start();
    }
}
