package com.mastertheboss.undertow;

import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.*;
import io.undertow.server.handlers.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.math.BigInteger;
import java.util.Date;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.concurrent.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;

import java.math.BigDecimal;

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

import java.io.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Base64;

import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

import com.mastertheboss.undertow.peer.PeerServer;
import com.mastertheboss.undertow.cache.*;
import com.mastertheboss.undertow.myutils.*;
import com.mastertheboss.undertow.connection.*;


/**
 * Hello world!
 *
 */
public class App {

    final static String teamLine = "Amazombies,jiajunwa,chiz2,sanchuah";
    final static int teamLineLength = teamLine.length();
    final static BigInteger publicKey= new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");

    static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
    public static boolean isDate(String dateStr)throws Exception{
        try{
            if(fmt.parse(dateStr) == null){return false;}
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static boolean isLong(String longStr){
        try{
            Long.parseLong(longStr);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static void warmUpQ1(ConcurrentMap<String,String> q1Cache, BigInteger k){
        BigInteger key = new BigInteger("0");
        BigInteger number = new BigInteger("0");
        while(k.compareTo(number) > 0){
            q1Cache.put(key.toString(), number.toString());
            number = number.add(new BigInteger("1"));
            key =  key.add(publicKey);
        }
    	System.out.println("total: "+ number);
    }

    public static void warmUpQ2(ConcurrentMap<String,String> q2Cache, HTableInterface q2table)throws Exception{
        String startRow = "1000494338_2014-05-01+15:57:51";
        String endRow = "1000494338_2014-05-01+15:57:51";

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
        byte[] familyByte = Bytes.toBytes("cfmain");
        byte[] tweetIdByte = Bytes.toBytes("tweetId");
        byte[] sentimentScoreByte = Bytes.toBytes("sentimentScore");
        byte[] censoredTextByte = Bytes.toBytes("censoredText");


        scan.addColumn(familyByte, tweetIdByte);
        scan.addColumn(familyByte, sentimentScoreByte);
        scan.addColumn(familyByte, censoredTextByte);
        ResultScanner rs = q2table.getScanner(scan);
        int i = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            if( i % 1000 == 0 ) { System.out.println("Q2 loading: " + i ); }
            i++;
            byte[] rowObj = r.getRow();
            byte[] tweetidObj = r.getValue(familyByte, tweetIdByte);
            byte[] sentimentScoreObj = r.getValue(familyByte, sentimentScoreByte);
            byte[] censoredTextObj = r.getValue(familyByte, censoredTextByte);
            JSONObject textObj = new JSONObject(new String(censoredTextObj));
            String textStr = textObj.getString("ct");

            StringBuilder sb = new StringBuilder(teamLine);
            sb.append("\n");
            sb.append(new String(tweetidObj));
            sb.append(":");
            sb.append(new String(sentimentScoreObj));
            sb.append(":");
            sb.append(textObj);
            sb.append("\n");
            q2Cache.put(new String(rowObj), sb.toString());
        }
    }

    public static void warmUpQ3ConncurrentMap(ConcurrentMap<String, String> q3Cache, String q3File){
        try{
            BufferedReader bf = new BufferedReader(new FileReader(q3File));
            String line = null;
            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String userid = tmp[0];
                String retweetids = tmp[1];
                q3Cache.put(userid, retweetids);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Q3 total: "+ q3Cache.size());
    }

    public static void warmUpQ3(Q3Cache q3Cache, String q3File){
        try{
            BufferedReader bf = new BufferedReader(new FileReader(q3File));
            String line = null;
            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String userid = tmp[0];
                String retweetids = tmp[1];
                q3Cache.put(userid, retweetids);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Q3 total: "+ q3Cache.size());
    }

    public static void warmUpQ4Cache(Q4Cache q4Cache, String q4File){
        try{
            BufferedReader bf = new BufferedReader(new FileReader(q4File));
            String line = null;

            while((line = bf.readLine()) != null){
                String[] tmp = line.trim().split("\t");
                String[] tmp2 = tmp[0].split("_");

                String locationStr ="";
                for( int i =0 ;i < tmp2.length -2 ; i++){
                    if(i >0){
                        locationStr += "_";
                    }
                    locationStr += tmp2[i];
                }
                String timeStr = tmp2[tmp2.length-2];
                String rankStr = tmp2[tmp2.length-1];

                String tagTweetids = tmp[1];
                q4Cache.put(locationStr, timeStr, rankStr, tagTweetids);
            }

        }catch(Exception e ){
            e.printStackTrace();
        }
        System.out.println("Q4 total: " + q4Cache.size());
    }

    public static void warmUpQ4(ConcurrentMap<String, Vector<String>> q4Cache, String q4File){
        try{
            BufferedReader bf = new BufferedReader(new FileReader(q4File));
            String line = null;

            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String locationDateRank = tmp[0];
                String retweet = tmp[1];
                String[] tmp2 = locationDateRank.split("_");
                String location ="";
                for( int i =0 ;i < tmp2.length -2 ; i++){
                    if(i >0){
                        location += "_";
                    }
                    location += tmp2[i];
                }
                String date = tmp2[tmp2.length-2];
                int rank = Integer.parseInt(tmp2[tmp2.length-1]);

                String locationDate = location +"_"+ date;
                if(!q4Cache.containsKey(locationDate)){
                    q4Cache.put(locationDate, new Vector<String>());
                }
                Vector<String> l = q4Cache.get(locationDate);
                if( (rank-1) >= l.size()){
                    l.setSize(rank-1+1);
                }
                l.insertElementAt(retweet, (rank-1));
            }

        }catch(Exception e ){
            e.printStackTrace();
        }
        System.out.println("Q4 total: " + q4Cache.size());
    }

    public static void showEnvParams(
        int port, String hbaseIp, String q3WarmUpFile, String q4WarmUpFile,
        String nodeType, String q3ServerIP, String q4ServerIP){

        System.out.println("Port: "+port);
        System.out.println("hbaseIp: "+hbaseIp);
        System.out.println("q3WarmUpFile: "+q3WarmUpFile);
        System.out.println("q4WarmUpFile: "+q4WarmUpFile);
        System.out.println("nodeType: "+nodeType);
        System.out.println("q3ServerIP: "+q3ServerIP);
        System.out.println("q4ServerIP: "+q4ServerIP);

    }

    public static void main(final String[] args) throws Exception{

        //parameters
        String warmUpQ1Num = System.getenv("WARMUPQ1NUM");
        int port = 8080;
        if(System.getenv("PORT") != null){
            port = Integer.parseInt(System.getenv("PORT"));
        }
        String hbaseIp = System.getenv("HBASEIP");
        String mysqlIp = System.getenv("MYSQLIP");
        String q3WarmUpFile = System.getenv("WARMUPQ3FILE");
        String q4WarmUpFile = System.getenv("WARMUPQ4FILE");
        final String nodeType = System.getenv("NODETYPE"); // nodeType = Q3 or Q4
        String q3ServerIP = System.getenv("Q3SERVERIP"); //Q3 server ip
        String q4ServerIP = System.getenv("Q4SERVERIP"); //Q4 server ip

        showEnvParams(port, hbaseIp, q3WarmUpFile, q4WarmUpFile, nodeType, q3ServerIP, q4ServerIP);

        // HeartBeat
        HttpHandler helloworld = new HttpHandler() {
                    public void handleRequest(final HttpServerExchange exchange)
                            throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                                "text/plain");
                        exchange.getResponseSender().send("Mom. I'm alive.");
                        }
                    };

        // Q1 handler
        // q1?key=20630300497055296189489132603428150008912572451445788755351067609550255501160184017902946173672156459
        final ConcurrentMap<String,String> q1Cache = new ConcurrentHashMap<String,String>(7944108 );
        final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        HttpHandler q1Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String key_str = exchange.getQueryParameters().get("key").getFirst();
                String numberStr = q1Cache.get(key_str);
        		//boolean isCached = (numberStr != null);
                //if(numberStr == null){
                    BigInteger key = new BigInteger(key_str);
                    BigInteger number = key.divide(publicKey);
                    numberStr = number.toString();
                //}

                StringBuilder sb = new StringBuilder();
                sb.append(numberStr);
                sb.append("\n");
                sb.append("Amazombies,jiajunwa,chiz2,sanchuah");
                sb.append("\n");
                sb.append(timeFormat.format(Calendar.getInstance().getTime()));

                exchange.getResponseSender().send(sb.toString());

        		//if(!isCached){
                //    q1Cache.put(key_str, numberStr);
        		//}
            }
        };

        // For testing
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

        // Q2 sql handler
    	System.out.println("Q2 SQL...start");
        final ConcurrentMap<String,String> sqlCache = new ConcurrentHashMap<String,String>();
        final Connection sqlConn = SQLConnection.getSQLConnection(mysqlIp);
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

        // Q2 handler
        // q2?userid=1473664038&tweet_time=2014-04-08+02:43:27
    	System.out.println("Q2 hbse: start");
        final ConcurrentMap<String,String> q2HbaseCache = new ConcurrentHashMap<String,String>();
        final HConnection q2hbaseConnection = HBaseConnection.getHBConnection(hbaseIp);
        final Q2IndexConvertor q2IndexConvertor = new Q2IndexConvertor();

        final byte[] q2familyByte = Bytes.toBytes("cfmain");
        final byte[] tweetIdByte = Bytes.toBytes("tweetId");
        final byte[] sentimentScoreByte = Bytes.toBytes("sentimentScore");
        final byte[] censoredTextByte = Bytes.toBytes("censoredText");

        HttpHandler q2HbaseHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String tweet_time = exchange.getQueryParameters().get("tweet_time").getFirst().replace(" ", "+");
                String row_key = userid + "_" + tweet_time;

                if(userid == null || !isLong(userid) || tweet_time == null || !isDate(tweet_time)){
                    exchange.getResponseSender().send(teamLine);
                    return;
                }

                String cachePage = q2HbaseCache.get(row_key);
                boolean isCached = (cachePage!= null);
                String page =null;
                if(!isCached){
                    HTableInterface q2HbaseTable = q2hbaseConnection.getTable("tweets");
                    Get g = new Get(Bytes.toBytes(row_key));
                    g.addColumn(q2familyByte, tweetIdByte);
                    g.addColumn(q2familyByte, sentimentScoreByte);
                    g.addColumn(q2familyByte, censoredTextByte);

                    Result r = q2HbaseTable.get(g);
                    StringBuilder sb = new StringBuilder(teamLine);
                    sb.append("\n");
                    if(!r.isEmpty()){
                        byte [] tweetidByte = r.getValue(
                            q2familyByte,
                            tweetIdByte
                        );
                        String tweetidStr = Bytes.toString(tweetidByte);

                        byte [] scoreByte = r.getValue(
                            q2familyByte,
                            sentimentScoreByte
                        );
                        String scoreInt = Bytes.toString(scoreByte);

                        byte [] jsonByte = r.getValue(
                            q2familyByte,
                            censoredTextByte
                        );
                        String jsonStr = Bytes.toString(jsonByte);
                        JSONObject textObj = new JSONObject(jsonStr);
                        String textStr = textObj.getString("ct");

                        sb.append(tweetidStr);
                        sb.append(":");
                        sb.append(scoreInt);
                        sb.append(":");
                        sb.append(textStr);
                        sb.append("\n");
                        page = sb.toString();
                    }else{
                        sb.append("");
                        page = sb.toString();
                    }

                    q2HbaseTable.close() ;
                }else{
                    page = cachePage;
                }

                exchange.getResponseSender().send(page);


                if(!isCached){
                    q2HbaseCache.put(userid, page);
                }
            }
        };

        // Q3 handler
        // q3?userid=2495192362
    	System.out.println("Q3: start");
        final Q3Cache q3Cache = new Q3Cache();
        System.out.println("Q3: warmup");
        warmUpQ3(q3Cache, q3WarmUpFile);
        System.out.println("Q3: get connection");
        final HConnection q3connection = HBaseConnection.getHBConnection(hbaseIp);
        final PeerServer q3Server = new PeerServer(q3ServerIP);
        HttpHandler q3Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();

                if(!nodeType.equals("Q3")){
                        String body = q3Server.getQ3(userid);
                        if(body == null){body = teamLine;}
                        exchange.getResponseSender().send(body);
                }else{
                    String retweetids = q3Cache.get(userid);
                    boolean isCached = (retweetids != null);
                    HTableInterface q3Table = null;
                    if(!isCached){
                        q3Table = q3connection.getTable("tweetsq3");
                        Get g = new Get(Bytes.toBytes(userid));
                        Result r = q3Table.get(g);

                        if(!r.isEmpty()){
                            byte [] retweeidsByte = r.getValue(
                                    Bytes.toBytes("cfmain"),
                                    Bytes.toBytes("retweetids")
                            );
                            retweetids = Bytes.toString(retweeidsByte);
                            retweetids = retweetids.replace(",", "\n");
                        }else{
                            retweetids = "";
                        }
                    }

                    int outputLength = teamLineLength + 2 + retweetids.length();
                    StringBuilder sb = new StringBuilder(outputLength);
                    sb.append(teamLine);
                    sb.append("\n");
                    sb.append(retweetids);
                    sb.append("\n");

                    exchange.getResponseSender().send(sb.toString());

                    q3Table.close();
                }
            }
        };

        // Q4 handler
        // q4?date=2014-05-22&location=Aalborg&m=1&n=3
        final HConnection q4connection = HBaseConnection.getHBConnection(hbaseIp);
        final ConcurrentMap<String, Vector<String>> warmUpQ4cache = new ConcurrentHashMap<String, Vector<String>>();
        final PeerServer q4Server = new PeerServer(q4ServerIP);

        // System.out.println("Q4 warmup: ");
        // warmUpQ4(warmUpQ4cache, q4WarmUpFile);
        // HttpHandler q4Handler = new HttpHandler(){
        //      public void handleRequest(final HttpServerExchange exchange)
        //              throws Exception {
        //         String date = exchange.getQueryParameters().get("date").getFirst();
        //         String location = exchange.getQueryParameters().get("location").getFirst();
        //         String mStr = exchange.getQueryParameters().get("m").getFirst();
        //         int m = Integer.parseInt(mStr);
        //         String nStr = exchange.getQueryParameters().get("n").getFirst();
        //         int n = Integer.parseInt(nStr);

        //         if(!nodeType.equals("Q4")){
        //             String body = q4Server.getQ4(location, date, mStr, nStr);
        //             if(body == null){
        //                 body = teamLine + "\n";
        //             }
        //             exchange.getResponseSender().send(body);
        //         }else{

        //             StringBuilder sb = new StringBuilder();
        //             sb.append(teamLine);
        //             sb.append("\n");


        //             List<String> hashtagRetweets = warmUpQ4cache.get(location+"_"+date);

        //             boolean isCached = (hashtagRetweets != null);
        //             if(!isCached){
        //                 HTableInterface q4Table = q4connection.getTable("tweetsq4");
        //                 List<Get> batchGets = new ArrayList<Get>();
        //                 for(int i = m ; i <= n ; i++){
        //                     String rowKey = String.format("%s_%s_%d", location, date, i);
        //                     Get g = new Get(Bytes.toBytes(rowKey));
        //                     batchGets.add(g);
        //                 }

        //                 Result[] rArray = q4Table.get(batchGets);

        //                 for(Result r: rArray){
        //                     byte [] tagtweetid = r.getValue(
        //                                 Bytes.toBytes("cfmain"),
        //                                 Bytes.toBytes("tagtweetid")
        //                     );
        //                     sb.append(Bytes.toString(tagtweetid));
        //                     sb.append("\n");
        //                 }
        //             }else{
        //                 int hashtagRetweetsSize = hashtagRetweets.size();
        //                 for( int i = (m -1) ; i < hashtagRetweetsSize && i<= (n -1) ; i++){
        //                       String tagText = hashtagRetweets.get(i) ;
        //                       if(tagText != null){
        //                         sb.append(tagText);
        //                         sb.append("\n");
        //                       }
        //                 }
        //             }

        //             exchange.getResponseSender().send(sb.toString());
        //          }
        //      }
        // };

        // System.out.println("Q4 SQL warmup: ");
        // warmUpQ4(warmUpQ4cache, q4WarmUpFile);
        // HttpHandler q4SQLHandler = new HttpHandler(){
        //      public void handleRequest(final HttpServerExchange exchange)
        //              throws Exception {
        //         String date = exchange.getQueryParameters().get("date").getFirst();
        //         String location = exchange.getQueryParameters().get("location").getFirst();
        //         String mStr = exchange.getQueryParameters().get("m").getFirst();
        //         int m = Integer.parseInt(mStr);
        //         String nStr = exchange.getQueryParameters().get("n").getFirst();
        //         int n = Integer.parseInt(nStr);

        //         if(!nodeType.equals("Q4")){
        //             String body = q4Server.getQ4(location, date, mStr, nStr);
        //             if(body == null){
        //                 body = teamLine + "\n";
        //             }
        //             exchange.getResponseSender().send(body);
        //         }else{

        //             StringBuilder sb = new StringBuilder();
        //             sb.append(teamLine);
        //             sb.append("\n");


        //             List<String> hashtagRetweets = warmUpQ4cache.get(location+"_"+date);

        //             boolean isCached = (hashtagRetweets != null);
        //             if(!isCached){
        //                 HTableInterface q4Table = q4connection.getTable("tweetsq4");
        //                 List<Get> batchGets = new ArrayList<Get>();
        //                 for(int i = m ; i <= n ; i++){
        //                     String rowKey = String.format("%s_%s_%d", location, date, i);
        //                     int rowKeyHash = rowKey.hashCode();

        //                 }
        //             }else{
        //                 int hashtagRetweetsSize = hashtagRetweets.size();
        //                 for( int i = (m -1) ; i < hashtagRetweetsSize && i<= (n -1) ; i++){
        //                       String tagText = hashtagRetweets.get(i) ;
        //                       if(tagText != null){
        //                         sb.append(tagText);
        //                         sb.append("\n");
        //                       }
        //                 }
        //             }

        //             exchange.getResponseSender().send(sb.toString());
        //          }
        //      }
        // };


        System.out.println("Q5 SQL...start");
        // final ConcurrentMap<String,String> sqlCache = new ConcurrentHashMap<String,String>();
        // final Connection sqlConn = SQLConnection.getSQLConnection(mysqlIp);
        HttpHandler q5SQLHandler = new HttpHandler(){
            public String getWinner(String userA, Integer userAScore, String userB, Integer userBScore){
                if(userAScore == userBScore){
                    return "X";
                }
                if(userAScore > userBScore){
                    return userA;
                }else{
                    return userB;
                }
            }

            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userAId = exchange.getQueryParameters().get("m").getFirst();
                String userBId = exchange.getQueryParameters().get("n").getFirst();


                // String cachePage = sqlCache.get(row_key);
                // String page =null;
                // if(cachePage == null){
                String page = null;
                    try{
                        Statement statement = sqlConn.createStatement();
                        String sql_query = String.format("select userId, s1, s2, s3, total from q5 where userId = %s or userId = %s ", userAId, userBId);
                        ResultSet resultSet = statement.executeQuery(sql_query);
                        String content = "";

                        Integer userAs1 = 0;
                        Integer userAs2 = 0;
                        Integer userAs3 = 0;
                        Integer userATotal = 0;

                        Integer userBs1 = 0;
                        Integer userBs2 = 0;
                        Integer userBs3 = 0;
                        Integer userBTotal = 0;

                        while ( resultSet.next() ) {
                            BigDecimal userId = resultSet.getBigDecimal("userId");
                            String userIdStr = userId.toString();
                            if(userIdStr.equals(userAId)){
                                userAs1 = resultSet.getInt("s1");
                                userAs2 = resultSet.getInt("s2");
                                userAs3 = resultSet.getInt("s3");
                                userATotal = resultSet.getInt("total");
                            }else{
                                userBs1 = resultSet.getInt("s1");
                                userBs2 = resultSet.getInt("s2");
                                userBs3 = resultSet.getInt("s3");
                                userBTotal = resultSet.getInt("total");
                            }
                        }

                        page = String.format(
                            "%s\n"+
                            "%s\t%s\tWINNER\n"+
                            "%d\t%d\t%s\n" +
                            "%d\t%d\t%s\n" +
                            "%d\t%d\t%s\n" +
                            "%d\t%d\t%s\n",
                            teamLine,
                            userAId, userBId,
                            userAs1, userBs1, getWinner(userAId, userAs1, userBId, userBs1),
                            userAs2, userBs2, getWinner(userAId, userAs2, userBId, userBs2),
                            userAs3, userBs3, getWinner(userAId, userAs3, userBId, userBs3),
                            userATotal, userBTotal, getWinner(userAId, userATotal, userBId, userBTotal)
                        );
                        // sqlCache.put(row_key, page);
                    }catch(Exception e ){
                        e.printStackTrace();
                    }
                // }else{
                //     page = cachePage;
                // }

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                exchange.getResponseSender().send(page);
            }
        };


        HttpHandler q6SQLHandler = new HttpHandler(){

            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userAId = exchange.getQueryParameters().get("m").getFirst();
                String userBId = exchange.getQueryParameters().get("n").getFirst();

                // String cachePage = sqlCache.get(row_key);
                // String page =null;
                // if(cachePage == null){
                String page = null;
                    try{
                        Statement statement = sqlConn.createStatement();
                        //String sql_query = String.format("select count(cnt) as cnt from q6 where %s <= userId and userId <= %s ", userAId, userBId);
                        String sql_query = String.format("call q6query(%s,%s)", userAId, userBId);
                        ResultSet resultSet = statement.executeQuery(sql_query);

                        Integer cnt = 0;
                        while ( resultSet.next() ) {
                            cnt = resultSet.getInt("cnt");
                        }

                        page = String.format("%s\n%s\n", teamLine, cnt.toString());

                        // sqlCache.put(row_key, page);
                    }catch(Exception e ){
                        e.printStackTrace();
                    }
                // }else{
                //     page = cachePage;
                // }

                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                exchange.getResponseSender().send(page);
            }
        };



        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);
        pathhandler.addPrefixPath("/q2", q2HbaseHandler);
        pathhandler.addPrefixPath("/sql/q2", q2SQLHandler);
        pathhandler.addPrefixPath("/q3", q3Handler);
        // pathhandler.addPrefixPath("/q4", q4Handler);
        pathhandler.addPrefixPath("/q5", q5SQLHandler);
        pathhandler.addPrefixPath("/q6", q6SQLHandler);


        pathhandler.addPrefixPath("/", helloworld);


        Undertow server = Undertow.builder()
                .setWorkerThreads(8)
                .setIoThreads(4)
                .addHttpListener(port, "0.0.0.0")
                .setHandler(pathhandler).build();
        server.start();
    }
}
