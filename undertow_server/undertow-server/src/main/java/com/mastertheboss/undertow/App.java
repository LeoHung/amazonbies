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

import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Base64;

import java.io.*;


class Q2IndexConvertor{

    static String thisline;
    static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
    static Date originDate ;

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public void initiateOriginDate() throws Exception
    {
        originDate = fmt.parse("2014-01-01+00:00:00");
    }

    public String convert(String text) throws Exception
    {
        String[] uidDtm = text.split("_");
        Date dt = fmt.parse(uidDtm[1]);
        Long seconds = (dt.getTime()-originDate.getTime())/1000;
        Long dt_uid = Long.parseLong(seconds.toString() + uidDtm[0]);
        byte[] binaryData = longToBytes(dt_uid);
        String encoded = Base64.encodeBase64String(binaryData);
        int ln = encoded.length();
        String a = encoded.substring(0,ln/2);
        String b = encoded.substring(ln/2,ln);
        return b+a;
    }

    public Q2IndexConvertor() throws Exception{
        this.initiateOriginDate();
    }

}


class Q4Cache{

    class LocationTime{
        public int locationId;
        public long time;
        public LocationTime(int locationId, long time){
            this.locationId = locationId; this.time = time;
        }
        public boolean equals(Object obj) {
            LocationTime lt2 = (LocationTime) obj;
            if(lt2.locationId == this.locationId && lt2.time == this.time){
                return true;
            }
            return false;
        }
    }
    class TagIDTweetids{
        public int tagId;
        public ArrayList<Long> tweetids;
        public TagIDTweetids(int tagId, ArrayList<Long> tweetids){
            this.tagId = tagId;
            this.tweetids = tweetids;
        }
    }

    // location, time -> array of tag, tweetid
    int maxLocationId=0;
    int maxTagId= 0;
    private ConcurrentMap<String, Integer> location2locationId;
    private ConcurrentMap<Integer, String> tagId2tag;
    private ConcurrentMap<String, Integer> tag2tagId;
    private ConcurrentMap<LocationTime, Vector<TagIDTweetids>> locationTime2tagIDTweetids;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public Q4Cache(){
        location2locationId = new ConcurrentHashMap<String, Integer>();
        tagId2tag = new ConcurrentHashMap<Integer, String>();
        tag2tagId = new ConcurrentHashMap<String, Integer>();
        locationTime2tagIDTweetids = new ConcurrentHashMap<LocationTime, Vector<TagIDTweetids>>();
    }


    public Integer getLocationId(String location){
        Integer locationId = location2locationId.get(location);
        if(locationId == null){
            locationId = maxLocationId;
            location2locationId.put(location, maxLocationId);
            maxLocationId++;
        }
        return locationId;
    }

    public String getTag(int tagId){
        String tag = tagId2tag.get(tagId);
        return tag;
    }

    public Integer getTagId(String tag){
        Integer tagId = tag2tagId.get(tag);
        if(tagId == null){
            tagId = maxTagId;
            tagId2tag.put(maxTagId, tag);
            tag2tagId.put(tag, maxTagId);
            maxTagId++;
        }
        return tagId;
    }

    public long getTime(String time){
        long timeDate = -1;
        try{
            timeDate = sdf.parse(time).getTime();
        }catch(Exception e){
            e.printStackTrace();
        }
        return timeDate;
    }

    public void put(String location, String time, String rankStr, String tagTweetids){
        int rank = Integer.parseInt(rankStr) - 1;
        // get location id
        Integer locationId = getLocationId(location);

        // convert time => time.sec
        long timeDate = getTime(time);

        // convert tag:id => tagid, Array(id)
        String[] tmp = tagTweetids.split(":");
        String tag = tmp[0];
        String[] tweetidStrs = tmp[1].split(",");

        Integer tagId = getTagId(tag);

        ArrayList<Long> tweetids = new ArrayList<Long>();
        for(String tweetidStr: tweetidStrs){
            long tweetid = Long.parseLong(tweetidStr);
            tweetids.add(tweetid);
        }

        TagIDTweetids tagIDTweetids = new TagIDTweetids(tagId, tweetids);
        LocationTime locationTime = new LocationTime(locationId, timeDate);

        if(!locationTime2tagIDTweetids.containsKey(locationTime)){
            locationTime2tagIDTweetids.put(locationTime, new Vector<TagIDTweetids>());
        }

        Vector<TagIDTweetids> v_TagIDTweetids = locationTime2tagIDTweetids.get(locationTime);
        if(v_TagIDTweetids.size() >= rank ){
            v_TagIDTweetids.setSize(rank+1);
        }
        v_TagIDTweetids.add(rank, tagIDTweetids);

    }

    public String get(String location, String timeStr, String mStr, String nStr){
        int locationId = getLocationId(location);
        long time = getTime(timeStr);
        int m = Integer.parseInt(mStr);
        int n = Integer.parseInt(nStr);

        Vector<TagIDTweetids> tagTweetidsVector = locationTime2tagIDTweetids.get(new LocationTime(location, timeStr));
        if(tagTweetids == null) return null;

        StringBuilder sb = new StringBuilder();

        for(int i = m-1 ; i < n-1; i++){
            if( i >= tagTweetidsVector.size()){sb.append("null");}
            TagIDTweetids tagTweetids = tagTweetidsVector.get(i);
            Strign tag = getTag(tagTweetids.tagId);
            sb.append(tagId);
            sb.append(":");
            for( int i = 0 ; i < tagTweetids.tweetids.length ; i++){
                sb.append(tweetid);
                if(i < tagTweetids.tweetids.length -1){
                    sb.append(',');
                }
            }
        }

        return sb.toString();
    }

}


class Q3Cache{
    private ConcurrentMap<Long, ArrayList<Long>> cache;


    public Q3Cache(){
        cache = new ConcurrentHashMap<Long, ArrayList<Long>>();
    }
    public Q3Cache(int initialCapacity){
        cache = new ConcurrentHashMap<Long, ArrayList<Long>>();
    }

    public int size(){
        return cache.size();
    }

    public void put(String useridStr, String retweetidsStr){
        long userid = Long.parseLong(useridStr);
        String[] tmp = retweetidsStr.split(",");
        ArrayList<Long> list = new ArrayList<Long>();
        for(String idStr : tmp){
            Long retweetid = (long)0;
            if(idStr.charAt(0) == '('){
                retweetid =  - Long.parseLong(idStr.substring(1, idStr.length()-1));
            }else{
                retweetid = Long.parseLong(idStr);
            }
            list.add(retweetid);
        }
        cache.put(userid, list);
    }
    public String get(String useridStr){
        long userid = Long.parseLong(useridStr);
        ArrayList<Long> retweetids = cache.get(userid);
        if(retweetids == null){
            return null;
        }

        StringBuilder sb = new StringBuilder();

        for(Long id : retweetids){
            if(id > 0){
                sb.append(id);
            }else{
                sb.append("(");
                sb.append(id);
                sb.append(")");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}

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

    public static HConnection getHBConnection(String hbaseIp){
        if(hbaseIp.trim().compareTo("null") ==0){return null;}

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
    final static int teamLineLength = teamLine.length();
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

    public static void main(final String[] args) throws Exception{

        //parameters
        String warmUpQ1Num = System.getenv("WARMUPQ1NUM");
        int port = 8080;
        if(System.getenv("PORT") != null){
            port = Integer.parseInt(System.getenv("PORT"));
            System.out.println("Run On: "+ port);
        }
        String hbaseIp = System.getenv("HBASEIP");
        String q3WarmUpFile = System.getenv("WARMUPQ3FILE");
        String q4WarmUpFile = System.getenv("WARMUPQ4FILE");

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
        final ConcurrentMap<String,String> q1Cache = new ConcurrentHashMap<String,String>(7944108 );
        //warmUpQ1(q1Cache, new BigInteger(warmUpQ1Num));
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

                StringBuilder sb = new StringBuilder();
                sb.append(numberStr);
                sb.append("\n");
                sb.append("Amazombies,jiajunwa,chiz2,sanchuah");
                sb.append("\n");
                sb.append(timeFormat.format(Calendar.getInstance().getTime()));

                /*String output = String.format(
                    "%s\nAmazombies,jiajunwa,chiz2,sanchuah\n%s",
                    numberStr,
                    timeFormat.format(Calendar.getInstance().getTime())
                );*/

                exchange.getResponseSender().send(sb.toString());

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
        // /q2?userid=1473664038&tweet_time=2014-04-08+02:43:27
    	System.out.println("Q2 hbse: start");
        final ConcurrentMap<String,String> q2HbaseCache = new ConcurrentHashMap<String,String>();
        final HConnection q2hbaseConnection = HBaseConnection.getHBConnection(hbaseIp);
        //warmUpQ2(q2HbaseCache, q2HbaseTable);

        final Q2IndexConvertor q2IndexConvertor = new Q2IndexConvertor();

        final byte[] q2familyByte = Bytes.toBytes("cfmain");
        final byte[] tweetIdByte = Bytes.toBytes("tweetId");
        final byte[] sentimentScoreByte = Bytes.toBytes("sentimentScore");
        final byte[] censoredTextByte = Bytes.toBytes("censoredText");

        HttpHandler q2HbaseHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {

                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String tweet_time = exchange.getQueryParameters().get("tweet_time").getFirst().replace(" ", "+");
                String row_key = userid + "_" + tweet_time;

                String cachePage = q2HbaseCache.get(row_key);
                boolean isCached = (cachePage!= null);
                String page =null;
                if(!isCached){
                    // HTableInterface q2HbaseTable = q2hbaseConnection.getTable("tweets");
                    // Get g = new Get(Bytes.toBytes(row_key));

                    HTableInterface q2HbaseTable = q2hbaseConnection.getTable("tweetsq2");
                    Get g = new Get(Bytes.toBytes(q2IndexConvertor.convert(row_key)));

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
                }else{
                    page = cachePage;
                }

                exchange.getResponseSender().send(page);

                //double endTime = System.currentTimeMillis();

                //System.out.println(String.format("whole: %f, get: %f, (%f)" , endTime-startTime, getEnd - getStart, (getEnd-getStart)/ (endTime - startTime)));


                if(!isCached){
                    q2HbaseCache.put(userid, page);
                }
            }
        };


        // Q3
        // /q3?userid=2495192362
    	System.out.println("Q3: start");
        // final ConcurrentMap<String,String> q3Cache = new ConcurrentHashMap<String,String>(13888216);
        final Q3Cache q3Cache = new Q3Cache();
        System.out.println("Q3: warmup");
        warmUpQ3(q3Cache, q3WarmUpFile);
        System.out.println("Q3: get connection");
        final HConnection q3connection = HBaseConnection.getHBConnection(hbaseIp);
        //final HConnection q3connection = null;
        HttpHandler q3Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();
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
                sb.append(";");

                exchange.getResponseSender().send(sb.toString());

                if(!isCached){
                    q3Cache.put(userid, retweetids);
                    //q3Table.close();
                }
            }
        };

        // Q4
        // /q4?date=2014-05-22&location=Aalborg&m=1&n=3
        final ConcurrentMap<String,String> q4Cache = new ConcurrentHashMap<String,String>();
        final HConnection q4connection = HBaseConnection.getHBConnection(hbaseIp);
        final ConcurrentMap<String, Vector<String>> warmUpQ4cache = new ConcurrentHashMap<String, Vector<String>>();
        System.out.println("Q4 warmup: ");
        warmUpQ4(warmUpQ4cache, q4WarmUpFile);
        HttpHandler q4Handler = new HttpHandler(){
             public void handleRequest(final HttpServerExchange exchange)
                     throws Exception {
                 String date = exchange.getQueryParameters().get("date").getFirst();
                 String location = exchange.getQueryParameters().get("location").getFirst();
                 String mStr = exchange.getQueryParameters().get("m").getFirst();
                 int m = Integer.parseInt(mStr);
                 String nStr = exchange.getQueryParameters().get("n").getFirst();
                 int n = Integer.parseInt(nStr);

                 StringBuilder sb = new StringBuilder();
                 sb.append(teamLine);
                 sb.append("\n");

                 List<String> hashtagRetweets = warmUpQ4cache.get(location+"_"+date);
                 boolean isCached = (hashtagRetweets != null);
                 if(!isCached){
                    HTableInterface q4Table = q4connection.getTable("tweetsq4");
                    List<Get> batchGets = new ArrayList<Get>();
                    for(int i = m ; i <= n ; i++){
                        String rowKey = String.format("%s_%s_%d", location, date, i);
                        Get g = new Get(Bytes.toBytes(rowKey));
                        batchGets.add(g);
                    }

                    Result[] rArray = q4Table.get(batchGets);

                    for(Result r: rArray){
                        byte [] tagtweetid = r.getValue(
                                    Bytes.toBytes("cfmain"),
                                    Bytes.toBytes("tagtweetid")
                        );
                        sb.append(Bytes.toString(tagtweetid));
                        sb.append("\n");
                    }
                 }else{
                    for( int i = (m -1) ; i<= (n -1) ; i++){
                        if(i < hashtagRetweets.size()){
                            sb.append(hashtagRetweets.get(i));
                            sb.append("\n");
                        }else{
                            sb.append("null");
                            sb.append("\n");
                        }
                    }
                }

                 exchange.getResponseSender().send(sb.toString());
             }
        };

        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);
        pathhandler.addPrefixPath("/q1Save", q1SaveHandler);
        pathhandler.addPrefixPath("/q2", q2HbaseHandler);
        pathhandler.addPrefixPath("/sql/q2", q2SQLHandler);
        pathhandler.addPrefixPath("/q3", q3Handler);
        pathhandler.addPrefixPath("/q4", q4Handler);

        pathhandler.addPrefixPath("/", helloworld);


        Undertow server = Undertow.builder().addHttpListener(port, "0.0.0.0")
                .setHandler(pathhandler).build();
        server.start();
    }
}
