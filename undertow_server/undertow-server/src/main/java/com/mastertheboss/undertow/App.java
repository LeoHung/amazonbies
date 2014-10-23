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

import java.util.HashMap;

class LimitedHashMap<K, V> extends HashMap<K, V> {
    int maxSize;

    public LimitedHashMap(int maxSize){
        this.maxSize = maxSize;
    }

    public V put(K key, V value) {
        if (this.size() >= this.maxSize && !this.containsKey(key)) {
            return null;
        } else {
            super.put(key, value);
            return value;
        }
    }
}


class SQLConnection{
    static Connection mysqlConn=null;

    public static Connection getSQLConnection(){

        if(SQLConnection.mysqlConn == null){
            String mysql_url="54.172.214.61";
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

/**
 * Hello world!
 *
 */
public class App {

    public static void main(final String[] args) {

        HttpHandler helloworld = new HttpHandler() {
                    public void handleRequest(final HttpServerExchange exchange)
                            throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                                "text/plain");
                        exchange.getResponseSender().send("Mom. I'm alive.");
                        }
                    };

        final BigInteger publicKey= new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");
        final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        HttpHandler q1Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                String key_str = exchange.getQueryParameters().get("key").getFirst();
                BigInteger key = new BigInteger(key_str);
                BigInteger number = key.divide(publicKey);
                String timeStr = timeFormat.format(Calendar.getInstance().getTime());

                String output = String.format(
                    "%s\nAmazombies,jiajunwa,chiz2,sanchuah\n%s",
                    number.toString(),
                    timeStr
                );
                exchange.getResponseSender().send(
                    output
                );
            }
        };

        final LimitedHashMap<String,String> cache = new LimitedHashMap<String,String>(1000* 1000);
        final Connection sqlConn = SQLConnection.getSQLConnection();
        HttpHandler q2SQLHandler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                String userid = exchange.getQueryParameters().get("userid").getFirst();
                String tweet_time = exchange.getQueryParameters().get("tweet_time").getFirst().replace(" ", "+");
                String row_key = userid+"_"+tweet_time;

                String cachePage = cache.get(row_key);
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
                        cache.put(row_key, page);
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

        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);
        pathhandler.addPrefixPath("/sql/q2", q2SQLHandler);
        pathhandler.addPrefixPath("/", helloworld);

        Undertow server = Undertow.builder().addHttpListener(8888, "localhost")
                .setHandler(pathhandler).build();
        server.start();
    }
}
