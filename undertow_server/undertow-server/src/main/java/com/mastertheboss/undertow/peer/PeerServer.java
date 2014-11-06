package com.mastertheboss.undertow.peer;


import java.net.HttpURLConnection;
import com.github.kevinsawicki.http.HttpRequest;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.*;
import org.apache.http.params.*;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.*;
import org.apache.http.impl.client.*;
import org.apache.http.client.*;
//import org.apache.commons.httpclient.params.* ;
import org.apache.http.*;
import org.apache.http.client.methods.HttpGet;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import org.apache.commons.io.IOUtils;

import java.io.*;
public class PeerServer{
    String url ;
    // url -> http://....ec2.com:80
    ThreadSafeClientConnManager connManager;
    public PeerServer(String url){
        this.url = url;
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http",PlainSocketFactory.getSocketFactory(),80));
        HttpParams params = new BasicHttpParams(); 
        connManager = new ThreadSafeClientConnManager(params,schemeRegistry);
        

    }

    // uri -> q3?= ....
    public String get(String uri){
        HttpParams params = new BasicHttpParams();
        HttpClient httpclient = new DefaultHttpClient(this.connManager, params);

        HttpGet httpget = new HttpGet(url + "/" + uri);
        try {
            HttpResponse response = httpclient.execute(httpget);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                InputStream instream = entity.getContent();
                try {
                    // do something useful
                    return IOUtils.toString(instream, "utf8");
                } finally {
                    instream.close();
                }
            }
        } catch(Exception e ){
            e.printStackTrace();
        }

        return null;
    }

    public String getQ3(String userId){
        return this.get("q3?userid="+userId);
    }

    public String getQ4(String location, String date, String mStr, String nStr){
        try{
            return this.get(
                String.format(
                    "q4?location=%s&date=%s&m=%s&n=%s",
                    URLEncoder.encode(location,"utf8"), date, mStr, nStr)
            );
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

}
