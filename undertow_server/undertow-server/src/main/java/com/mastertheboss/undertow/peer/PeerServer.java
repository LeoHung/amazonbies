package com.mastertheboss.undertow.peer;


import java.net.HttpURLConnection;
import com.github.kevinsawicki.http.HttpRequest;
public class PeerServer{
    String url ;
    // url -> http://....ec2.com:80
    public PeerServer(String url){this.url = url;}

    // uri -> /q3?= ....
    public String get(String uri){
        try{
            return HttpRequest.get(url + "/" + uri).body();
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public String getQ3(String userId){
        return this.get("q3?userid="+userId);
    }

    public String getQ4(String location, String date, String mStr, String nStr){
        return this.get(
            String.format(
                "q4?location=%s&date=%s&m=%s&n=%s",
                location, date, mStr, nStr)
        );
    }

}
