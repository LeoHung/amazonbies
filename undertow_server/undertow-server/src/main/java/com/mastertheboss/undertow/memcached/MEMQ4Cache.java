package com.mastertheboss.undertow.memcached;
import java.io.*;
import java.util.*;
import net.rubyeye.xmemcached.*;


public class MEMQ4Cache{
    //String, Vector<String>
    MemcachedClient client;
    public int expireTime = 7200;

    public boolean containsKey(String key){
        if(this.get(key) == null){
            return false;
        }
        return true;
    }

    public void warmUp(String filename){
        int size = 0;
        try{
            BufferedReader bf = new BufferedReader(new FileReader(filename));
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

                if(!this.containsKey(locationDate)){
                    this.put(locationDate, new Vector<String>());
                }
                Vector<String> l = this.get(locationDate);
                if( (rank-1) >= l.size()){
                    l.setSize(rank-1+1);
                }
                l.insertElementAt(retweet, (rank-1));

                // put back
                this.put(locationDate, l);

                size += 1;
            }
        }catch(Exception e ){
            e.printStackTrace();
        }
        System.out.println("Q4 total: " + size);
    }

    public MEMQ4Cache(MemcachedClient client){ this.client = client; }

    // public void put(String locationStr, String dateStr, String rankStr, String retweet){
    //     String[] tmp = line.split("\t");
    //     String locationDateRank = tmp[0];
    //     String retweet = tmp[1];
    //     String[] tmp2 = locationDateRank.split("_");
    //     String location ="";
    //     for( int i =0 ;i < tmp2.length -2 ; i++){
    //         if(i >0){
    //             location += "_";
    //         }
    //         location += tmp2[i];
    //     }
    //     String date = tmp2[tmp2.length-2];
    //     int rank = Integer.parseInt(tmp2[tmp2.length-1]);

    //     String locationDate = location +"_"+ date;
    //     if(!q4Cache.containsKey(locationDate)){
    //         q4Cache.put(locationDate, new Vector<String>());
    //     }
    //     Vector<String> l = q4Cache.get(locationDate);
    //     if( (rank-1) >= l.size()){
    //         l.setSize(rank-1+1);
    //     }
    //     l.insertElementAt(retweet, (rank-1));
    // }

    public void put(String key, Object obj){
        try{
            client.set(key, expireTime, obj);
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    public Vector<String> get(String locationDate){
        Vector<String> vectorString = null;
        try{
            vectorString = (Vector<String>) client.get(locationDate);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return vectorString;
    }

}
