package com.mastertheboss.undertow.memcached;
import java.io.*;
import java.util.*;

import net.rubyeye.xmemcached.auth.AuthInfo;
import net.rubyeye.xmemcached.buffer.BufferAllocator;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.ReconnectRequest;
import net.rubyeye.xmemcached.networking.Connector;
import net.rubyeye.xmemcached.transcoders.Transcoder;
import net.rubyeye.xmemcached.utils.Protocol;
import net.rubyeye.xmemcached.*;
import java.util.concurrent.TimeoutException;
import java.lang.InterruptedException;
import net.rubyeye.xmemcached.exception.MemcachedException;
public class MEMQ3Cache{
    //String, ArrayList<Long>

    public int expireTime = 7200;

    MemcachedClient client;
    public MEMQ3Cache(MemcachedClient client){ this.client = client; }

    public void warmUp(String filename){
        int size =0;
        try{
            BufferedReader bf = new BufferedReader(new FileReader(filename));
            String line = null;
            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String userid = tmp[0];
                String retweetids = tmp[1];
                this.put(userid, retweetids);
                size += 1;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Q3 total: "+ size);
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

        try{
            client.set(useridStr, expireTime, list);
        }catch(Exception e){
            System.out.println("Cannot set data");
            e.printStackTrace();
        }
    }

    public String get(String useridStr){
        long userid = Long.parseLong(useridStr);
        ArrayList<Long> retweetids = null;
        try{
            retweetids = (ArrayList<Long>) client.get(useridStr);
        }catch(Exception e ){
            e.printStackTrace();
            return null;
        }

        if(retweetids == null){
            return null;
        }
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < retweetids.size(); i++){
            Long id = retweetids.get(i);
            if(id > 0){
                sb.append(id);
            }else{
                sb.append("(");
                sb.append(-id);
                sb.append(")");
            }
            if( i < retweetids.size() -1){
                sb.append("\n");
            }
        }
        return sb.toString();
    }

}
