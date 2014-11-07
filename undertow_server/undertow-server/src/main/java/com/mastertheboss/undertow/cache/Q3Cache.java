
package com.mastertheboss.undertow.cache;

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

public class Q3Cache{
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
