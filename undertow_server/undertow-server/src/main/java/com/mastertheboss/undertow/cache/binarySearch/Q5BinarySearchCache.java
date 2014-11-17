package com.mastertheboss.undertow.cache.binarySearch;

import com.mastertheboss.undertow.cache.BinarySearchCache;
import com.mastertheboss.undertow.cache.*;

import java.io.*;
import java.util.*;


public class Q5BinarySearchCache implements MyCache<Long, Scores>{
    private BinarySearchCache<Long, Scores> cache ;
    public Q5BinarySearchCache(String filename){
        List<Long> userIds = new ArrayList<Long>();
        List<Scores> scores = new ArrayList<Scores>();

        try{
            BufferedReader bf = new BufferedReader(new FileReader(filename));
            String line = null;
            while( (line = bf.readLine()) != null){
                try{
                    String[] tmp = line.trim().split(",");
                    Long userId = Long.parseLong(tmp[0]);
                    short s1 = Short.parseShort(tmp[1]);
                    short s2 = Short.parseShort(tmp[2]);
                    short s3 = Short.parseShort(tmp[3]);
                    userIds.add(userId);
                    scores.add(new Scores(s1, s2, s3));
                }catch(Exception e){
                    continue;
                }
            }
            bf.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        Long[] userIdArray = (Long[]) userIds.toArray(new Long[userIds.size()]);
        Scores[] scoreArray = (Scores[]) scores.toArray(new Scores[scores.size()]);

        cache = new BinarySearchCache<Long, Scores>(userIdArray, scoreArray,  new LongComparator());

    }

    public Scores get(Long key){
        return this.cache.get(key);
    }

    public void put(Long key, Scores value){
        this.cache.put(key, value);
    }

}
