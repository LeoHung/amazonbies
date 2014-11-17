package com.mastertheboss.undertow.cache.binarySearch;

import com.mastertheboss.undertow.cache.BinarySearchCache;
import com.mastertheboss.undertow.cache.*;

import java.io.*;
import java.util.*;


public class Q5BinarySearchCache implements MyCache<Integer, Scores>{
    private BinarySearchCache<Integer, Scores> cache ;
    public Q5BinarySearchCache(String filename){
        List<Integer> userIds = new ArrayList<Integer>();
        List<Scores> scores = new ArrayList<Scores>();


        try{
            BufferedReader bf = new BufferedReader(new FileReader(filename));
            String line = null;
            while( (line = bf.readLine()) != null){
                try{
                    String[] tmp = line.trim().split(",");
                    Integer userId = Integer.parseInt(tmp[0]);
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

        Integer[] userIdArray = (Integer[]) userIds.toArray(new Integer[userIds.size()]);
        Scores[] scoreArray = (Scores[]) scores.toArray(new Scores[scores.size()]);

        cache = new BinarySearchCache<Integer, Scores>(userIdArray, scoreArray,  new IntegerComparator());

    }

    public Scores get(Integer key){
        return this.cache.get(key);
    }

    public void put(Integer key, Scores value){
        this.cache.put(key, value);
    }

}
