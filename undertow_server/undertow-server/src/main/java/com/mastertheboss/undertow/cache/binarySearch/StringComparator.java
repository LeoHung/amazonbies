
package com.mastertheboss.undertow.cache.binarySearch;

import java.util.Comparator;

public class StringComparator implements Comparator<String>{
    public int compare(String o1, String o2){
        if(o1.equals(o2)) {return 0;}
        return o1.hashCode() - o2.hashCode();
    }
    public boolean equals(Object obj){
        return false;
    }

}
