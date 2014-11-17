

package com.mastertheboss.undertow.cache.binarySearch;

import java.util.Comparator;

public class LongComparator implements Comparator<Long>{
    public int compare(Long o1, Long o2){
        if(o1.equals(o2) ){
            return 0;
        }
        return o1 > o2 ? 1 : -1;
    }
    public boolean equals(Object obj){
        return false;
    }

}
