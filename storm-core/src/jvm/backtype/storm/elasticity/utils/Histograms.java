package backtype.storm.elasticity.utils;


import java.io.Serializable;
import java.util.*;

/**
 * Created by robert on 12/18/15.
 * This class is used to represent a distribution on the key space
 */
public class Histograms implements Serializable {

    public Map<Integer, Long> histograms;

    public Histograms() {
        histograms = new HashMap<>();
    }

    public Histograms(Map<Integer, Long> histograms) {
        this.histograms = new HashMap<>();
        this.histograms.putAll(histograms);
//        this.histograms = histograms;
    }

    public String toString() {
        String ret = "Distribution: \n";
//        for(Object o: histograms.keySet()) {
//            ret += o + ": " + histograms.get(o) + " ";
//        }

        Object[] keys = histograms.keySet().toArray();
        Arrays.sort(keys);
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE, different;
        long sum = 0;
        for(Object key: keys) {
            max = Math.max(histograms.get(key), max);
            min = Math.min(histograms.get(key), min);
            long count = histograms.get(key);
            sum += count;

            if(count > 0)
            ret += key + ": " + histograms.get(key) + " ";
        }
        ret += "\n";
        ret += "Min: " + min + "  Max:" + max + "  Avg: " + sum/keys.length + "\n";
        return ret;
    }

    public void setDefaultValueForAbsentKey(int numberOfKeys) {
        for(int i=0; i< numberOfKeys; i++ ) {
            if(!histograms.containsKey(i)) {
                histograms.put(i, 0L );
            }
        }
    }

    public ArrayList<Long> histogramsToArrayList () {
        ArrayList<Long> ret = new ArrayList<Long>();
        Object[] keys = histograms.keySet().toArray();
        Arrays.sort(keys);
        for(Object key: keys) {
            ret.add(histograms.get(key));
        }

        return ret;
    }

    public void merge(Histograms his) {
        for(int key: his.histograms.keySet()) {
            if(histograms.containsKey(key)) {
                histograms.put(key, histograms.get(key) + his.histograms.get(key));
            } else {
                histograms.put(key, his.histograms.get(key));
            }
        }
    }

}
