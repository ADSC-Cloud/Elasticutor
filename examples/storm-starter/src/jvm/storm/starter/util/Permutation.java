package storm.starter.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by robert on 18/1/17.
 */
public class Permutation {

    private int keys;
    List<Integer> list;


    public Permutation(int keys) {
        this.keys = keys;
        shuffle();
    }

    public Permutation(int keys, long seed) {
        this.keys = keys;
        shuffle(seed);
    }

    public synchronized void shuffle() {
        shuffle(System.currentTimeMillis());
    }

    public synchronized void shuffle(long seed) {
        list = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            list.add(i);
        }
        Collections.shuffle(list, new Random(seed));
    }

    public synchronized Integer get(int index) {
        return list.get(index);
    }

    public String toString() {
        String ret = "";
        for (Integer key: list) {
            ret += key + " ";
        }
        return ret;
    }

    public static void main(String[] args) {
        Permutation permutation = new Permutation(10);
        permutation.shuffle();
        System.out.println(permutation.toString());
        permutation.shuffle(1);
        System.out.println(permutation.toString());
        permutation.shuffle(1);
        System.out.println(permutation.toString());
        permutation.shuffle();
        System.out.println(permutation.toString());
    }
}
