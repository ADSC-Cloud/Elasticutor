package storm.starter.util;

import backtype.storm.utils.RateTracker;
import backtype.storm.utils.Utils;

import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class ComputationSimulator {
    public static long compute(long timeInNanoSecond) {
        final long start = System.nanoTime();
        long seed = start;
        while(System.nanoTime() - start < timeInNanoSecond) {
            seed = (long) Math.sqrt(new Random().nextInt());
        }
        return seed;
    }

    public static void sleep(long timeInNanoSecond, long randomSeed) {
        if (timeInNanoSecond <= 1000000) {
            if (randomSeed % (1000000 / timeInNanoSecond) == 0) {
                Utils.sleep(1);
            }
        } else {
            Utils.sleep(timeInNanoSecond / 1000000);
        }
    }

    static public void main(String[] args) {
        final RateTracker rateTracker = new RateTracker(1000,10);
        new Thread(new Runnable() {
            @Override
            public void run() {
                long count = 0;
                while(true) {
                    ComputationSimulator.sleep(100000, count++);
                    rateTracker.notify(1);
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("throughput: " + rateTracker.reportRate());
                }
            }
        }).start();
    }
}
