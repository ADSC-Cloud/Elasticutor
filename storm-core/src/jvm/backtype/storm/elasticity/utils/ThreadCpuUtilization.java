package backtype.storm.elasticity.utils;

import backtype.storm.utils.Utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Created by robert on 8/12/16.
 */
public class ThreadCpuUtilization {
    static class Context {
        long threadId;
        long lastCpuTimeInNanoSeconds;
        long lastIssuedTimeInMillis;
        Context(long threadId) {
            this.threadId = threadId;
            lastCpuTimeInNanoSeconds = 0;
            lastIssuedTimeInMillis = System.currentTimeMillis();
        }
    }
    static double getCpuUtilizationRate(Context context) {
        ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();

        long cpuTime = tmxb.getThreadUserTime(context.threadId);
        long duration = System.currentTimeMillis() - context.lastIssuedTimeInMillis;
        double utilization;
        if(duration > 0)
            utilization = (cpuTime - context.lastCpuTimeInNanoSeconds) / (double)(duration * 1000000);
        else
            utilization = 0;
        context.lastCpuTimeInNanoSeconds = cpuTime;
        context.lastIssuedTimeInMillis = System.currentTimeMillis();
        return utilization;
    }

    public static void main(String[] args) {
        Context context = new Context(Thread.currentThread().getId());
        getCpuUtilizationRate(context);
        Utils.sleep(1000);
        System.out.println("CPU rate: " + getCpuUtilizationRate(context));

        getCpuUtilizationRate(context);
        compute(1000000000L);
        System.out.println("CPU rate: " + getCpuUtilizationRate(context));

    }

    public static long compute(long timeInNanosecond) {
        final long start = System.nanoTime();
        long seed = start;
        while(System.nanoTime() - start < timeInNanosecond) {
            seed = (long) Math.sqrt(3.15);
        }
        return seed;

//            try {
//                TimeUnit.NANOSECONDS.sleep(timeInNanosecond);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//                return 0;
    }
}
