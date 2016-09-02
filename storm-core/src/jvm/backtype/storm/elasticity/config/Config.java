package backtype.storm.elasticity.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by robert on 12/22/15.
 */
public class Config {


    public static void overrideFromStormConf(Map conf) {

        try {
            InetAddress address = InetAddress.getByName(readString(conf, "nimbus.host", "NimbusHostNameNotGiven"));
            masterIp = address.getHostAddress();
            System.out.println("Master ip: " + masterIp);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            InetAddress address = InetAddress.getByName(readString(conf, "elasticity.slave.ip", "SlaveIpNotGiven"));
            slaveIp = address.getHostAddress();
            System.out.println("Slave ip: " + slaveIp);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }



        EnableSubtaskLevelLoadBalancing = readBoolean(conf, "elasticity.EnableIntraExecutorLoadBalancing", false);

        EnableAutomaticScaling = readBoolean(conf, "elasticity.EnableAutomaticScaling", false);

        SubtaskLevelLoadBalancingCycleInMilliSecs = readInteger(conf, "elasticity.IntraExecutorLoadBalancingCycle", 1000);

        ElasticSchedulingCycleInMillisecond = readInteger(conf, "elasticity.ElasticSchedulingCycleInMillisecond", 2000);

        CPUBudget = readInteger(conf, "elasticity.cpu.budget", 0);

        DisableDataIntensivenessInfo = readBoolean(conf, "elasticity.scheduler.DisableDataIntensivenessInfo", false);

        DisableStateSizeInfo = readBoolean(conf, "elasticity.scheduler.DisableStateSizeInfo", false);
    }

    static int readInteger(Map conf, String key, int defaultValue) {
        if(conf.containsKey(key))
            return (int)conf.get(key);
        else
            return defaultValue;
    }

    static String readString(Map conf, String key, String defaultValue) {
        if(conf.containsKey(key))
            return (String) conf.get(key);
        else
            return defaultValue;
    }

    static boolean readBoolean(Map conf, String key, boolean defaultValue) {
        if(conf.containsKey(key))
            return ((int) conf.get(key)) >= 1;
        else
            return defaultValue;
    }


    /* The following are the default value */

    public static int NumberOfShard = 512;

    public static double RoutingSamplingRate = 1.0;

    public static int SubtaskInputQueueCapacity = 256;

    public static int ResultQueueCapacity = 1024;

    public static int RemoteExecutorInputQueueCapacity = 256;

    public static int ElasticTaskHolderOutputQueueCapacity = 1024;

    public static int StateCheckPointingCycleInSecs = 10;

    public static int CreateBalancedHashRoutingSamplingTimeInSecs = 3;

    public static int ProcessCPULoadReportCycleInSecs = 1;

    public static int WorkloadHighWaterMark = 6;

    public static int WorkloadLowWaterMark = 3;

    public static int WorkerLevelLoadBalancingCycleInSecs = 10;

    public static int SubtaskLevelLoadBalancingCycleInMilliSecs = 500;

    public static boolean EnableWorkerLevelLoadBalancing = false;

    public static boolean EnableSubtaskLevelLoadBalancing = false;

    public static boolean EnableAutomaticScaling = false;

    public static int ElasticSchedulingCycleInMillisecond = 5000;

    public static int LoggingServerPort = 10000;

    public static double latencySampleRate = 0.02;

    public static int numberOfLatencyHistoryRecords = 10;

    public static double tupleLengthSampleRate = 0.001;

    public static int numberOfTupleLengthHistoryRecords = 100;

    public static int latencyMaximalTimeIntervalInSecond = 1;

    public static double taskLevelLoadBalancingThreshold = 0.2;

    public static int CPUBudget = 0;

    public static String masterIp = "192.168.0.183";

    public static String slaveIp = "192.168.0.183";

    public static boolean DisableDataIntensivenessInfo = false;

    public static boolean DisableStateSizeInfo = false;

}
