package backtype.storm.elasticity.config;

import backtype.storm.utils.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by robert on 12/22/15.
 */
public class Config {

    public static void overrideFromStormConfigFile() {
        Map conf = Utils.readStormConfig();
        overrideFromStormConf(conf);
    }

    public static void overrideFromStormConf(Map conf) {

        try {
            InetAddress address = InetAddress.getByName(readString(conf, "nimbus.host", "NimbusHostNameNotGiven"));
            masterIp = address.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            InetAddress address = InetAddress.getByName(readString(conf, "elasticity.slave.ip", "SlaveIpNotGiven"));
            slaveIp = address.getHostAddress();
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

        EnableStateMigratedMetrics = readBoolean(conf, "elasticity.master.EnableStateMigratedMetrics", false);

        EnableIntraExecutorDataTransferMetrics = readBoolean(conf, "elasticity.master.EnableIntraExecutorDataTransferMetrics", false);
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


    /* The following are the default values */

    public static int NumberOfShard = 256;

    public static double RoutingSamplingRate = 1.0;

    public static int SubtaskInputQueueCapacity = 64;

    public static int ResultQueueCapacity = 256;

    public static int RemoteExecutorInputQueueCapacity = 64;

    public static int ElasticTaskHolderOutputQueueCapacity = 1024;

    public static int StateCheckPointingCycleInSecs = 10;

    public static int CreateBalancedHashRoutingSamplingTimeInSecs = 3;

    public static int TaskThroughputMonitorDurationInMilliseconds = 5000;

    public static int ExecutorKeyDistributionMonitorDurationgInMilliseconds = 5000;

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

    public static double latencySampleRate = 0.1;

    public static int numberOfLatencyHistoryRecords = 100;

    public static double tupleLengthSampleRate = 0.001;

    public static int numberOfTupleLengthHistoryRecords = 100;

    public static int latencyMaximalTimeIntervalInSecond = 1;

    public static double taskLevelLoadBalancingThreshold = 0.2;

    public static int CPUBudget = 0;

    public static String masterIp = "172.31.17.217";

    public static String slaveIp;

    public static boolean DisableDataIntensivenessInfo = false;

    public static boolean DisableStateSizeInfo = false;

    public static boolean EnableStateMigratedMetrics = false;

    public static boolean EnableIntraExecutorDataTransferMetrics = false;

}
