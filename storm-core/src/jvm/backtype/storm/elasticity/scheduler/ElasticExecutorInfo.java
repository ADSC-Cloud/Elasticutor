package backtype.storm.elasticity.scheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * The basic information of an elastic executor.
 */
public class ElasticExecutorInfo {
    int taskId;
    String hostIp;
    long stateSize;
    double dataIntensivenessFactor;
    List<String> allocatedCores;
    int desirableParallelism;

    ElasticExecutorInfo(int taskId, String hostIp) {
        this.taskId = taskId;
        this.hostIp = hostIp;
        stateSize = 0;
        dataIntensivenessFactor = 0;
        allocatedCores = new ArrayList<>();
        allocatedCores.add(hostIp);
    }

    public void updateStateSize(long stateSize) {
        this.stateSize = stateSize;
    }

    public void updateDataIntensivenessFactor(long dataRate) {
        dataIntensivenessFactor = dataRate / (double) allocatedCores.size();
    }

    public void scalingOut(String hostIp) {
        allocatedCores.add(hostIp);
        System.out.println("scaling out is called.");
        System.out.println("allocated cores:" + allocatedCores);
    }

    public String scalingIn() {
        final String ip = allocatedCores.get(allocatedCores.size() - 1);
        allocatedCores.remove(allocatedCores.size() - 1);
        System.out.println("allocated cores:" + allocatedCores);
        System.out.println("scaling in is called.");
        return ip;
    }

    public void taskMigrate(int taskId, String targetIp) {
        allocatedCores.set(taskId, targetIp);
        System.out.println("task migration is called.");
        System.out.println("allocated cores:" + allocatedCores);
    }

    public void updateDesirableParallelism(int desirableParallelism) {
        this.desirableParallelism = desirableParallelism;
        System.out.println(String.format("Desirable DOP of Task %d is %d.", taskId, desirableParallelism));
    }
}
