package backtype.storm.elasticity.scheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Robert on 8/12/16.
 */
public class ElasticExecutorInfo {
    int taskId;
    String hostIp;
    long stateSize;
    double dataIntensivenessFactor;
    List<String> allocatedCores;

    public ElasticExecutorInfo(int taskId, String hostIp) {
        this.taskId = taskId;
        this.hostIp = hostIp;
        stateSize = 0;
        dataIntensivenessFactor = 0;
        allocatedCores =  new ArrayList<>();
        allocatedCores.add(hostIp);
    }

    public void updateStateSize(long stateSize) {
        this.stateSize = stateSize;
    }

    public void updateDataIntensivenessFactor(long dataRate) {
        dataIntensivenessFactor = dataRate / (double) allocatedCores.size();
    }
}
