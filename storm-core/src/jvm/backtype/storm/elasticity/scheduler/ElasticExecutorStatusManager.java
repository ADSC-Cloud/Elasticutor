package backtype.storm.elasticity.scheduler;

import scala.Int;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 16-8-16.
 */
public class ElasticExecutorStatusManager {
    final private Map<Integer, ElasticExecutorInfo> taskIdToInfo = new HashMap<>();

    /**
     * get a snapshot of the current executor information. The snapshot is a copy of current state of taskIdToInfo.
     * @return snapshot
     */
    public Map<Integer, ElasticExecutorInfo> getInfoSnapshot() {
        Map<Integer, ElasticExecutorInfo> snapshot = new HashMap<>();
        synchronized (taskIdToInfo) {
            for(Integer taskid: taskIdToInfo.keySet()) {
                snapshot.put(taskid, taskIdToInfo.get(taskid).duplicate());
            }
        }
        return snapshot;
    }

    public String getAllocatedCoreForARoute(int taskid, int route) {
        synchronized (taskIdToInfo) {
            return taskIdToInfo.get(taskid).getAllocatedCores().get(route);
        }
    }

    public List<String> getAllocatedCoresForATask(int taskid) {
        synchronized (taskIdToInfo) {
            return taskIdToInfo.get(taskid).getAllocatedCores();
        }
    }

    void registerNewElasticExecutor(int taskid, String hostip) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.put(taskid, new ElasticExecutorInfo(taskid, hostip));
        }
    }

    void unregisterElasticExecutor(int taskid) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.remove(taskid);
        }
    }

    public void updateStateSize(int taskid, long stateSize) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.get(taskid).updateStateSize(stateSize);
        }
    }

    public void updateDataIntensivenessFactor(int taskid, long dataRate) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.get(taskid).updateDataIntensivenessFactor(dataRate);
        }
    }

    public void updateDesirableParallelism(int taskid, int desirableParallelism) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.get(taskid).updateDesirableParallelism(desirableParallelism);
        }
    }

    public Map<Integer, ElasticExecutorInfo> getTaskIdToInfo() {
        return taskIdToInfo;
    }

    public String scalingInExecutor(int taskid) {
        synchronized (taskIdToInfo) {
            return taskIdToInfo.get(taskid).scalingIn();
        }
    }

    public void scalingOutExecutor(int taskid, String hostIP) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.get(taskid).scalingOut(hostIP);
        }
    }

    public void migrateRoute(int taskid, int route, String targetIP) {
        synchronized (taskIdToInfo) {
            taskIdToInfo.get(taskid).taskMigrate(route, targetIP);
        }
    }


}
