package backtype.storm.elasticity.scheduler;

import java.util.*;

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

    public ElasticExecutorInfo(int taskId, String hostIp) {
        this(taskId, hostIp, 0, 0.0);
    }

    public ElasticExecutorInfo(int taskId, String hostIp, long stateSize, double dataIntensivenessFactor) {
        this.taskId = taskId;
        this.hostIp = hostIp;
        this.stateSize = stateSize;
        this.dataIntensivenessFactor = dataIntensivenessFactor;
        allocatedCores = new ArrayList<>();
        allocatedCores.add(hostIp);

    }

    public ElasticExecutorInfo duplicate() {
        ElasticExecutorInfo ret = new ElasticExecutorInfo(this.taskId, this.hostIp, this.stateSize, this.dataIntensivenessFactor);
        ret.allocatedCores.clear();
        ret.allocatedCores.addAll(this.allocatedCores);
        ret.desirableParallelism = this.desirableParallelism;
        return ret;
    }

    public void updateStateSize(long stateSize) {
        this.stateSize = stateSize;
    }

    public void updateDataIntensivenessFactor(long dataRate) {
        dataIntensivenessFactor = dataRate / (double) allocatedCores.size();
    }

    public void scalingOut(String hostIp) {
        allocatedCores.add(hostIp);
//        System.out.println("scaling out is called.");
//        System.out.println("allocated cores:" + allocatedCores);
    }

    public String scalingIn() {
        final String ip = allocatedCores.get(allocatedCores.size() - 1);
        allocatedCores.remove(allocatedCores.size() - 1);
//        System.out.println("scaling in is called.");
//        System.out.println("allocated cores:" + allocatedCores);
        return ip;
    }

    public void taskMigrate(int taskId, String targetIp) {
        allocatedCores.set(taskId, targetIp);
//        System.out.println("task migration is called.");
//        System.out.println("allocated cores:" + allocatedCores);
    }

    public void updateDesirableParallelism(int desirableParallelism) {
        this.desirableParallelism = desirableParallelism;
//        System.out.println(String.format("Desirable DOP of Task %d is %d.", taskId, desirableParallelism));
    }

    public int getCurrentParallelism() {
        return allocatedCores.size();
    }

    public int getDesirableParallelism() {
        return desirableParallelism;
    }

    public double getDataIntensiveness() {
        return dataIntensivenessFactor;
    }

    public List<String> getAllocatedCores() {
        return allocatedCores;
    }

    public String getHostIp() {
        return hostIp;
    }

    public long getStateSize() {
        return stateSize;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getRouteIdForACore(String core) {
        for(int i = allocatedCores.size() - 1; i >= 0; i--) {
            if(allocatedCores.get(i).equals(core)) {
                return i;
            }
        }
        return -1;
    }

    public String toString() {
        String ret = "";
        ret += String.format("{%d, %d, %.2f, %d -> %d}", taskId, stateSize, dataIntensivenessFactor, getCurrentParallelism(), desirableParallelism);
//        ret += String.format("ID = %d, State size = %d, Data-intensiveness = %.2f", taskId, stateSize, dataIntensivenessFactor);

        return ret;
    }

    static public Comparator<ElasticExecutorInfo> createDataIntensivenessReverseComparator() {
        return new Comparator<ElasticExecutorInfo>() {
            @Override
            public int compare(ElasticExecutorInfo o1, ElasticExecutorInfo o2) {
                return Double.compare(o2.dataIntensivenessFactor, o1.dataIntensivenessFactor);
            }
        };
    }

    @Override
    public int hashCode() {
        return new Integer(taskId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ElasticExecutorInfo && ((ElasticExecutorInfo) obj).taskId == taskId;
    }
    public static void main(String[] args) {
        Set<ElasticExecutorInfo> infos = new HashSet<>();
        infos.add(new ElasticExecutorInfo(10, "ip1", 10, 4.2));
        infos.add(new ElasticExecutorInfo(12, "ip3", 10, 3.2));
        infos.add(new ElasticExecutorInfo(10, "ip4", 10, 2.1));
        System.out.println(infos);
        List<ElasticExecutorInfo> list = new ArrayList<>(infos);
        Collections.sort(list, createDataIntensivenessReverseComparator());
        System.out.println(list);
    }
}
