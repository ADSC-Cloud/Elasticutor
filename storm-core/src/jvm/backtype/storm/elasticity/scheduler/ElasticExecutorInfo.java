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

    public int getCurrentParallelism() {
        return allocatedCores.size();
    }

    public int getDesirableParallelism() {
        return desirableParallelism;
    }

    public double getDataIntensiveness() {
        return dataIntensivenessFactor;
    }

    public String toString() {
        String ret = "";
        ret += String.format("%d, %d, %.2f", taskId, stateSize, dataIntensivenessFactor);
//        ret += String.format("ID = %d, State size = %d, Data-intensiveness = %.2f", taskId, stateSize, dataIntensivenessFactor);

        return ret;
    }

    static public Comparator<ElasticExecutorInfo> createDataIntensivenessComparator() {
        return new Comparator<ElasticExecutorInfo>() {
            @Override
            public int compare(ElasticExecutorInfo o1, ElasticExecutorInfo o2) {
                return Double.compare(o1.dataIntensivenessFactor, o2.dataIntensivenessFactor);
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
        Collections.sort(list, createDataIntensivenessComparator());
        System.out.println(list);
    }
}
