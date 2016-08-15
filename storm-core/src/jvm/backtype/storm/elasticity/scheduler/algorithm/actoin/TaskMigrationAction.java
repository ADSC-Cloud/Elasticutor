package backtype.storm.elasticity.scheduler.algorithm.actoin;

/**
 * Created by robert on 16-8-15.
 */
public class TaskMigrationAction implements ScheduingAction {
    public int taskID;
    public int routeID;
    public String targetIP;
}
