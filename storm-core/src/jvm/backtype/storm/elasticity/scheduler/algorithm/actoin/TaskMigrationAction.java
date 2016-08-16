package backtype.storm.elasticity.scheduler.algorithm.actoin;

/**
 * Created by robert on 16-8-15.
 */
public class TaskMigrationAction extends ScheduingAction {
    public int routeID;
    public String targetIP;
    public TaskMigrationAction(int taskID, int routeID, String targetIP) {
        this.taskID = taskID;
        this.routeID = routeID;
        this.targetIP = targetIP;
    }

    public String toString() {
        return String.format("Task migration: %d  %d -> %s", taskID, routeID, targetIP);
    }
}
