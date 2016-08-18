package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 11/12/15.
 */
public class TaskMigrationCommand implements ICommand {

    public String _targetHostName;
    public int _taskID;
    public int _route;

    public TaskMigrationCommand(String targetHostName, int taskId, int route) {
        _targetHostName = targetHostName;
        _taskID = taskId;
        _route = route;
    }
}
