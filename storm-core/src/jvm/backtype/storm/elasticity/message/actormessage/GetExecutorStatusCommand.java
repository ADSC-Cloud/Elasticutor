package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 9/12/16.
 */
public class GetExecutorStatusCommand implements ICommand {
    public int taskid;
    public GetExecutorStatusCommand(int taskid) {
        this.taskid = taskid;
    }
}
