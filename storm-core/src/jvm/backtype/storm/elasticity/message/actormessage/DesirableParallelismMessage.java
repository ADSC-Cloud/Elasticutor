package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 16-8-15.
 */
public class DesirableParallelismMessage implements IMessage {
    public int desriableParallelism;
    public int taskID;
    public DesirableParallelismMessage(int taskID, int desriableParallelism) {
        this.taskID = taskID;
        this.desriableParallelism = desriableParallelism;
    }
}
