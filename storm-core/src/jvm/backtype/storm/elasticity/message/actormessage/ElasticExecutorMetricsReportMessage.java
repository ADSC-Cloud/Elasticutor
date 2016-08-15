package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 8/12/16.
 */
public class ElasticExecutorMetricsReportMessage implements IMessage {
    public int taskID;
    public long stateSize;
    public long dataTransferBytesPerSecond;
    public ElasticExecutorMetricsReportMessage(int taskID, long stateSize, long dataTransferBytesPerSecond) {
        this.taskID = taskID;
        this.stateSize = stateSize;
        this.dataTransferBytesPerSecond = dataTransferBytesPerSecond;
    }

}
