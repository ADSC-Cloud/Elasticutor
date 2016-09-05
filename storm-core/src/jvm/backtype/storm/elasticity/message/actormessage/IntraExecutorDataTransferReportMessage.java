package backtype.storm.elasticity.message.actormessage;

/**
 * Created by robert on 5/9/16.
 */
public class IntraExecutorDataTransferReportMessage implements IMessage {
    public long dataTransferSize;
    public IntraExecutorDataTransferReportMessage(long size) {
        dataTransferSize = size;
    }
}
