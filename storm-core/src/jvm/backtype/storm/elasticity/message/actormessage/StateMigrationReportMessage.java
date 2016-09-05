package backtype.storm.elasticity.message.actormessage;

/**
 * Created by Robert on 9/3/16.
 */
public class StateMigrationReportMessage implements IMessage {
    public long stateSize;
    public StateMigrationReportMessage(long stateSize) {
        this.stateSize = stateSize;
    }
}
