package backtype.storm.elasticity.message;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import java.util.List;

/**
 * Created by robert on 27/12/16.
 */
public class LabelingTuple implements Tuple {

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String field) {
        return false;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public int fieldIndex(String field) {
        return 0;
    }

    @Override
    public List<Object> select(Fields selector) {
        return null;
    }

    @Override
    public Object getValue(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public Integer getInteger(int i) {
        return null;
    }

    @Override
    public Long getLong(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Short getShort(int i) {
        return null;
    }

    @Override
    public Byte getByte(int i) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return null;
    }

    @Override
    public Float getFloat(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public Object getValueByField(String field) {
        return null;
    }

    @Override
    public String getStringByField(String field) {
        return null;
    }

    @Override
    public Integer getIntegerByField(String field) {
        return null;
    }

    @Override
    public Long getLongByField(String field) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return null;
    }

    @Override
    public Short getShortByField(String field) {
        return null;
    }

    @Override
    public Byte getByteByField(String field) {
        return null;
    }

    @Override
    public Double getDoubleByField(String field) {
        return null;
    }

    @Override
    public Float getFloatByField(String field) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return new byte[0];
    }

    @Override
    public List<Object> getValues() {
        return null;
    }
}
