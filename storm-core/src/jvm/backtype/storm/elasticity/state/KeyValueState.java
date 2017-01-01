package backtype.storm.elasticity.state;

import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Robert on 11/3/15.
 */
public class KeyValueState implements Serializable {

    private Map<Serializable, Serializable> state = new ConcurrentHashMap<>();

    public Serializable getValueByKey(Object key) {
        if (state.containsKey(key))
            return state.get(key);
        else
            return null;
    }

    public void setValueByKey(Serializable key, Serializable value) {
        state.put(key,value);
    }

    public void update(KeyValueState newState) {
        state.putAll(newState.state);
    }

    public void update(Map<Serializable, Serializable> newState) {
        state.putAll(newState);
    }

    public Map<Serializable, Serializable> getState() {
        return state;
    }

    public KeyValueState getValidState(StateFilter filter) {
        KeyValueState ret = new KeyValueState();
        for(Serializable key: state.keySet()) {
            if(filter.isValid(key)) {
                ret.setValueByKey(key, state.get(key));
            }
        }
        return ret;
    }

    public void removeValueByKey(Object key) {
        state.remove(key);
    }

    /**
     * get the state size by serializing the state and measuring the size of bytes after serialization.
     * This is very expensive when the state is large.
     * TODO: using sampling to reduce the overhead of size estimation
     * @return state size
     */
    public long  getStateSize() {
//        return SerializationUtils.serialize(this).length;

        long size = 0;
        double sampleRate = 0.1;
        Random random = new Random();
        for(Serializable key: state.keySet()) {
            try {
                if (random.nextDouble() < sampleRate)
                    size += SerializationUtils.serialize(key).length + SerializationUtils.serialize(state.get(key)).length;
            } catch (Exception e) {

            }
        }
        return (long) (size / sampleRate);
    }

    public String toString() {
        return String.format("State with %d entries: \n%s\n", state.size(), state);
    }
}
