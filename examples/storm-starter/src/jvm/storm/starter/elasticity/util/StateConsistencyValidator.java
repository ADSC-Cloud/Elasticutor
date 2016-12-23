package storm.starter.elasticity.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 22/12/16.
 */
public class StateConsistencyValidator implements Serializable {

    private int numberOfKeys;
    final private int checkFrequency;

    public class Generator {
        Map<Integer, ValidateState> keyToState = new HashMap<>();
        public Generator() {
            for (int i = 0; i < numberOfKeys; i++) {
                keyToState.put(i, new ValidateState(0, 0));
            }
        }

        Random random = new Random();
        public ValidateTuple generate() {
            int key = random.nextInt(numberOfKeys);
            ValidateState state = keyToState.get(key);
            long count = state.count;
            long value;
            if (count % checkFrequency == 0) {
                value = state.value;
            } else {
                value = random.nextLong();
            }

            state.value = state.value ^ value;
            state.count = state.count + 1;
            return new ValidateTuple(key, value);
        }
    }

    public Generator createGenerator() {
        return new Generator();
    }

    public boolean validate(long value, ValidateState state) {
        state.value ^= value;
        final long count = state.count;
        boolean ret = true;
        if (count % checkFrequency ==0) {
            ret = state.value == 0;
            state.value = 0;
        }
        state.count += 1;
        return ret;
    }

    public static class ValidateState implements Serializable {
        public long count;
        public long value;
        public ValidateState(long count, long value) {
            this.count = count;
            this.value = value;
        }
        public ValidateState() {
            this.count = 0;
            this.value = 0;
        }
    }

    public static class ValidateTuple implements Serializable {
        public Integer key;
        public Long value;
        public ValidateTuple(int key, long value) {
            this.key = key;
            this.value = value;
        }
        public String toString() {
            return String.format("key: %d, value: %d.", key, value);
        }
    }

    public StateConsistencyValidator(int numberOfKeys, int checkFrequency) {
        this.numberOfKeys = numberOfKeys;
        this.checkFrequency = checkFrequency;
    }


    static public void main(String[] args) {
        StateConsistencyValidator stateConsistencyValidator = new StateConsistencyValidator(10000,10);
        StateConsistencyValidator.Generator generator = stateConsistencyValidator.createGenerator();
        Map<Integer, ValidateState> keyToState = new HashMap<>();
        int round = 0;
        while (true) {
            for (int i = 0; i < 10000; i++) {
                ValidateTuple tuple = generator.generate();
                if (!keyToState.containsKey(tuple.key)) {
                    keyToState.put(tuple.key, new ValidateState());
                }
                ValidateState state = keyToState.get(tuple.key);
                if (!stateConsistencyValidator.validate(tuple.value, state))
                    System.out.println("Error!");
                keyToState.put(tuple.key, state);
            }
            System.out.println("round " + round++);
        }
    }




}
