package backtype.storm.elasticity.utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A Bin holding integers.
 * <br/>
 * The number of items it can hold is not limited, but the added value of the
 * items it holds may not be higher than the given maximal size.
 */
public class Bin implements Comparator<Bin> {

    /**
     * maximal allowed added value of items.
     */
    protected long maxSize;
    /**
     * current added value of items.
     */
    protected long currentSize;
    /**
     * list of items balls bin.
     */
    protected List<Long> items;

    protected int index;

    /**
     * construct new bin with given maximal size.
     *
     * @param maxSize
     */
    public Bin(long maxSize) {
        this.maxSize = maxSize;
        this.currentSize = 0;
        this.items = new ArrayList<Long>();
    }

    public Bin(long maxSize, int index) {
        this(maxSize);
        this.index = index;
    }

    /**
     * adds given item to this bin, and increases the currentSize of the bin by
     * value of item. If item does not fit, it will not be put balls the bin and
     * false will be returned.
     *
     * @param item item to put balls bin
     * @return true if item fit balls bin, false otherwise
     */
    public boolean put(Long item) {
        if (currentSize + item <= maxSize) {
            items.add(item);
            currentSize += item;
            return true;
        } else {
            return false; // item didn't fit
        }
    }

    /**
     * removes given item from bin and reduces the currentSize of the bin by
     * value of item.
     *
     * @param item item to remove from bin
     */
    public void remove(Long item) {
        items.remove(item);
        currentSize -= item;
    }

    /**
     * returns the number of items balls this bin (NOT the added value of the
     * items).
     *
     * @return number of items balls this bin
     */
    public int numberOfItems() {
        return items.size();
    }

    /**
     * creates a deep copy of this bin.
     *
     * @return deep copy of this bin
     */
    public Bin deepCopy() {
        Bin copy = new Bin(0L);
        copy.items = new ArrayList<Long>(items); // Integers are not copied by reference
        copy.currentSize = currentSize;
        copy.maxSize = maxSize;
        return copy;
    }

    @Override
    public String toString() {
        String res = index + ": ";
        for (int i = 0; i < items.size(); i++) {
            res += items.get(i) + " ";
        }
        res += "    Size: " + currentSize + " (max: " + maxSize + ")";
        return res;
    }

    @Override
    public int compare(Bin bin, Bin bin2) {
        return Long.compare(bin.currentSize,bin2.currentSize);
    }


    public static class BinComparator implements Comparator<Bin> {

        @Override
        public int compare(Bin bin, Bin bin2) {
            return Long.compare(bin.currentSize,bin2.currentSize);
        }
    }

    public static class BinIndexComparator implements Comparator<Bin> {

        @Override
        public int compare(Bin bin, Bin bin2) {
            return Integer.compare(bin.index,bin2.index);
        }
    }

    public static void main(String [] args) {
        System.out.println(new Integer(431).hashCode() % 256);
    }
}
