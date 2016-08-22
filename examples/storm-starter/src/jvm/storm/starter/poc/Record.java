package storm.starter.poc;


import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;

/**
 * Created by robert on 25/5/16.
 */
public class Record implements Serializable {
    public long orderNo;
    public String accountId;
    public double price;
    public int volume;
    public int secCode;
//    public String date;
//    public String time;
//    public String millisecond;
    Date date;

//    public Record(long orderNo, String accountId, double price, int volume, int secCode, String date, String time, String millisecond) {
//        this.orderNo = orderNo;
//        this.accountId = accountId;
//        this.price = price;
//        this.volume = volume;
//        this.secCode = secCode;
//        this.date = date;
//        this.time = time;
//        this.millisecond = millisecond;
//    }
    public Record(long orderNo, String accountId, double price, int volume, int secCode, Date date) {
        this.orderNo = orderNo;
        this.accountId = accountId;
        this.price = price;
        this.volume = volume;
        this.secCode = secCode;
        this.date = date;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Record && ((Record) obj).orderNo == orderNo;
    }

    public static Comparator<Record> getPriceComparator() {
        return new Comparator<Record>() {
            @Override
            public int compare(Record record, Record record2) {
                return Double.compare(record.price, record2.price);
            }
        };
    }

    public static Comparator<Record> getPriceReverseComparator() {
        return new Comparator<Record>() {
            @Override
            public int compare(Record record, Record record2) {
                return Double.compare(record2.price, record.price);
            }
        };
    }

//    public static Comparator<Record> getTimeComparator() {
//        return new Comparator<Record>() {
//            @Override
//            public int compare(Record o1, Record o2) {
//
//            }
//        }
//    }
}
