package storm.starter.util;

import backtype.storm.generated.MasterService;
import storm.starter.generated.ResourceCentricControllerService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by robert on 5/5/16.
 */
public class ShardReassignment {
    public static void main(String[] args) {

        System.out.println("command: thrift-ip sourceTaskId targetTaskId shardId repeat[default = 1]");


        try {
            TTransport transport = new TSocket(args[0],29090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            ResourceCentricControllerService.Client thriftClient = new ResourceCentricControllerService.Client(protocol);
//            int sum = Integer.getInteger(args[0]);
//            sum += Integer.getInteger(args[1]);
//            sum += Integer.getInteger(args[2]);
//            System.out.println(sum);
            int repeat = 1;
            if(args.length >=5 && args[4]!=null) {
                repeat = Integer.parseInt(args[4]);
            }
            long totalTime = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for(int i = 0; i < repeat; i++) {
                long start = System.currentTimeMillis();
                if(i%2==0)
                    thriftClient.shardReassignment(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                else
                    thriftClient.shardReassignment(Integer.parseInt(args[2]), Integer.parseInt(args[1]), Integer.parseInt(args[3]));
                long time = System.currentTimeMillis() - start;
                min = Math.min(min, time);
                max = Math.max(max, time);
                totalTime += time;
                System.out.println(time);
            }
            System.out.println("total: " + totalTime);
            System.out.println(String.format("Max: %d  Min: %d AVG: %.2f AVG(exclusive max): %.2f\n", max, min, totalTime / ((double) repeat), (totalTime - max) / ((double) (repeat - 1))));


            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
