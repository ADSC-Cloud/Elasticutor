package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import backtype.storm.metric.SystemBolt;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by robert on 12/16/15.
 */
public class MigrateBucket {
    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("args: taskid, bucketid, originalroute, new route, repeat [default = 1] ");
            return;
        }

        int repeat = 1;
        if(args.length > 4 && args[4]!=null) {
            repeat = Integer.parseInt(args[4]);
        }

        backtype.storm.elasticity.config.Config.overrideFromStormConfigFile();
        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            MasterService.Client thriftClient = new MasterService.Client(protocol);

            long totalTime = 0;
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            for(int i = 0; i < repeat; i++) {
                long start = System.currentTimeMillis();
                if(i%2 == 0)
                    thriftClient.reassignBucketToRoute(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                else
                    thriftClient.reassignBucketToRoute(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[3]), Integer.parseInt(args[2]));
                long time = System.currentTimeMillis() - start;
                max = Math.max(max, time);
                min = Math.min(min, time);
                totalTime +=time;
                System.out.println(time);
            }
            transport.close();

            System.out.println(String.format("Min: %d, Max: %d, Avg: %.4f", min, max, totalTime / (double) repeat));

        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("finished!");
    }
}
