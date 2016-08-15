package backtype.storm.elasticity.resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 4/8/16.
 */
public class ComputationResourceManager {

    Map<String, Integer> nodeIpToProcessors = new HashMap<>();

    public void registerNode(String ip, int processors) {
        if(nodeIpToProcessors.containsKey(ip)) {
            System.out.println("Node has already been registered! Ignore...");
        } else
            nodeIpToProcessors.put(ip, processors);
        System.out.println(this.toString());
    }

    public void unregisterNode(String ip) {
        nodeIpToProcessors.remove(ip);
        System.out.println(this.toString());
    }

    public String toString() {
        String ret = "Computation resource:\n";
        for(String node: nodeIpToProcessors.keySet()) {
            ret += node + ": " + nodeIpToProcessors.get(node) + "\n";
        }
        return ret;
    }


    synchronized public boolean allocateProcessOnGivenNode(String preferredNodeIp) {
        if(nodeIpToProcessors.containsKey(preferredNodeIp) && nodeIpToProcessors.get(preferredNodeIp) > 0) {
            nodeIpToProcessors.put(preferredNodeIp,nodeIpToProcessors.get(preferredNodeIp) - 1);
            System.out.println("A processor is allocated from " + preferredNodeIp);
            System.out.println(this.toString());
            return true;
        } else
            return false;

    }

    /**
     *
     * @param preferredNodeIp
     * @return the location of the processor allocated, return null if no processor is available.
     */
   synchronized public String allocateProcessOnPreferredNode(String preferredNodeIp) {

        boolean onPreferredNode = allocateProcessOnGivenNode(preferredNodeIp);
        if(onPreferredNode)
            return preferredNodeIp;
       /*
        If there is no available on the preferred node, we allocate a processor on the idlest node.
        */
        String ret = null;

        int max = 0;
        for(String ip: nodeIpToProcessors.keySet()) {
            if(nodeIpToProcessors.get(ip) > max) {
                max = nodeIpToProcessors.get(ip);
                ret = ip;
            }
        }

        if(ret!=null) {
            System.out.println("A processor is allocated from " + ret);
            nodeIpToProcessors.put(ret, nodeIpToProcessors.get(ret) - 1);
        }
        System.out.println(this.toString());

        return ret;
    }

    synchronized public void returnProcessor(String nodeIP) {

        if(nodeIpToProcessors.containsKey(nodeIP)) {
            nodeIpToProcessors.put(nodeIP,nodeIpToProcessors.get(nodeIP)+1);
            System.out.println(this.toString());
        } else {
            System.out.println("Cannot find " + nodeIP + " in the computation resource manager!");
        }
    }
}
