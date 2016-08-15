package backtype.storm.elasticity.scheduler.algorithm;

import backtype.storm.elasticity.scheduler.ElasticExecutorInfo;
import backtype.storm.elasticity.scheduler.algorithm.actoin.ScheduingAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by robert on 16-8-15.
 */
public class LocalityAndMigrationCostAwareScheduling {
    public List<ScheduingAction> schedule(Collection<ElasticExecutorInfo> executorInfos, List<String> freeCPUCores, double dataIntensivenessThreshold) {

        // create a virtual elastic executor with all the free CPU cores.
        ElasticExecutorInfo idleElasticExecutor = new ElasticExecutorInfo(-100, "no_node");
        idleElasticExecutor.updateDesirableParallelism(0);
        executorInfos.add(idleElasticExecutor);
        if(freeCPUCores.size() != 0) {
            for(String ip: freeCPUCores) {
                idleElasticExecutor.scalingOut(ip);
            }
        }

        List<ElasticExecutorInfo> sortedExecutors = new ArrayList<>(executorInfos);
        Collections.sort(sortedExecutors, ElasticExecutorInfo.createDataIntensivenessComparator());

        List<ElasticExecutorInfo> overProvisionedExecutors = new ArrayList<>();
        for(ElasticExecutorInfo info: sortedExecutors) {
            if(info.getCurrentParallelism() > info.getDesirableParallelism()) {
//                idleElasticExecutor.scalingOut(info.scalingIn());
                overProvisionedExecutors.add(info);
            }
        }

        List<ElasticExecutorInfo> nonDataIntensiveElasticExecutors = new ArrayList<>();
        nonDataIntensiveElasticExecutors.add(idleElasticExecutor);
        for(ElasticExecutorInfo info: sortedExecutors) {
            if(info.getDesirableParallelism() < dataIntensivenessThreshold) {
                nonDataIntensiveElasticExecutors.add(info);
            }
        }



        for(ElasticExecutorInfo executor: sortedExecutors) {

            if(executor.getDataIntensiveness() >= dataIntensivenessThreshold) {
                while(executor.getDesirableParallelism() < executor.getCurrentParallelism()) {

                }
            }
        }




        return null;
    }
}
