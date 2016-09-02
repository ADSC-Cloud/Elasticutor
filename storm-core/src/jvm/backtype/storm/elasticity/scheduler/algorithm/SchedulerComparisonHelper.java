package backtype.storm.elasticity.scheduler.algorithm;


import backtype.storm.elasticity.scheduler.ElasticExecutorInfo;

import java.util.List;

/**
 * Created by robert on 2/9/16.
 */
public class SchedulerComparisonHelper {

    static public List<ElasticExecutorInfo> disableStateSizeInfo(List<ElasticExecutorInfo> infos) {
        for(ElasticExecutorInfo info: infos) {
            info.updateStateSize(0);
        }
        return infos;
    }

    static public List<ElasticExecutorInfo> disableDataIntensivenessInfo(List<ElasticExecutorInfo> infos) {
        for(ElasticExecutorInfo info: infos) {
            info.updateDataIntensivenessFactor(0);
        }
        return infos;
    }
}
