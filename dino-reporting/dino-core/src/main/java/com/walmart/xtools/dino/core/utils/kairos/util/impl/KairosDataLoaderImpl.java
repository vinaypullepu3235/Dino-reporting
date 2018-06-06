package com.walmart.xtools.dino.core.utils.kairos.util.impl;

import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO;
import com.walmart.xtools.dino.core.utils.kairos.util.api.KairosDataLoader;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KairosDataLoaderImpl implements KairosDataLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(com.walmart.xtools.dino.core.utils.kairos.util.impl.KairosDataLoaderImpl.class);
    private String endPoint = null;

    public KairosDataLoaderImpl(String kairosEndPoint) {
        endPoint = kairosEndPoint;
        LOGGER.info("Initializing KairosDataLoaderImpl with endpoint: "+endPoint);
    }

    @Override
    public void saveDataPoints(List<KairosDTO> metricList) {

        HttpClient client = null;

        try{
            MetricBuilder builder = MetricBuilder.getInstance();
            client  = new HttpClient(endPoint);
            int counter =0;
            for(int i=0; i<metricList.size(); i++){
                counter++;
                KairosDTO kairosDTO = metricList.get(i);
                Metric metric = builder.addMetric(kairosDTO.getName());
                metric.addTags(kairosDTO.getTags());

                metric.addTtl(kairosDTO.getTtl());
                metric.addDataPoint(kairosDTO.getTimestamp(), kairosDTO.getValue());
                if(counter==1000){
                    client.pushMetrics(builder);
                    builder = MetricBuilder.getInstance();
                    counter =0;
                }
            }

            if(counter!=0) {
                client.pushMetrics(builder);
            }
        }
        catch (Exception e){
            LOGGER.error(ExceptionUtils.getStackTrace(e));
        }finally {
            try{
                client.shutdown();
            }
            catch (Exception e){
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }
    }

}
