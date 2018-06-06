package com.walmart.xtools.dino.core.utils.kairos.util.api;

import java.util.List;
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO;

public interface KairosDataLoader {

    public void saveDataPoints(List<KairosDTO> metricList);

}
