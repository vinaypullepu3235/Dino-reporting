package com.walmart.xtools.dino.core.utils.Solr;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.common.SolrInputDocument;

import java.text.ParseException;



public class EventParseUtil {

    /*public EventParseUtil() {
        super();
    }


    static SolrInputDocument convertData (String incomingRecord) throws NumberFormatException, ParseException {

        String jsonStr = JsonFlattener.flatten(incomingRecord);
        //System.out.println("Here is the flatten json "+ jsonStr);
        Gson gson = new Gson();
        ResultData eventDataElement = gson.fromJson(jsonStr,ResultData.class);

        SolrInputDocument solrDoc =
                SolrSupport.autoMapToSolrInputDoc(String.valueOf(1234), eventDataElement, null);
        //eventDataElement.getResultData().getId()
        return solrDoc;
    }*/
}
