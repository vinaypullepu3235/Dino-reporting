package com.walmart.xtools.dino.core.functesting

import com.github.wnameless.json.flattener.JsonFlattener

import com.google.gson.Gson

import com.lucidworks.spark.util.SolrSupport

import org.apache.solr.common.SolrInputDocument

import java.text.ParseException

import EventParseUtil._

object EventParseUtil {

  def convertData(incomingRecord: String): SolrInputDocument = {
    val jsonStr: String = JsonFlattener.flatten(incomingRecord)

    val gson: Gson = new Gson()

    val eventDataElement: ResultData =
      gson.fromJson(jsonStr, classOf[ResultData])

    val solrDoc: SolrInputDocument = SolrSupport.autoMapToSolrInputDoc(
      String.valueOf(1234),
      eventDataElement,
      null)
    //eventDataElement.getResultData().getId()
    solrDoc
  }

}

class EventParseUtil