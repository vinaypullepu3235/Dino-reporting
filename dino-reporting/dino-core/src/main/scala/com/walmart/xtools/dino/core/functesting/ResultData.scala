package com.walmart.xtools.dino.core.functesting

class ResultData {

  private var result_data: ResultsDataElement = new ResultsDataElement()

  def getResultData(): ResultsDataElement = result_data

  def setResultData(resultData: ResultsDataElement): Unit = {
    this.result_data = resultData
  }

  override def toString(): String = "ResultData {" + result_data + "}"

}
