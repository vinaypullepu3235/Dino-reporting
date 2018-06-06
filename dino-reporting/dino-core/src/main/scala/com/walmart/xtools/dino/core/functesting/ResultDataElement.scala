package com.walmart.xtools.dino.core.functesting

import com.google.gson.annotations.Expose
import com.google.gson.annotations.SerializedName
import scala.beans.{BeanProperty, BooleanBeanProperty}

class ResultsDataElement {

  @SerializedName("docType")
  @Expose
  @BeanProperty
  var docType: String = _

  @SerializedName("updated")
  @Expose
  @BeanProperty
  var updated: String = _

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: String = _

  @SerializedName("testName")
  @Expose
  @BeanProperty
  var testName: String = _

  @SerializedName("environments.browserType")
  @Expose
  @BeanProperty
  var environmentsBrowserType: String = _

  @SerializedName("environments.retries")
  @Expose
  @BeanProperty
  var environmentsRetries: java.lang.Long = _

  @SerializedName("environments.status")
  @Expose
  @BeanProperty
  var environmentsStatus: String = _

  @SerializedName("environments.ottoURL")
  @Expose
  @BeanProperty
  var environmentsOttoURL: String = _

  @SerializedName("environments.sauceURL")
  @Expose
  @BeanProperty
  var environmentsSauceURL: String = _

  @SerializedName("projectId")
  @Expose
  @BeanProperty
  var projectId: String = _

  @SerializedName("projectName")
  @Expose
  @BeanProperty
  var projectName: String = _

  @SerializedName("phaseId")
  @Expose
  @BeanProperty
  var phaseId: String = _

  @SerializedName("phaseName")
  @Expose
  @BeanProperty
  var phaseName: String = _

  @SerializedName("runId")
  @Expose
  @BeanProperty
  var runId: String = _

  @SerializedName("runName")
  @Expose
  @BeanProperty
  var runName: String = _

  @SerializedName("triggerEvent")
  @Expose
  @BeanProperty
  var triggerEvent: String = _

  @SerializedName("startTime")
  @Expose
  @BeanProperty
  var startTime: String = _

  @SerializedName("created")
  @Expose
  @BeanProperty
  var created: String = _

  @SerializedName("endTime")
  @Expose
  @BeanProperty
  var endTime: String = _

}

