package com.walmart.xtools.dino.core.service

/** Enum for Spark config source types. */
object ConfigSourceType extends Enumeration {
  type ConfigSourceType = Value
  val CCM = Value("--ccm")
  val LOCAL = Value("--local")
}
