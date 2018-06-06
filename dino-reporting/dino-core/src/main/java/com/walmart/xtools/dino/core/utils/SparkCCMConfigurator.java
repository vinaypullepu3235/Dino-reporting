package com.walmart.xtools.dino.core.utils;

import io.strati.configuration.Configuration;
import io.strati.configuration.ConfigurationService;
import io.strati.configuration.LookupOrder;
import io.strati.configuration.context.ConfigurationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * This Class allows you to get spark job related configurations. There are two
 * ways to get your configurations namely by setting up CCM or by providing user-defined
 * configuration file or the system properties.
 */
public class SparkCCMConfigurator {
  private final static String PROVIDER_NAME = "dino-spark-streaming";
  private final static String DINO_CONFIG_NAME = "dino.config.name";
  private final static String SPARK = "spark";
  private static Logger LOG = LoggerFactory.getLogger(SparkCCMConfigurator.class);
  //private static final ConfigurationService configurationService = StratiServiceProvider.getInstance().getConfigurationService().get();
  private static final ConfigurationService configurationService = null;
  private Map<String, Map<String, String>> Properties = new HashMap<>();
  private Map<String, String> sparkJobProperties;
  private String jobName;

  /**
   * Default Constructor
   */
  public SparkCCMConfigurator() {}

  /**
   * Constructor to initialize all connectors using properties from CCM.
   *
   * @param ccmConfigName ccm configuration name
   */
  public SparkCCMConfigurator(String ccmConfigName) {
    LOG.debug(String.format("Getting Spark Job Configuration from CCM config name %s", ccmConfigName));
    try {
      Properties consumerProps = getPropertiesFromConsumerService(ccmConfigName);
      sparkJobProperties = convertToMap(consumerProps);
      jobName = ccmConfigName;
    } catch (Exception e) {
      throwException(e, "readCCMConfig", "throw");
    }
  }

  /**
   * Constructor to initialize all connectors using properties from user-defined configurations.
   *
   * @param userConfig Map of properties for all connectors
   */
  public SparkCCMConfigurator(Map<String, String> userConfig, String jobName) throws Exception {
    LOG.debug("Getting Spark Job Configuration from local configurations");
    sparkJobProperties = userConfig;
    this.jobName = jobName;
  }

  /**
   * Provides the constant string corresponding to a connector type.
   *
   * @param connectorType type of the connector
   */
  private String getDefaultConfigNameForConnectorType(String connectorType) {
    return connectorType + "-" + "default";
  }

  /**
   * Get properties from DF Service CCM for a given config file
   * @param configName  CCM config name
   */
  private Properties getPropertiesFromDFService(String configName) throws Exception {
    LOG.debug("Getting Properties from DF Service CCM config file " + configName);
    ConfigurationContext context = configurationService.getNewContext();
    context.setService(PROVIDER_NAME);
    Configuration configuration = configurationService.getConfiguration(context, configName, LookupOrder.FINAL);
    if (configuration != null) return configuration.getPropertiesSnapshot();
    else {
      throw new Exception("Cannot provide an incorrect or empty CCM Config File Name " + configName);
    }
  }

  /**
   * Get properties from Customer Service CCM for a given config file
   * @param configName  CCM config name
   */
  private Properties getPropertiesFromConsumerService(String configName) throws Exception {
    LOG.debug("Getting Properties from Customer Service CCM config file " + configName);
    ConfigurationContext context = configurationService.getNewContext();
    Configuration configuration = configurationService.getConfiguration(context, configName);
    if (configuration != null) return configuration.getPropertiesSnapshot();
    else {
      throw new Exception("Cannot provide an incorrect or empty CCM Config File Name " + configName);
    }
  }

  /**
   * Converts Properties into Map
   *
   * @param prop Properties to be converted
   * @return Map generated from Properties
   */
  private Map<String, String> convertToMap(Properties prop) {
    Map<String, String> propertiesMap = new HashMap<>();

    for (Map.Entry<Object, Object> entry : prop.entrySet()) {
      propertiesMap.put((String) entry.getKey(), (String) entry.getValue());
    }
    return propertiesMap;
  }

  public Map<String, String> getPropertiesWithId(String id, String type) {
    Map<String, String> props = new HashMap<>();
    LOG.debug(String.format("Getting properties for id %s and type %s ", id, type));
    try {
      props = Properties.get(id);
      if (props != null) return props;
      props = getUpdatedProperties(id, type);
    } catch (Exception e) {
      LOG.error("Exception during fetching information for id " + id + " of type " +
          type, e);
      throwException(e, "readProperties", type);
    }
    return props;
  }



  public Map<String, String> getUpdatedPropertiesWithId(String id, String type) {
    Map<String, String> props = new HashMap<>();
    LOG.debug(String.format("Getting properties for id %s and type %s ", id, type));
    try {
      props = Properties.get(id);
      if (props != null) return props;
    } catch (Exception e) {
      LOG.error("Exception during fetching information for id " + id + " of type " +
              type, e);
      throwException(e, "readProperties", type);
    }
    return props;
  }

  public Map<String,String> getConnectorPropertiesFromService(String configName, String type) throws Exception {
        Properties consumerProps = getPropertiesFromConsumerService(configName);
        Map<String, String> props = convertToMap(consumerProps);
        String customDinoConfig = props.get(DINO_CONFIG_NAME);
        updateMap(props, customDinoConfig, type);
        return props;
    }

    public Map<String,String> getConnectorPropertiesFromUser(Map<String, String> props , String type) throws Exception {
        String customDinoConfig = props.get(DINO_CONFIG_NAME);
        updateMap(props, customDinoConfig, type);
        return props;
    }

  private Map<String, String> getUpdatedCCMProperties(String id, String type) throws Exception {
    Map<String, String> props = getMapWithId(id + ".");
    if (props.isEmpty()) {
      LOG.debug(String.format("Invalid id {} and type %s ", id, type));
      throwException(new Exception("Invalid id"), id, "throw");
    }
    String customDinoConfig;
    switch (type) {
      case SPARK: {
        customDinoConfig = SPARK + "-" + jobName;
        break;
      }
      default: {
        props = removePrefixes(props, id);
        customDinoConfig = props.get(DINO_CONFIG_NAME);
        props.remove(DINO_CONFIG_NAME);
        break;
      }
    }
    updateMap(props, customDinoConfig, type);
    Properties.put(id, props);
    return props;
  }

  private Map<String, String> getUpdatedProperties(String id, String type) throws Exception {
    Map<String, String> props = getMapWithId(id + ".");
    if (props.isEmpty()) {
      LOG.debug(String.format("Invalid id {} and type %s ", id, type));
      throwException(new Exception("Invalid id"), id, "throw");
    }
    switch (type) {
      case SPARK: {
        break;
      }
      default: {
        props = removePrefixes(props, id);
        break;
      }
    }
    Properties.put(id, props);
    return props;
  }

  private Map<String, String> getMapWithId(String id) {
    return sparkJobProperties.entrySet().stream()
        .filter(p -> p.getKey().startsWith(id))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, String> removePrefixes(Map<String, String> props, String id) {
    id = id + ".";
    Iterator<String> iter;
    Map<String, String> connector = new HashMap<>();
    iter = props.keySet().iterator();
    while (iter.hasNext()) {
      String key = iter.next();
      if (key.startsWith(id)) {
        connector.put(key.substring(id.length()), props.get(key));
      }
    }
    return connector;
  }

  private void updateMap(Map<String, String> mainMap, String configFileName, String type) {
    Properties prop = readDefaultConfigForType(type);
    Properties customProp = new Properties();
    try {
      customProp = getPropertiesFromDFService(configFileName);
    } catch (Exception e) {
      LOG.warn(String.format("Custom config file not found for type %s. Error message : %s", type,
          e.getMessage()));
    }
    prop.putAll(customProp);
    mainMap.putAll(convertToMap(prop));
  }

  private Properties readDefaultConfigForType(String connectorType) {
    Properties prop = new Properties();
    try {
      String defaultConfig = getDefaultConfigNameForConnectorType(connectorType);
      prop = getPropertiesFromDFService(defaultConfig);
    } catch (Exception ex) {
      LOG.debug(String.format("Default config file not found for type %s. Error message : %s", connectorType,
          ex.getMessage()));
      throwException(ex, "readDefaultConfig", "throw");
    }
    return prop;
  }

  private void throwException(Exception e, String category, String group) {
    throw new RuntimeException(e);
  }
}