package org.celfocus;

import java.io.IOException;
import java.util.Properties;

/**
 * A class used to read .properties file.
 */
public class PropertiesReader {

  double fiveMinWindowTolerance;
  double tenMinWindowTolerance;
  double fifteenMinWindowTolerance;
  String brokerName;
  String brokerPort;

  /**
   * 5 min tolerance getter.
   * @return fiveMinWindowTolerance
   */
  public double getFiveMinWindowTolerance() {
    return fiveMinWindowTolerance;
  }

  /**
   * 50 min tolerance getter.
   * @return fiveMinWindowTolerance
   */
  public double getTenMinWindowTolerance() {
    return tenMinWindowTolerance;
  }

  /**
   * 15 min tolerance getter.
   * @return fiveMinWindowTolerance
   */
  public double getFifteenMinWindowTolerance() {
    return fifteenMinWindowTolerance;
  }

  /**
   * brokerName getter.
   * @return brokerName
   */
  public String getBrokerName() {
    return brokerName;
  }

  /**
   * brokerPort getter.
   * @return brokerPort
   */
  public String getBrokerPort() {
    return brokerPort;
  }


  /**
   * Constructor.
   * Reads yfinance.properties file and acts as a setter for:
   *   double fiveMinWindowTolerance;
   *   double tenMinWindowTolerance;
   *   double fifteenMinWindowTolerance;
   *   String brokerName;
   *   String brokerPort;
   * based on these properties.
   */
  public PropertiesReader() {

    Properties properties = new Properties();
    java.net.URL url = ClassLoader
        .getSystemResource("yfinance.properties");

    try {
      properties.load(url.openStream());
    } catch (IOException fie) {
      fie.printStackTrace();
    }

    this.fiveMinWindowTolerance = Double.parseDouble(properties.getProperty("5minWindowTolerance"));
    this.tenMinWindowTolerance = Double.parseDouble(properties.getProperty("10minWindowTolerance"));
    this.fifteenMinWindowTolerance =
        Double.parseDouble(properties.getProperty("15minWindowTolerance"));
    this.brokerName = properties.getProperty("kafkaBrokerName");
    this.brokerPort = properties.getProperty("kafkaBrokerPort");
  }
}
