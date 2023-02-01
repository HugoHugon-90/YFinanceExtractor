package org.celfocus;

import java.io.*;
import java.util.Properties;

public class Utils {

    double fiveMinWindowTolerance;
    double tenMinWindowTolerance;
    double fifteenMinWindowTolerance;
    String brokerName;
    String brokerPort;

    public double getFiveMinWindowTolerance() {
        return fiveMinWindowTolerance;
    }
    public double getTenMinWindowTolerance() {
        return tenMinWindowTolerance;
    }
    public double getFifteenMinWindowTolerance() {
        return fifteenMinWindowTolerance;
    }
    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerPort() {
        return brokerPort;
    }

    public void propertiesReader() {

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
        this.fifteenMinWindowTolerance = Double.parseDouble(properties.getProperty("15minWindowTolerance"));
        this.brokerName = properties.getProperty("kafkaBrokerName");
        this.brokerPort = properties.getProperty("kafkaBrokerPort");
    }
}
