package org.celfocus;

import java.io.*;
import java.util.Properties;

public class Utils {

    public double[] propertiesReader() {

        Properties properties = new Properties();
        java.net.URL url = ClassLoader
                .getSystemResource("tolerances.properties");

        try {
            properties.load(url.openStream());
        } catch (FileNotFoundException fie) {
            fie.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        double fiveMinWindowTolerance = Double.parseDouble(properties.getProperty("5minWindowTolerance"));
        double tenMinWindowTolerance = Double.parseDouble(properties.getProperty("10minWindowTolerance"));
        double fifteenMinWindowTolerance = Double.parseDouble(properties.getProperty("15minWindowTolerance"));

        return new double[]{fiveMinWindowTolerance, tenMinWindowTolerance, fifteenMinWindowTolerance};
    }
}
