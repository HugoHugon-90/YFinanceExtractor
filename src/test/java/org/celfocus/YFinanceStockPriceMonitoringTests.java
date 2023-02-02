package org.celfocus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Properties;

import static org.celfocus.YfinanceStockPriceMonitoring.*;
import static org.junit.Assert.assertThat;
public class YFinanceStockPriceMonitoringTests {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();

    Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

    static Pair<Integer, Double> fiveMinWindowAndTolerance = new Pair<>(5, 0.0);
    static Pair<Integer, Double> tenMinWindowAndTolerance = new Pair<>(10, 0.0);
    static Pair<Integer, Double> fifteenMinWindowAndTolerance = new Pair<>(15, 0.0);


    void pushNewInputRecord(String topicName, String key, JsonNode value){
        TestInputTopic<String, JsonNode> inputTopic =
                testDriver.createInputTopic(topicName, stringSerializer, jsonSerializer);

        inputTopic.pipeInput(key, value);
    }

    void pushNewInputRecords(String topicName, String[] keys, JsonNode[] values){
        TestInputTopic<String, JsonNode> inputTopic =
                testDriver.createInputTopic(topicName, stringSerializer, jsonSerializer);

        for(int i = 0; i < values.length; i++){
            inputTopic.pipeInput(keys[i], values[i]);
        }
    }

    KeyValue<String, JsonNode> readTopicRecordAsKeyValue(String topicName){
        try {
            TestOutputTopic<String, JsonNode> outputTopic =
                    testDriver.createOutputTopic(topicName, stringDeserializer, jsonDeserializer);
            return outputTopic.readKeyValue();

        }catch (NoSuchElementException e){
            return null;
        }

    }

    // Before any test done
    @Before
    public void setupTopologyTestDriver(){
        Properties props = new Properties();

        // All configs from main app should be mocked here
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroupId");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                MyExtractor.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // Fetch Topology!
        YfinanceStockPriceMonitoring yfinanceStockPriceMonitoring = new YfinanceStockPriceMonitoring();
        testDriver = new TopologyTestDriver(yfinanceStockPriceMonitoring.createYfinanceTopology(), props);

    }

    // After all tests done
    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public JsonNode createInputTestJson(String entity, double currentStockPrice, Long lastUpdateDate ){
        ObjectNode inputTestJson = JsonNodeFactory.instance.objectNode();
        inputTestJson.put("entityname",entity);
        inputTestJson.put("currentstockprice",currentStockPrice);
        inputTestJson.put("lastupdatedate", lastUpdateDate);
        return inputTestJson;
    }
    public JsonNode createAverageOutputTestJson(String entity, Integer windowTime, double stockPriceAverage ){
        ObjectNode outputAverageTestJson = JsonNodeFactory.instance.objectNode();
        outputAverageTestJson.put("entityname", entity);
        outputAverageTestJson.put("windowtime", windowTime);
        outputAverageTestJson.put("stockpriceavg", stockPriceAverage);
        return outputAverageTestJson;
    }


    public JsonNode createDeviationOutputTestJson(String entity, Integer windowTime, double deviation ){
        ObjectNode outputAverageTestJson = JsonNodeFactory.instance.objectNode();
        outputAverageTestJson.put("entityname", entity);
        outputAverageTestJson.put("windowtime", windowTime);
        outputAverageTestJson.put("deviation", deviation);
        return outputAverageTestJson;
    }

    // Tests
    @Test
    public void oneInputRecordShallProduceThreeOutputAverageRecordsForEachWindowing(){

        // setup
        // create input json
        JsonNode inputTestJson = createInputTestJson("testentity1", 100.0, 1674645998L);
        String inputTestKey = "testentity1";

        // starts the testing process
        pushNewInputRecord(inputTopic ,inputTestKey, inputTestJson);

        // expected outputs
        JsonNode expectedAverageJson1 = createAverageOutputTestJson("testentity1", fiveMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey1 = "\"testentity1\"" + fiveMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson2 = createAverageOutputTestJson("testentity1", tenMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey2 = "\"testentity1\"" + tenMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson3 = createAverageOutputTestJson("testentity1", fifteenMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey3 = "\"testentity1\"" + fifteenMinWindowAndTolerance.getKey().toString();

        KeyValue<String,JsonNode> expectedAverageOutputPair1 = new KeyValue<>(expectedAverageKey1, expectedAverageJson1);
        KeyValue<String,JsonNode> expectedAverageOutputPair2 = new KeyValue<>(expectedAverageKey2, expectedAverageJson2);
        KeyValue<String,JsonNode> expectedAverageOutputPair3 = new KeyValue<>(expectedAverageKey3, expectedAverageJson3);

        // actual outputs
        ArrayList<KeyValue<String,JsonNode>> actualAverageOutputList = new ArrayList<>();
        actualAverageOutputList.add(readTopicRecordAsKeyValue(outputAveragesTopic));
        actualAverageOutputList.add(readTopicRecordAsKeyValue(outputAveragesTopic));
        actualAverageOutputList.add(readTopicRecordAsKeyValue(outputAveragesTopic));

        // testing
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair1));
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair2));
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair3));
    }

    @Test
    public void oneInputRecordProducesNoDeviationOutput(){

        // setup
        // create input json
        JsonNode inputTestJson = createInputTestJson("testentity1", 100.0, 1674645998L);
        String inputTestKey = "testentity1";

        // starts the testing process
        pushNewInputRecord(inputTopic, inputTestKey, inputTestJson);
        Assert.assertNull(readTopicRecordAsKeyValue(outputAlarmTopic));

    }
    @Test
    public void aConsecutiveRecordWithSameEntityAndTimestampShallBeDiscardedAsDuplicate(){

        // setup
        // create inputs
        JsonNode inputTestJson1 = createInputTestJson("testentity1", 100.0,java.time.Instant.now().getEpochSecond());
        String inputTestKey1 = "testentity1";
        JsonNode inputTestJson2 = createInputTestJson("testentity1", 200.0, java.time.Instant.now().getEpochSecond());//fifteenMinWindowAndTolerance.getKey());
        String inputTestKey2 = "testentity1";

        String[] inputKeys = new String[]{inputTestKey1, inputTestKey2};
        JsonNode[] inputValues = new JsonNode[]{inputTestJson1, inputTestJson2};

        // Start testing
        pushNewInputRecords(inputTopic, inputKeys, inputValues);

        // outputs to be tested (expected and not expected)
        // averages
        JsonNode expectedAverageJson1 = createAverageOutputTestJson("testentity1", fiveMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey1 = "\"testentity1\"" + fiveMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson2 = createAverageOutputTestJson("testentity1", tenMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey2 = "\"testentity1\"" + tenMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson3 = createAverageOutputTestJson("testentity1", fifteenMinWindowAndTolerance.getKey(), 100.0);
        String expectedAverageKey3 = "\"testentity1\"" + fifteenMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson4 = createAverageOutputTestJson("testentity1", fiveMinWindowAndTolerance.getKey(), 200.0);
        String expectedAverageKey4 = "\"testentity1\"" + fiveMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson5 = createAverageOutputTestJson("testentity1", tenMinWindowAndTolerance.getKey(), 200.0);
        String expectedAverageKey5 = "\"testentity1\"" + tenMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson6 = createAverageOutputTestJson("testentity1", fifteenMinWindowAndTolerance.getKey(), 200.0);
        String expectedAverageKey6 = "\"testentity1\"" + fifteenMinWindowAndTolerance.getKey();

        // expected
        KeyValue<String,JsonNode> expectedAverageOutputPair1 = new KeyValue<>(expectedAverageKey1, expectedAverageJson1);
        KeyValue<String,JsonNode> expectedAverageOutputPair2 = new KeyValue<>(expectedAverageKey2, expectedAverageJson2);
        KeyValue<String,JsonNode> expectedAverageOutputPair3 = new KeyValue<>(expectedAverageKey3, expectedAverageJson3);
        // not expected
        KeyValue<String,JsonNode> expectedAverageOutputPair4 = new KeyValue<>(expectedAverageKey4, expectedAverageJson4);
        KeyValue<String,JsonNode> expectedAverageOutputPair5 = new KeyValue<>(expectedAverageKey5, expectedAverageJson5);
        KeyValue<String,JsonNode> expectedAverageOutputPair6 = new KeyValue<>(expectedAverageKey6, expectedAverageJson6);

        ArrayList<KeyValue<String,JsonNode>> actualAverageOutputList = new ArrayList<>();
        for(int i = 0; i < 6; i++){
            actualAverageOutputList.add(readTopicRecordAsKeyValue(outputAveragesTopic));
        }

        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair1)
                &&
                actualAverageOutputList.contains(expectedAverageOutputPair2)
                &&
                actualAverageOutputList.contains(expectedAverageOutputPair3)
        );

        Assert.assertFalse(actualAverageOutputList.contains(expectedAverageOutputPair4)
                                    ||
                                    actualAverageOutputList.contains(expectedAverageOutputPair5)
                                    ||
                                    actualAverageOutputList.contains(expectedAverageOutputPair6)
                            );

    }
    @Test
    public void twoConsecutiveRecordsWithSameEntityAndLargeDeviationsShallProduceThreeAveragesAndThreeDeviationOutputs() throws InterruptedException {

        // setup
        // create inputs
        JsonNode inputTestJson1 = createInputTestJson("testentity1", 100.0,java.time.Instant.now().getEpochSecond());
        String inputTestKey1 = "testentity1";
        JsonNode inputTestJson2 = createInputTestJson("testentity1", 200.0, java.time.Instant.now().getEpochSecond() + +5L);//fifteenMinWindowAndTolerance.getKey());
        String inputTestKey2 = "testentity1";

        String[] inputKeys = new String[]{inputTestKey1, inputTestKey2};
        JsonNode[] inputValues = new JsonNode[]{inputTestJson1, inputTestJson2};

        // Start testing
        pushNewInputRecords(inputTopic, inputKeys, inputValues);

        // expected deviation
        double expectedDeviation = Math.abs(100.-((100.+200.)/2.))/100.;
        double expectedAverage = (100.+200.)/2.;

        // expected outputs

        // averages
        JsonNode expectedAverageJson1 = createAverageOutputTestJson("testentity1", fiveMinWindowAndTolerance.getKey(), expectedAverage);
        String expectedAverageKey1 = "\"testentity1\"" + fiveMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson2 = createAverageOutputTestJson("testentity1", tenMinWindowAndTolerance.getKey(), expectedAverage);
        String expectedAverageKey2 = "\"testentity1\"" + tenMinWindowAndTolerance.getKey();

        JsonNode expectedAverageJson3 = createAverageOutputTestJson("testentity1", fifteenMinWindowAndTolerance.getKey(), expectedAverage);
        String expectedAverageKey3 = "\"testentity1\"" + fifteenMinWindowAndTolerance.getKey().toString();

        KeyValue<String,JsonNode> expectedAverageOutputPair1 = new KeyValue<>(expectedAverageKey1, expectedAverageJson1);
        KeyValue<String,JsonNode> expectedAverageOutputPair2 = new KeyValue<>(expectedAverageKey2, expectedAverageJson2);
        KeyValue<String,JsonNode> expectedAverageOutputPair3 = new KeyValue<>(expectedAverageKey3, expectedAverageJson3);

        // deviations
        JsonNode expectedDeviationJson1 = createDeviationOutputTestJson("testentity1",
                fiveMinWindowAndTolerance.getKey(), expectedDeviation);
        String expectedDeviationKey1 = "\"testentity1\"" + fiveMinWindowAndTolerance.getKey();

        JsonNode expectedDeviationJson2 = createDeviationOutputTestJson("testentity1",
                tenMinWindowAndTolerance.getKey(), expectedDeviation);
        String expectedDeviationKey2 = "\"testentity1\"" + tenMinWindowAndTolerance.getKey();

        JsonNode expectedDeviationJson3 = createDeviationOutputTestJson("testentity1",
                fifteenMinWindowAndTolerance.getKey(), expectedDeviation);
        String expectedDeviationKey3 = "\"testentity1\"" + fifteenMinWindowAndTolerance.getKey();

        KeyValue<String,JsonNode> expectedDeviationOutputPair1 = new KeyValue<>(expectedDeviationKey1, expectedDeviationJson1);
        KeyValue<String,JsonNode> expectedDeviationOutputPair2 = new KeyValue<>(expectedDeviationKey2, expectedDeviationJson2);
        KeyValue<String,JsonNode> expectedDeviationOutputPair3 = new KeyValue<>(expectedDeviationKey3, expectedDeviationJson3);

        // actual outputs
        ArrayList<KeyValue<String,JsonNode>> actualAverageOutputList = new ArrayList<>();
        ArrayList<KeyValue<String,JsonNode>> actualDeviationOutputList = new ArrayList<>();
        // instantiate the 3 readings (each window) of outputAveragesTopic, from which 
        // outputAlarmTopic depends
        for(int i = 0; i < inputValues.length*3; i++){
            actualAverageOutputList.add(readTopicRecordAsKeyValue(outputAveragesTopic));
        }
        for(int i = 0; i < inputValues.length*3; i++){
            actualDeviationOutputList.add(readTopicRecordAsKeyValue(outputAlarmTopic));
        }

        // Assert
        // averages
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair1));
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair2));
        Assert.assertTrue(actualAverageOutputList.contains(expectedAverageOutputPair3));
        // deviations
        Assert.assertTrue(actualDeviationOutputList.contains(expectedDeviationOutputPair1));
        Assert.assertTrue(actualDeviationOutputList.contains(expectedDeviationOutputPair2));
        Assert.assertTrue(actualDeviationOutputList.contains(expectedDeviationOutputPair3));

    }
}
