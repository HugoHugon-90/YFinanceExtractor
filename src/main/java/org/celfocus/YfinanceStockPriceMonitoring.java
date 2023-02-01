package org.celfocus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Properties;

public class YfinanceStockPriceMonitoring{

    private static final Logger log = LoggerFactory.getLogger(YfinanceStockPriceMonitoring.class.getSimpleName());

    // app configs
    private static final String groupId  = "yfinance-stock-value-monitoring-application";

    // topics
    public static final String inputTopic = "yfinance-raw-input";
    public static final String afterDedupStorageTableTopic = "yfinance-after-dedup-storage-table";
    public static final String afterDedupStreamTopic = "yfinance-after-dedup-stream";
    public static final String outputAveragesTopic = "yfinance-averages-output";
    public static final String outputAlarmTopic = "yfinance-deviation-alarm";

    // json fields
    private static final String lastUpdateDateField = "lastupdatedate";
    private static final String isDuplicateField = "isduplicate";
    private static final String entityNameField = "entityname";
    private static final String currentStockPriceField = "currentstockprice";
    private static final String windowTimeField = "windowtime";
    private static final String stockPriceAvgField = "stockpriceavg";
    private static final String deviationField = "deviation";
    private static final String stockPriceAccumField = "stockpriceaccumulator";
    private static final String stockPriceCounterField = "stockpricecounter";



    public Topology createYfinanceTopology(){

        //Read tolerances and broker from properties file
        Utils utils = new Utils();
        utils.propertiesReader();

        // set windowing configs
        KeyValue<Integer, Double> fiveMinWindowAndTolerance = new KeyValue<>(5, utils.getFiveMinWindowTolerance());
        KeyValue<Integer, Double> tenMinWindowAndTolerance = new KeyValue<>(10, utils.getTenMinWindowTolerance());
        KeyValue<Integer, Double> fifteenMinWindowAndTolerance = new KeyValue<>(15, utils.getFifteenMinWindowTolerance());

        // kafka-streams builder
        StreamsBuilder builder = new StreamsBuilder();

        //custom json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // custom timestamp extractor, using the last-update date of
        // the current stock price, given by the yfinance api
        TimestampExtractor yfinanceTimeExtractor = (consumerRecord, l) ->
                ((JsonNode)consumerRecord.value()).get(lastUpdateDateField).asLong();

        // just to save some space...
        Produced<String,JsonNode> yFinanceProduced = Produced.with(Serdes.String(), jsonSerde);
        Consumed<String,JsonNode> yFinanceConsumed = Consumed.with(Serdes.String(), jsonSerde);


        // raw consumption from yfinance
        KStream<String, JsonNode> yfinanceRawStream =
                builder.stream(inputTopic, yFinanceConsumed);

        // global table to keep storage of the last record,
        // with the purpose of not allowing 2 consecutive equal records
        GlobalKTable<String, JsonNode> globalTableStorageForDedup =
                builder.globalTable(afterDedupStorageTableTopic, yFinanceConsumed);

        // de-duplicate data
        KStream<String, JsonNode> yfinanceDedupStream = filterDuplicates(yfinanceRawStream, globalTableStorageForDedup);

        // populate topics for global table (state-storage) and for stream (continue topology logic)
        yfinanceDedupStream.to(afterDedupStorageTableTopic, yFinanceProduced);
        yfinanceDedupStream.to(afterDedupStreamTopic, yFinanceProduced);

        // reads from de-duplicated data topic into KStream
        KStream<String, JsonNode> yfinanceAfterDedup =
                builder.stream(afterDedupStreamTopic, Consumed.with(Serdes.String(), jsonSerde,
                        yfinanceTimeExtractor, Topology.AutoOffsetReset.EARLIEST));


        // aggregates data in 5, 10 and 15 mins windows, computing the average stock price value
        // for each entity given by the producer
        KStream<String, JsonNode> yfinanceTimeAggregatedFiveMin =
                timeAggregatedAverageStream(yfinanceAfterDedup, fiveMinWindowAndTolerance.key, jsonSerde);
        KStream<String, JsonNode> yfinanceTimeAggregatedTenMin =
                timeAggregatedAverageStream(yfinanceAfterDedup, tenMinWindowAndTolerance.key, jsonSerde);
        KStream<String, JsonNode> yfinanceTimeAggregatedFifteenMin =
                timeAggregatedAverageStream(yfinanceAfterDedup, fifteenMinWindowAndTolerance.key, jsonSerde);


        // global table that stores the value of last average (not calculated with current event)
        GlobalKTable<String, JsonNode> yfinanceLastAverageStorageTable =
                builder.globalTable(outputAveragesTopic, yFinanceConsumed);


        // computes deviation and filters depending on the tolerance given for each windowing time
        KStream<String, JsonNode> yfinanceAlarmTriggerStreamFiveMin =
                alarmTriggerStream(yfinanceTimeAggregatedFiveMin, yfinanceLastAverageStorageTable,
                        fiveMinWindowAndTolerance);
        KStream<String, JsonNode> yfinanceAlarmTriggerStreamTenMin =
                alarmTriggerStream(yfinanceTimeAggregatedTenMin, yfinanceLastAverageStorageTable,
                        tenMinWindowAndTolerance);
        KStream<String, JsonNode> yfinanceAlarmTriggerStreamFifteenMin =
                alarmTriggerStream(yfinanceTimeAggregatedFifteenMin, yfinanceLastAverageStorageTable,
                        fifteenMinWindowAndTolerance);


        // updates the averages for each widowing time
        yfinanceTimeAggregatedFiveMin.to(outputAveragesTopic, yFinanceProduced);
        yfinanceTimeAggregatedTenMin.to(outputAveragesTopic, yFinanceProduced);
        yfinanceTimeAggregatedFifteenMin.to(outputAveragesTopic, yFinanceProduced);

        // sends alarm event if above conditions were met
        yfinanceAlarmTriggerStreamFiveMin.to(outputAlarmTopic, yFinanceProduced);
        yfinanceAlarmTriggerStreamTenMin.to(outputAlarmTopic, yFinanceProduced);
        yfinanceAlarmTriggerStreamFifteenMin.to(outputAlarmTopic, yFinanceProduced);

        return builder.build();
    }
    public static void main(String[] args) {

        //Read tolerances and broker from properties file
        Utils utils = new Utils();
        utils.propertiesReader();

        //set host
        String bootstrapServers = utils.getBrokerName() + ":" + utils.getBrokerPort();

        //consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //stream properties
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        properties.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, myExtractor.class.getName());

        //producer properties
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // build and start
        YfinanceStockPriceMonitoring yfinanceStockPriceMonitoring = new YfinanceStockPriceMonitoring();
        KafkaStreams streams = new KafkaStreams(yfinanceStockPriceMonitoring.createYfinanceTopology(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        System.out.println(streams);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static class myExtractor implements TimestampExtractor{

        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
            return ((JsonNode)consumerRecord.value()).get(lastUpdateDateField).asLong();
        }
    }

    private static KStream<String, JsonNode> filterDuplicates(
            KStream<String, JsonNode> yfinanceRawStream,
            GlobalKTable<String, JsonNode> globalTableStorageForDedup){

        return yfinanceRawStream
                .leftJoin(globalTableStorageForDedup,
                        (key, value) -> key,
                        (rawStream, globalStorageTable) -> {
                            if (globalStorageTable == null)
                                return afterDedupJson(rawStream, false);

                            else if (rawStream.get(lastUpdateDateField).asLong() ==
                                    globalStorageTable.get(lastUpdateDateField).asLong())
                                return afterDedupJson(rawStream,true);

                            else
                                return afterDedupJson(rawStream,false);
                        }
                )
                .filter((key,value) -> !value.get(isDuplicateField).asBoolean());
    }
    private static KStream<String, JsonNode> alarmTriggerStream(
              KStream<String, JsonNode> yfinanceAggregatedStream,
              GlobalKTable<String, JsonNode> yfinanceLastAverageStorageTable,
              KeyValue<Integer,Double> windowTimeAndTolerance )
    {

        return yfinanceAggregatedStream
                .join(yfinanceLastAverageStorageTable,
                        (key, value) -> key,
                        YfinanceStockPriceMonitoring::deviationCalculation
                )
                .filter(
                        (key,value) ->
                                (
                                    value.get("deviation").asDouble() >= windowTimeAndTolerance.value
                                )
                );
    }

    private static KStream<String, JsonNode> timeAggregatedAverageStream(KStream<String, JsonNode> yfinanceAfterDedup,
                                                                         int windowTime, Serde<JsonNode> jsonSerde ){
        // initializer
        ObjectNode yfinanceAccumulatorJson = JsonNodeFactory.instance.objectNode();
        yfinanceAccumulatorJson.put(stockPriceCounterField,0);
        yfinanceAccumulatorJson.put(stockPriceAccumField,0);

        return yfinanceAfterDedup
                .selectKey((key, value) -> (value.get(entityNameField).toString() + windowTime))
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(windowTime)).grace(Duration.ofMinutes(windowTime)))
                .aggregate(
                        () -> yfinanceAccumulatorJson,
                        (key, value, newValue) -> newCounterAndAccum(value, newValue),
                        Materialized.with(Serdes.String(), jsonSerde)
                )
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .mapValues(value ->  averageCalculator(value, windowTime));
    }
    private static JsonNode deviationCalculation(JsonNode value, JsonNode newValue) {

        ObjectNode newDeviation = JsonNodeFactory.instance.objectNode();

        newDeviation.put(entityNameField, value.get(entityNameField));
        newDeviation.put(windowTimeField, value.get(windowTimeField));
        newDeviation.put(deviationField,
                Math.abs(newValue.get(stockPriceAvgField).asDouble()-value.get(stockPriceAvgField).asDouble())
                        /newValue.get(stockPriceAvgField).asDouble());

        return  newDeviation;
    }

    public static JsonNode afterDedupJson(JsonNode value, boolean isDup) {

        ObjectNode afterDedup = JsonNodeFactory.instance.objectNode();

        afterDedup.put(entityNameField, value.get(entityNameField));
        afterDedup.put(currentStockPriceField, value.get(currentStockPriceField));
        afterDedup.put(lastUpdateDateField, value.get(lastUpdateDateField));
        afterDedup.put(isDuplicateField, isDup);

        return  afterDedup;
    }

    public static JsonNode newCounterAndAccum(JsonNode value, JsonNode newValue) {

        ObjectNode newCounter = JsonNodeFactory.instance.objectNode();

        newCounter.put(entityNameField, value.get(entityNameField));
        newCounter.put(stockPriceCounterField, newValue.get(stockPriceCounterField).asInt() + 1);
        newCounter.put(stockPriceAccumField, value.get(currentStockPriceField).asDouble()
                                                            + newValue.get(stockPriceAccumField).asDouble());
        return  newCounter;
    }

    public static JsonNode averageCalculator(JsonNode value, Integer windowTime) {

        ObjectNode avgJson = JsonNodeFactory.instance.objectNode();

        avgJson.put(entityNameField, value.get(entityNameField));
        avgJson.put(windowTimeField, windowTime);
        avgJson.put(stockPriceAvgField,
                value.get(stockPriceAccumField).asDouble()/(value.get(stockPriceCounterField).asDouble()));

        return  avgJson;
    }

}