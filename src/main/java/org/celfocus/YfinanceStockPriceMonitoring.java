package org.celfocus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 In this code we use YFinance API to extract, in real time, the current stock price of a list of
 entities, (as of now: meta, ibm, amazon, apple, general motors and vanguard_etf), continuously
 processing their current-price moving average for 5, 10 and 15 minute windows.
 Duplicates, i.e., input values from YFInance with the same update time, are filtered and ignored
 in the ETL process.

 For each new ocurrence, and for each average-window, we compare the newest average
 aggregation with the previous stored one, computing its absolute deviation from the latter.
 If the deviations are larger than a user-defined tolerance, an alarm is produced.

 Technologies:
 - Extractor is done with python3.
 - Data is stored using Kafka.
 - ETL is performed using Kafka Streams.
 - Environment is local and self-contained, deployed and managed using kubernetes
   in a minikube cluster and docker images.
 */
public class YfinanceStockPriceMonitoring {

  // topics
  public static final String inputTopic = "yfinance-raw-input";
  public static final String afterDedupStorageTableTopic = "yfinance-after-dedup-storage-table";
  public static final String afterDedupStreamTopic = "yfinance-after-dedup-stream";
  public static final String outputAveragesTopic = "yfinance-averages-output";
  public static final String outputAlarmTopic = "yfinance-deviation-alarm";
  private static final Logger log =
      LoggerFactory.getLogger(YfinanceStockPriceMonitoring.class.getSimpleName());
  // app configs
  private static final String groupId = "yfinance-stock-value-monitoring-application";
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


  /**
   * Main method,where Consumer/Producer/Streams Configs are setup,
   * Kafka-Streams is initialized and built with the above topology,
   * Streams App is started, and a proper shutdown is offered.
   *
   * @param args a list of String. Not used here.
   */
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
    properties.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        MyExtractor.class.getName());

    //producer properties
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    // build and start
    YfinanceStockPriceMonitoring yfinanceStockPriceMonitoring = new YfinanceStockPriceMonitoring();
    KafkaStreams streams =
        new KafkaStreams(yfinanceStockPriceMonitoring.createYfinanceTopology(), properties);

    streams.cleanUp(); // only do this in dev - not in prod
    streams.start();

    // print the topology
    System.out.println(streams);

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

  /**
   * Performs a Left-Join using the key between a KStream and a GlobalKTable. The key is the
   * entityNameField. Both sides of the Join have a JsonNode with the following fields:
   * - entityNameField;
   * - currentStockPriceField;
   * - lastUpdateDateField.
   * Computes the json given by the afterDedupJson method, with the duplicate flag == true
   * when the right value is not null (i.e., there is a key to join in the GlobalKTable)
   * AND the JsonField lastUpdateDateField from the input value is equal in both sides
   * of the join.
   * Then, it returns the event if and only if the isDuplicateField from the above computed
   * value is false.
   *
   * @param yfinanceRawStream a KStream, fed with Raw input (therefore with potentially
   *                          duplicated data).
   * @param globalTableStorageForDedup a GlobalKtable, which stores the exact-previous event
   *                                   from yfinanceRawStream.
   *
   * @return a Kstream with a non-duplicate event.
   */
  public static KStream<String, JsonNode> filterDuplicates(
      KStream<String, JsonNode> yfinanceRawStream,
      GlobalKTable<String, JsonNode> globalTableStorageForDedup) {

    return yfinanceRawStream.leftJoin(globalTableStorageForDedup, (key, value) -> key,
        (rawStream, globalStorageTable) -> {
          if (globalStorageTable == null) {
            return afterDedupJson(rawStream, false);
          } else if (rawStream.get(lastUpdateDateField).asLong()
              == globalStorageTable.get(lastUpdateDateField).asLong()) {
            return afterDedupJson(rawStream, true);
          } else {
            return afterDedupJson(rawStream, false);
          }
        }).filter((key, value) -> !value.get(isDuplicateField).asBoolean());
  }

  /**
   * Performs an Inner-Join using the key between a KStream and a GlobalKTable.
   * The key is the entityNameField+windowTimeField.
   * Both sides of the Join have a JsonNode with the following fields:
   * - entityNameField;
   * - windowTimeField;
   * - stockPriceAvgField.
   * Computes the Json given by the deviationCalculation method, where deviationField is calculated.
   * This calculation is done using the exact-previous stockPriceAvgField from
   * yfinanceAggregatedStream, which is stored in the GlobalKTable.
   * Then, it returns the event if and only if the deviationField from the above computed
   * value is greater than a user given-tolerance (check yfinance.properties file),
   * given as a param.
   *
   * @param yfinanceAggregatedStream a KStream, with the fields described above.
   * @param yfinanceLastAverageStorageTable a GlobalKtable, which stores the exact-previous event
   *                                       from yfinanceAggregatedStream.
   * @param windowTimeAndTolerance a KeyValue< Integer, Double >, which stores the tolerance (value)
   *                               for each windowing time: 5, 10, or 15 (key).
   *
   * @return a Kstream with a non-duplicate event.
   */
  public static KStream<String, JsonNode> alarmTriggerStream(
      KStream<String, JsonNode> yfinanceAggregatedStream,
      GlobalKTable<String, JsonNode> yfinanceLastAverageStorageTable,
      KeyValue<Integer, Double> windowTimeAndTolerance) {

    return yfinanceAggregatedStream.join(yfinanceLastAverageStorageTable, (key, value) -> key,
        YfinanceStockPriceMonitoring::deviationCalculation).filter(
              (key, value) -> (value.get("deviation").asDouble() >= windowTimeAndTolerance.value)
        );
  }

  /**
   * Performs a time-aggregation of a KStream within a windowing time given as a param.
   * - Receives a JsonNode with key entityNameField, and values
   *   - entityNameField;
   *   - currentStockPriceField;
   *   - lastUpdateDateField.
   * - Repartitions the key into entityNameField+windowTime.
   * - GroupBy Key and aggregates in windowTime minutes, using lastUpdateDateField as
   *   time reference, and aggregating using newCounterAndAccum method.
   * - Computes the stock-price average using averageCalculator method.
   * - Returns a JsonNode with the key entityNameField+windowTimeField, and values
   *   - entityNameField
   *   - windowTimeField
   *   - stockPriceAvgField
   *
   * @param yfinanceAfterDedup a KStream, with the input fields described above.
   * @param windowTime time, in minutes for which the aggregation shall be performed.
   * @param jsonSerde a Json serializer/deserializer.
   *
   * @return a Kstream with the output key/values described above.
   */
  public static KStream<String, JsonNode> timeAggregatedAverageStream(
      KStream<String, JsonNode> yfinanceAfterDedup, int windowTime, Serde<JsonNode> jsonSerde) {
    // initializer
    ObjectNode yfinanceAccumulatorJson = JsonNodeFactory.instance.objectNode();
    yfinanceAccumulatorJson.put(stockPriceCounterField, 0);
    yfinanceAccumulatorJson.put(stockPriceAccumField, 0);

    return yfinanceAfterDedup.selectKey(
            (key, value) -> (value.get(entityNameField).toString() + windowTime))
        .groupByKey(Grouped.with(Serdes.String(), jsonSerde)).windowedBy(
            TimeWindows.of(Duration.ofMinutes(windowTime)).grace(Duration.ofMinutes(windowTime)))
        .aggregate(() -> yfinanceAccumulatorJson,
            (key, value, newValue) -> newCounterAndAccum(value, newValue),
            Materialized.with(Serdes.String(), jsonSerde)).toStream()
        .map((wk, value) -> KeyValue.pair(wk.key(), value))
        .mapValues(value -> averageCalculator(value, windowTime));
  }


  /**
   * Calculates the deviation between the ongoing average and the exact-previous one.
   * - Receives a JsnNode with key entityNameField+windowTimeField, and values
   *   - entityNameField;
   *   - windowTimeField;
   *   - stockPriceAvgField.
   * - Returns a JsonNode with the key entityNameField+windowTimeField, and values
   *   - entityNameField;
   *   - windowTimeField;
   *   - deviationField;
   * where the value of deviationField is calculated through the formula
   * abs(avgStockPriceOld-avgStockPriceNew)/avgStockPriceNew, where avgStockPriceOld and
   * avgStockPriceNew are the stock price averages of the previous and newest occurence,
   * respectively.
   *
   * @param value a JsonNode, with the input fields described above.
   * @param newValue a JsonNode, with the output fields described above.
   *
   * @return a JsonNode with the output field-values described above.
   */
  public static JsonNode deviationCalculation(JsonNode value, JsonNode newValue) {

    ObjectNode newDeviation = JsonNodeFactory.instance.objectNode();

    newDeviation.put(entityNameField, value.get(entityNameField));
    newDeviation.put(windowTimeField, value.get(windowTimeField));
    newDeviation.put(deviationField, Math.abs(
        newValue.get(stockPriceAvgField).asDouble() - value.get(stockPriceAvgField).asDouble())
        / newValue.get(stockPriceAvgField).asDouble());

    return newDeviation;
  }

  /**
   * Constructs a Json with an added field "isDuplicateField". If this flag is true it means
   * the event is a duplicate.
   *
   * @param value the json event that will be reconstructed with the additional flag in the
   *              Json field "isDuplicateField".
   * @param isDup the flag that instantiates the "isDuplicateField" Json field.
   *
   * @return a JsonNode with the enrichment "isDuplicateField" Json field.
   */
  public static JsonNode afterDedupJson(JsonNode value, boolean isDup) {

    ObjectNode afterDedup = JsonNodeFactory.instance.objectNode();

    afterDedup.put(entityNameField, value.get(entityNameField));
    afterDedup.put(currentStockPriceField, value.get(currentStockPriceField));
    afterDedup.put(lastUpdateDateField, value.get(lastUpdateDateField));
    afterDedup.put(isDuplicateField, isDup);

    return afterDedup;
  }

  /**
   * Calculates a counter and a stock-price value accumulator, for the posterior calculation
   * of the stock-price average.Creates a JsonNode with the fields:
   * - entityNameField;
   * - stockPriceCounterField;
   * - stockPriceAccumField.
   * This method is to be used as a lambda within an KStream windowed aggregation.
   *
   * @param value previous Json event of the aggregation.
   * @param newValue newest Json event of the aggregation.
   *
   * @return a JsonNode with the above-mentioned Json fields, where counter and accumulator
   *         are calculated.
   */
  public static JsonNode newCounterAndAccum(JsonNode value, JsonNode newValue) {

    ObjectNode newCounter = JsonNodeFactory.instance.objectNode();

    newCounter.put(entityNameField, value.get(entityNameField));
    newCounter.put(stockPriceCounterField, newValue.get(stockPriceCounterField).asInt() + 1);
    newCounter.put(stockPriceAccumField, value.get(currentStockPriceField).asDouble()
        + newValue.get(stockPriceAccumField).asDouble());
    return newCounter;
  }


  /**
   * Calculates the stock-price average, using accum/counter, from the event given by
   * newCounterAndAccum method. Constructs a JsonNode with the fields:
   * - entityNameField;
   * - windowTimeField;
   * - stockPriceAvgField.
   * This method is to be used as a lambda within an KStream windowed aggregation.
   *
   * @param value previous Json event of the aggregation.
   * @param windowTime windowing time: 5, 10, or 15 minutes.
   *
   * @return a JsonNode with the above-mentioned Json fields, where stock price average is
   *         calculated.
   */
  public static JsonNode averageCalculator(JsonNode value, Integer windowTime) {

    ObjectNode avgJson = JsonNodeFactory.instance.objectNode();

    avgJson.put(entityNameField, value.get(entityNameField));
    avgJson.put(windowTimeField, windowTime);
    avgJson.put(stockPriceAvgField, value.get(stockPriceAccumField).asDouble()
        / (value.get(stockPriceCounterField).asDouble()));

    return avgJson;
  }


  /**
   * YFinance current stock price and its last update date are fetched using YFinance API
   * and a Python extractor. Data is fetched for several entities once each minute into a Kafka
   * topic, which will feed the Kafka Streams application.
   * Kafka Streams application will have three main functional stages:
   * a: Deduplication of events with the same entity and last update date.
   * b: Moving average aggregation of the current stock price into 5, 10 an 15-minute windows.
   * c: Calculation of the deviation between the ongoing average (calculated with the ongoing event)
   *    and last calculated one (without the ongoing event), sending it into an alarm-output kafka
   *    topic if the deviation, calculated for each windowing time, is greater than a given user
   *    tolerance.
   * Steps a. and c. are stateful operations, and therefore a GlobalKTable is used to store the
   * state of the exact-previous event, needed for the deduplication and deviation calculus,
   * respectively.
   * Output topics are the averages and the alarm outputs. All other topics are internal.
   *
   */
  public Topology createYfinanceTopology() {

    //Read tolerances and broker from properties file
    Utils utils = new Utils();
    utils.propertiesReader();

    // set windowing configs
    KeyValue<Integer, Double> fiveMinWindowAndTolerance =
        new KeyValue<>(5, utils.getFiveMinWindowTolerance());
    KeyValue<Integer, Double> tenMinWindowAndTolerance =
        new KeyValue<>(10, utils.getTenMinWindowTolerance());
    KeyValue<Integer, Double> fifteenMinWindowAndTolerance =
        new KeyValue<>(15, utils.getFifteenMinWindowTolerance());

    // kafka-streams builder
    StreamsBuilder builder = new StreamsBuilder();

    //custom json Serde
    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    // custom timestamp extractor, using the last-update date of
    // the current stock price, given by the yfinance api
    TimestampExtractor yfinanceTimeExtractor =
        (consumerRecord, l) -> ((JsonNode) consumerRecord.value()).get(lastUpdateDateField)
            .asLong();

    // just to save some space...
    Produced<String, JsonNode> yfinanceProduced = Produced.with(Serdes.String(), jsonSerde);
    Consumed<String, JsonNode> yfinanceConsumed = Consumed.with(Serdes.String(), jsonSerde);


    // raw consumption from yfinance
    KStream<String, JsonNode> yfinanceRawStream = builder.stream(inputTopic, yfinanceConsumed);

    // global table to keep storage of the last record,
    // with the purpose of not allowing 2 consecutive equal records
    GlobalKTable<String, JsonNode> globalTableStorageForDedup =
        builder.globalTable(afterDedupStorageTableTopic, yfinanceConsumed);

    // de-duplicate data
    KStream<String, JsonNode> yfinanceDedupStream =
        filterDuplicates(yfinanceRawStream, globalTableStorageForDedup);

    // populate topics for global table (state-storage) and for stream (continue topology logic)
    yfinanceDedupStream.to(afterDedupStorageTableTopic, yfinanceProduced);
    yfinanceDedupStream.to(afterDedupStreamTopic, yfinanceProduced);

    // reads from de-duplicated data topic into KStream
    KStream<String, JsonNode> yfinanceAfterDedup = builder.stream(afterDedupStreamTopic,
        Consumed.with(Serdes.String(), jsonSerde, yfinanceTimeExtractor,
            Topology.AutoOffsetReset.EARLIEST));


    // aggregates data in 5, 10 and 15 mins windows, computing the average stock price value
    // for each entity given by the producer
    KStream<String, JsonNode> yfinanceTimeAggregatedFiveMin =
        timeAggregatedAverageStream(yfinanceAfterDedup, fiveMinWindowAndTolerance.key, jsonSerde);
    KStream<String, JsonNode> yfinanceTimeAggregatedTenMin =
        timeAggregatedAverageStream(yfinanceAfterDedup, tenMinWindowAndTolerance.key, jsonSerde);
    KStream<String, JsonNode> yfinanceTimeAggregatedFifteenMin =
        timeAggregatedAverageStream(yfinanceAfterDedup, fifteenMinWindowAndTolerance.key,
            jsonSerde);


    // global table that stores the value of last average (not calculated with current event)
    GlobalKTable<String, JsonNode> yfinanceLastAverageStorageTable =
        builder.globalTable(outputAveragesTopic, yfinanceConsumed);


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

    // sends alarm event if above conditions were met
    yfinanceAlarmTriggerStreamFiveMin.to(outputAlarmTopic, yfinanceProduced);
    yfinanceAlarmTriggerStreamTenMin.to(outputAlarmTopic, yfinanceProduced);
    yfinanceAlarmTriggerStreamFifteenMin.to(outputAlarmTopic, yfinanceProduced);

    // updates the averages for each widowing time
    yfinanceTimeAggregatedFiveMin.to(outputAveragesTopic, yfinanceProduced);
    yfinanceTimeAggregatedTenMin.to(outputAveragesTopic, yfinanceProduced);
    yfinanceTimeAggregatedFifteenMin.to(outputAveragesTopic, yfinanceProduced);


    return builder.build();
  }

  /**
   * Custom TimestampExtractor that implements the extract method in the
   * TimestampExtractor interface in Kafka-streams:2.4.1.
   * It tells the TimeStampExtractor what time to extract, hence to serve as time-reference
   * in kafka-streams time-windowing aggregations. In our case, our timestamp will come
   * from the input Json, which contains the field lastUpdateDateField.
   */
  public static class MyExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
      return ((JsonNode) consumerRecord.value()).get(lastUpdateDateField).asLong();
    }
  }

}