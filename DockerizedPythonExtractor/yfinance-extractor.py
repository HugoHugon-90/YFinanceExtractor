import yfinance as yf
import time
import json
import numpy


from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='server-k8s-service:9092', client_id="stock_price_python_producer")
output_topic = "yfinance-raw-input"


def get_latest_stock_prices_random_generator(date_counter=0, verbose=False):

    list_entities = ["meta", "ibm", "amazon", "apple", "general_motors"]

    for entity in list_entities:
        try:

            output_dict = {
                "entityname": entity,
                "currentstockprice": round(numpy.random.uniform(100, 1000), 2),
                "lastupdatedate": 1674645943 + date_counter,
            }

            output_json = json.dumps(output_dict, indent=4)
            if verbose:
                print(output_json, flush=True)

            #producer.send(output_topic, output_json.encode('utf-8'),
                #          json.dumps(output_dict["entityname"]).encode('utf-8'))

        except Exception as e:
            print(e)


def get_latest_stock_prices(verbose=False):
    meta = {"yf_object": yf.Ticker("META"), "entity_name": "meta"}
    ibm = {"yf_object": yf.Ticker("IBM"), "entity_name": "ibm"}
    amazon = {"yf_object": yf.Ticker("AMZN"), "entity_name": "amazon"}
    apple = {"yf_object": yf.Ticker("AAPL"), "entity_name": "apple"}
    general_motors = {"yf_object": yf.Ticker("GM.MX"), "entity_name": "general motors"}
    vanguard_etf = {"yf_object": yf.Ticker("VWCE.DE"), "entity_name": "vanguard etf"}

    list_entities = [meta, ibm, amazon, apple, general_motors, vanguard_etf]

    for entity_dict in list_entities:
        try:
            stats_dict = entity_dict["yf_object"].stats()

            stock_price = stats_dict["price"]["regularMarketPrice"]
            stock_date = stats_dict["price"]["regularMarketTime"]

            output_dict = {"entityname": entity_dict["entity_name"],
                           "currentstockprice": stock_price,
                           "lastupdatedate": stock_date,
                           "key": entity_dict["entity_name"]}

            output_json = json.dumps(output_dict, indent=4)
            if verbose:
                print(output_json)

            producer.send(output_topic, output_json.encode('utf-8'), json.dumps(output_dict["entityname"]).encode('utf-8'))

        except Exception as e:
            print(e)
            pass


while True:
    get_latest_stock_prices(verbose=False)
    time.sleep(2)
