"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass # added


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass   # ADDED
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass #ADDED
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
# topic = app.topic("TODO", value_type=Station)
topic = app.topic("station3", value_type=Station)

# TODO: Define the output Kafka Topic
# out_topic = app.topic("TODO", partitions=1)
out_topic = app.topic("station3-out", partitions=1)

# TODO: Define a Faust Table
table = app.Table(
    # "TODO",
    "station_table", 
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
@app.agent(topic)
async def transform(stations):
    
    
    async for station in stations:
    
        transformed_station = TransformedStation(
            station_id=station.stop_id,
            station_name=station.station_name,
            order=station.order, 
            line=''
        )
    
        if staion.red == True:
            transformed_station.line='red'
        elif station.blue==True:
            transformed_station.line='blue'
        elif station.green==True:
            transfomred_station.line='green'
        print('station:', station)

if __name__ == "__main__":
    app.main()
