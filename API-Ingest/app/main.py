from ensurepip import bootstrap
from fastapi import FastAPI, status, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json
from pydantic import BaseModel
from datetime import datetime
from tables import Description
from kafka import KafkaProducer


class Application(BaseModel):
    company_name: str
    company_url: str
    application_date: str

app=FastAPI()

@app.get("/")
async def root():
    return {"message" : "Hello my API Service"}

@app.post("/applications")
async def post_invoice_item(item: Application):
    print("Message received")
    try:

        json_of_item = jsonable_encoder(item)

        json_as_string = json.dumps(json_of_item)

        print(json_as_string)

        produce_kafka_string(json_as_string)

        return JSONResponse(content=json_of_item,status_code=201)

    except:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)

def produce_kafka_string(json_as_string):
    producer=KafkaProducer(bootstrap_servers='localhost:9092',acks=1)

    producer.send('applications',bytes(json_as_string,'utf-8'))
    producer.flush()
