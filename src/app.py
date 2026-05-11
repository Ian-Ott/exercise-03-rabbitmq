from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate
import pika
import os
import time
import json

Base.metadata.create_all(bind=engine)
app = FastAPI()
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}

@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    publicarEvento("register",node.name)
    return db_node

@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()

@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node

@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    publicarEvento("delete", name)
    return Response(status_code=204)

# TODO: After each POST /api/nodes (register) and DELETE /api/nodes/{name},
# publish an event to RabbitMQ with this format:
# {"event": "node_registered" or "node_deleted", "node_name": "<name>", "timestamp": "<ISO8601>"}
#
# Use pika to connect to RabbitMQ at RABBITMQ_URL env var.
# Queue name: "node_events"
def publicarEvento(evento, nombre):
    connection = conectar_con_reintento()
    try:
        channel = connection.channel()

        # Declarar cola durable
        channel.queue_declare(queue='node_events', durable=True)
        if(evento == "register"):
            mensaje = {
                "event": "node_registered",
                "node_name": nombre,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        elif(evento == "delete"):
            mensaje = {
                "event": "node_deleted",
                "node_name": nombre,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            return
        channel.basic_publish(
            exchange='',
            routing_key='node_events',
            body=json.dumps(mensaje).encode(),
            properties=pika.BasicProperties(
            delivery_mode=2
            )
        )
    finally:
        connection.close()

def conectar_con_reintento():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(RABBITMQ_URL)
            )
            return connection
        except Exception as e:
            time.sleep(3)
