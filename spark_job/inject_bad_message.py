"""
Injecteur de messages corrompus, pour tester la Dead Letter Queue.
Envoie directement dans 'sensor-readings' des octets qui NE respectent PAS
le format Avro attendu. Le job Spark doit les rediriger vers 'dead-letter'
sans crasher.
"""
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Trois messages volontairement invalides :
bad_messages = [
    b"ceci n'est pas de l'avro",              # texte brut, aucun sens en Avro
    b"\x00\x00\x00\x00\x01garbage\xff\xfe",   # faux prefixe Confluent + octets pourris
    b"{'json': 'pas avro non plus'}",         # du JSON, pas de l'Avro binaire
]

print(f"Injection de {len(bad_messages)} messages corrompus dans '{TOPIC}'...")

for i, bad in enumerate(bad_messages):
    producer.produce(
        topic=TOPIC,
        key="machine-BAD",       # cle reconnaissable pour les repérer
        value=bad,
    )
    print(f"  Message corrompu #{i+1} envoye ({len(bad)} octets)")

producer.flush()
print("Termine. Ces messages doivent atterrir dans 'dead-letter'.")