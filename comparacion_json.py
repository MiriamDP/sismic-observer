import json
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="sismic-earth",
    user="chicho_terremoto",
    password="lfelmpeut5",
    port=5432
)

cur = conn.cursor()

cur.execute("""
    SELECT id, response_body
    FROM raw_extractions
    WHERE id IN (13, 14)
    ORDER BY id
""")

rows = cur.fetchall()

old = json.loads(rows[0][1])
new = json.loads(rows[1][1])

print("Extracción anterior:", rows[0][0])
print("Extracción nueva:", rows[1][0])

print("Número de eventos anterior:", len(old["features"]))
print("Número de eventos nuevo:", len(new["features"]))

old_ids = {f["id"] for f in old["features"]}
new_ids = {f["id"] for f in new["features"]}

print("Eventos nuevos:", len(new_ids - old_ids))
print("Eventos desaparecidos:", len(old_ids - new_ids))

common = old_ids & new_ids

changed = []

old_map = {f["id"]: f for f in old["features"]}
new_map = {f["id"]: f for f in new["features"]}

for event_id in common:
    if old_map[event_id] != new_map[event_id]:
        changed.append(event_id)

print("Eventos modificados:", len(changed))
print("IDs modificados:", changed[:20])

cur.close()
conn.close()

print("\n--- COMPARACIÓN DE METADATA ---")

old_metadata = old.get("metadata", {})
new_metadata = new.get("metadata", {})

for key in set(old_metadata) | set(new_metadata):

    old_value = old_metadata.get(key)
    new_value = new_metadata.get(key)

    if old_value != new_value:
        print(f"\nCampo diferente: {key}")
        print("Anterior:", old_value)
        print("Nuevo:", new_value)