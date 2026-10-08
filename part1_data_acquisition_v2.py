"""
Part 1 (v2): Real-Time Environmental Data Acquisition and Sensor Simulation Framework
New in v2:
  - CSV logging of every reading (dataset for the ML module)
  - Offline buffer: if MQTT is disconnected, readings are queued and sent after reconnection
  - Auto-reconnect using connect_async + loop_start
"""
import csv
import json
import math
import os
import random
import time
from collections import deque
from datetime import datetime

import paho.mqtt.client as mqtt
import requests

# ---------------- CONFIG ----------------
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
TOPIC_BASE = "ievm"            # topic = ievm/<node_id>/data
PUBLISH_INTERVAL = 5           # seconds

USE_LIVE_API = True          # set True after adding API key
OWM_API_KEY = "jawahar1411"

CSV_FILE = "environment_log.csv"
BUFFER_MAX = 10000             # max readings kept while offline

NODES = {
    "school":     {"lat": 12.9692, "lon": 79.1559, "base_aqi": 70},
    "hospital":   {"lat": 12.9165, "lon": 79.1325, "base_aqi": 55},
    "industrial": {"lat": 12.9716, "lon": 79.1600, "base_aqi": 140},
}

LIMITS = {
    "temp": (-10, 60),
    "humidity": (0, 100),
    "aqi": (0, 500),
    "gas": (0, 1000),
    "pm25": (0, 500),
}

CSV_FIELDS = ["timestamp", "node_id", "lat", "lon", "temp", "humidity",
              "aqi", "gas", "pm25", "quality_flags"]

offline_buffer = deque(maxlen=BUFFER_MAX)


# ---------------- DATA SOURCES ----------------
def simulate_reading(node_id, cfg):
    """Generate realistic values with daily cycle, noise and random spikes."""
    now = datetime.now()
    hour = now.hour + now.minute / 60
    daily = math.sin((hour - 8) / 24 * 2 * math.pi)
    temp = 30 + 4 * daily + random.gauss(0, 0.4)
    humidity = 65 - 12 * daily + random.gauss(0, 1.5)
    aqi = cfg["base_aqi"] + 15 * math.sin((hour - 7) / 12 * math.pi) + random.gauss(0, 5)
    if random.random() < 0.03:
        aqi += random.uniform(50, 120)
    gas = max(0, aqi * 0.8 + random.gauss(0, 8))
    pm25 = max(0, aqi * 0.55 + random.gauss(0, 4))
    return {"temp": temp, "humidity": humidity, "aqi": aqi, "gas": gas, "pm25": pm25}


def fetch_live(cfg):
    """Fetch live pollution data from OpenWeatherMap Air Pollution API."""
    url = "http://api.openweathermap.org/data/2.5/air_pollution"
    r = requests.get(url, params={"lat": cfg["lat"], "lon": cfg["lon"],
                                  "appid": OWM_API_KEY}, timeout=10)
    r.raise_for_status()
    item = r.json()["list"][0]
    comp = item["components"]
    return {
        "temp": 30 + random.gauss(0, 0.5),
        "humidity": 65 + random.gauss(0, 2),
        "aqi": item["main"]["aqi"] * 60,
        "gas": comp.get("co", 0) / 10,
        "pm25": comp.get("pm2_5", 0),
    }


# ---------------- VALIDATION ----------------
def validate(reading):
    """Clean and validate a reading. Returns (clean_reading, issues)."""
    issues = []
    clean = {}
    for key, (lo, hi) in LIMITS.items():
        val = reading.get(key)
        if val is None or (isinstance(val, float) and math.isnan(val)):
            issues.append(f"{key} missing")
            continue
        if not lo <= val <= hi:
            issues.append(f"{key} out of range ({val:.1f})")
            val = min(max(val, lo), hi)
        clean[key] = round(val, 1)
    return clean, issues


# ---------------- CSV LOGGING ----------------
def log_to_csv(payload):
    new_file = not os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        row = {k: payload.get(k, "") for k in CSV_FIELDS}
        row["quality_flags"] = ";".join(payload.get("quality_flags", []))
        writer.writerow(row)


# ---------------- MQTT WITH OFFLINE BUFFER ----------------
def send_or_buffer(client, topic, payload):
    """Publish if connected, otherwise store in the offline buffer."""
    message = json.dumps(payload)
    if client.is_connected():
        info = client.publish(topic, message)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            return True
    offline_buffer.append((topic, message))
    print(f"[OFFLINE] buffered {len(offline_buffer)} message(s)")
    return False


def flush_buffer(client):
    """Send all buffered messages once the connection is back."""
    if not client.is_connected() or not offline_buffer:
        return
    print(f"[ONLINE] sending {len(offline_buffer)} buffered message(s)...")
    while offline_buffer:
        topic, message = offline_buffer[0]
        info = client.publish(topic, message)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            break
        offline_buffer.popleft()


# ---------------- MAIN ----------------
def main():
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    else:
        client = mqtt.Client()

    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_BROKER, MQTT_PORT, 60)   # works even if offline at start
    client.loop_start()
    print("MQTT client started (auto-reconnect on). Press Ctrl+C to stop.")
    print(f"Logging to {CSV_FILE}")

    try:
        while True:
            flush_buffer(client)
            for node_id, cfg in NODES.items():
                try:
                    raw = fetch_live(cfg) if USE_LIVE_API else simulate_reading(node_id, cfg)
                except Exception as e:
                    print(f"[{node_id}] live fetch failed ({e}), using simulation")
                    raw = simulate_reading(node_id, cfg)

                clean, issues = validate(raw)
                payload = {
                    "node_id": node_id,
                    "lat": cfg["lat"],
                    "lon": cfg["lon"],
                    **clean,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "quality_flags": issues,
                }
                log_to_csv(payload)
                topic = f"{TOPIC_BASE}/{node_id}/data"
                sent = send_or_buffer(client, topic, payload)
                print(("SENT " if sent else "QUEUED ") + topic, json.dumps(payload))
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
