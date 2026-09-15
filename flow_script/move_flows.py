import argparse
import sys
import yaml
import requests
from kafka import KafkaConsumer


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def get_kafka_topics(bootstrap_servers):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    topics = consumer.topics()
    consumer.close()
    return topics


def move_flows(cfg, flows, source, dest):
    url = cfg["url"].rstrip("/") + "/move"
    headers = {"Authorization": f"Bearer {cfg['token']}"}
    payload = {"flows": flows, "source": source, "dest_cluster": dest}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()


def whitelist(cfg, dest):
    url = cfg["url"].rstrip("/") + "/whitelist"
    headers = {"Authorization": f"Bearer {cfg['token']}"}
    resp = requests.post(url, json={"dest_cluster": dest}, headers=headers)
    resp.raise_for_status()
    return resp.json()


def blacklist(cfg, source):
    url = cfg["url"].rstrip("/") + "/blacklist"
    headers = {"Authorization": f"Bearer {cfg['token']}"}
    resp = requests.post(url, json={"source": source}, headers=headers)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Move flows between clusters")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--flows", nargs="+")
    parser.add_argument("--source")
    parser.add_argument("--dest")
    args = parser.parse_args()

    config = load_config(args.config)

    flows = args.flows or config.get("flows")
    source = args.source or config.get("source")
    dest = args.dest or config.get("dest")

    if not flows or not source or not dest:
        sys.exit("flows, source and dest are required (via args or config)")

    print(f"Connecting to kafka: {config['kafka']['bootstrap_servers']}")
    topics = get_kafka_topics(config["kafka"]["bootstrap_servers"])
    print(f"Found {len(topics)} topics")

    print(f"Moving flows {flows} from {source} to {dest}")
    move_flows(config["laas_api"], flows, source, dest)

    print(f"Whitelisting {dest} on streamer_api")
    whitelist(config["streamer_api"], dest)

    print(f"Blacklisting {source} on streamer_api")
    blacklist(config["streamer_api"], source)

    print("Done")


if __name__ == "__main__":
    main()
