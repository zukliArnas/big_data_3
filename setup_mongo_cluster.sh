#!/bin/bash

set -e

echo "➡️  Shutting down existing cluster (if any)..."
docker-compose down -v

echo "🚀 Starting up MongoDB cluster..."
docker-compose up -d

echo "Waiting a few seconds for containers to fully start..."
sleep 5x

echo "Initiating config server replica set..."
docker exec -i big_data_3-configsvr-1 mongosh --port 27019 <<EOF
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [{ _id: 0, host: "configsvr:27019" }]
})
EOF

sleep 3

echo "Initiating shard replica set..."
docker exec -i big_data_3-shard1-1 mongosh --port 27018 <<EOF
rs.initiate({
  _id: "shardReplSet",
  members: [{ _id: 0, host: "shard1:27018" }]
})
EOF

sleep 3

echo "➕ Adding shard to cluster via mongos..."
docker exec -i big_data_3-mongos-1 mongosh <<EOF
sh.addShard("shardReplSet/shard1:27018")
EOF

echo "Verifying cluster setup..."

echo "Checking configReplSet..."
docker exec -i big_data_3-configsvr-1 mongosh --quiet --port 27019 --eval "rs.status().ok" | grep 1 >/dev/null \
  && echo "Config replica set is OK" \
  || echo "Config replica set has issues"

echo "🧩 Checking shardReplSet..."
docker exec -i big_data_3-shard1-1 mongosh --quiet --port 27018 --eval "rs.status().ok" | grep 1 >/dev/null \
  && echo "Shard replica set is OK" \
  || echo "Shard replica set has issues"

echo "🧩 Checking sharding status via mongos..."
docker exec -i big_data_3-mongos-1 mongosh --quiet --eval "sh.status().shards" | grep shardReplSet >/dev/null \
  && echo "Shard successfully added to the cluster" \
  || echo "Shard was NOT added to the cluster"

echo "🎉 All done!"
