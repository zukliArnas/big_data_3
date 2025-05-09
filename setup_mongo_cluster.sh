#!/bin/bash

set -e

echo "Shutting down existing cluster (if any)..."
docker-compose down -v

echo "Starting up MongoDB cluster..."
docker-compose up -d

echo "Waiting a few seconds for containers to fully start..."
sleep 5

echo "Initiating config server replica set..."
docker exec -i configsvr mongosh --port 27019 <<EOF
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [{ _id: 0, host: "configsvr:27019" }]
})
EOF

sleep 5

echo "Initiating shard replica set..."
docker exec -i shard1-1 mongosh --port 27018 <<EOF
rs.initiate({
  _id: "shardReplSet",
  members: [
    { _id: 0, host: "shard1-1:27018" },
    { _id: 1, host: "shard1-2:27020" },
    { _id: 2, host: "shard-arbiter:27021", arbiterOnly: true }
  ]
})
EOF

sleep 5

echo "Adding shard to the cluster via mongos..."
docker exec -i mongos mongosh --port 27017 <<EOF
sh.addShard("shardReplSet/shard1-1:27018,shard1-2:27020")
EOF

echo "Verifying cluster setup..."

docker exec -i configsvr mongosh --quiet --port 27019 --eval "rs.status().ok" | grep 1 >/dev/null \
  && echo "Config replica set is OK" \
  || echo "!!! Config replica set has issues"

docker exec -i shard1-1 mongosh --quiet --port 27018 --eval "rs.status().ok" | grep 1 >/dev/null \
  && echo "Shard replica set is OK" \
  || echo "!!! Shard replica set has issues"

docker exec -i mongos mongosh --quiet --port 27017 --eval "sh.status().shards" | grep shardReplSet >/dev/null \
  && echo "Shard successfully added to the cluster" \
  || echo "!!! Shard was NOT added to the cluster"

echo "Enabling sharding on the 'ais' database..."
docker exec -i mongos mongosh --port 27017 <<EOF
sh.enableSharding("ais")
EOF

sleep 2

echo "Creating hashed index on 'ais.test._id'..."
docker exec -i mongos mongosh --port 27017 <<EOF
use ais
db.test.createIndex({ _id: "hashed" })
EOF

echo "Sharding the 'ais.test' collection using hashed _id..."
docker exec -i mongos mongosh --port 27017 <<EOF
sh.shardCollection("ais.test", { _id: "hashed" })
EOF

echo "Done!"
