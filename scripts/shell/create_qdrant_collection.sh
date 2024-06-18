#!/bin/bash
set -eux

collection_name=$1
vector_size=$2

curl -X PUT "http://localhost:6333/collections/$collection_name" \
     -H "Content-Type: application/json" \
     -d '{
           "vectors": {
                "size": '$vector_size',
                "distance": "Cosine"
           }
         }'
