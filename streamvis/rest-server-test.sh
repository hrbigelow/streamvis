# test the init endpoint
set -x
curl -s -X POST http://localhost:8080/init/diffusion -d '{ "mu": [ "x1", "x2", "x3" ] }'
curl -s http://localhost:8080/init/diffusion | jq
curl -s -X POST http://localhost:8080/clear/diffusion

# test the update endpoint
curl -s -X POST http://localhost:8080/update/diffusion/3/plot -d '{"x": 5, "y": { "z": [] } }'
curl -s -X POST http://localhost:8080/update/diffusion/5/plot -d '{"x": 5, "y": 8}'
curl -s -X POST http://localhost:8080/update/diffusion/3/graph -d '{"a": 5, "b": 8}'
curl -s http://localhost:8080/update/diffusion/0 | jq
curl -s http://localhost:8080/update/diffusion/3 | jq
curl -s http://localhost:8080/update/diffusion/5 | jq

curl -s -X POST http://localhost:8080/clear/diffusion
