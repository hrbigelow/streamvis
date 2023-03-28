# test the init endpoint
echo POST ycols
curl -s -X POST http://localhost:8080/init/ycols -H 'Content-Type: application/json' -d '{ "mu": [ "x1", "x2", "x3" ] }'
curl -s http://localhost:8080/init/ycols | jq
curl -s -X POST http://localhost:8080/step/clear

# test the step endpoint
curl -s -X POST http://localhost:8080/step/3 -H 'Content-Type: application/json' -d '{"x": 5, "y": { "z": [] } }'
curl -s -X POST http://localhost:8080/step/5 -H 'Content-Type: application/json' -d '{"x": 5, "y": 8}'
curl -s http://localhost:8080/step/0 | jq
curl -s http://localhost:8080/step/3 | jq
curl -s http://localhost:8080/step/5 | jq

