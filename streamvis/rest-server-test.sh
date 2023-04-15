# test the init endpoint
set -x
BASE=http://localhost:8080
RUN=diffusion

curl -s -X POST ${BASE}/${RUN}/cfg/mu -d '{ "kind": "scatter" }'
curl -s -X POST ${BASE}/${RUN}/cfg/sigma -d '{ "kind": "multi_line" }'
curl -s -X GET  ${BASE}/${RUN}/cfg | jq
curl -s -X DELETE ${BASE}/${RUN}/cfg/mu
curl -s -X GET  ${BASE}/${RUN}/cfg | jq

curl -s -X GET  ${BASE}/${RUN}/cfg/sigma | jq

curl -s -X DELETE ${BASE}/${RUN}/cfg

# echo 'Should be empty: '
curl -s -X GET  ${BASE}/${RUN}/cfg | jq

curl -s -X POST ${BASE}/${RUN}/data/mu -d '[ [1,2,3], [4,5,6] ]'
curl -s -X POST ${BASE}/${RUN}/data/mu -d '[ [7,5,3], [2,5,1] ]'
curl -s -X GET ${BASE}/${RUN}/data/mu | jq

curl -s -X DELETE ${BASE}/${RUN}

set +x

