import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { Service } from "./streamvis/v1/data_pb.js";


const transport = createConnectTransport({
  baseUrl: "http://localhost:8080",
  httpVersion: "1.1"
});

async function main() {
  const client = createClient(Service, transport);
  const req = {};
  for await (const msg of client.scopes(req)) {
    console.log(msg)
  }
  const nreq = {"scope": "test-scope"}
  for await (const msg of client.names(nreq)) {
    console.log(msg)
  }
  const dreq = {
    "scope_pattern": "test-scope",
    "name_pattern": "sinusoidal",
    "file_offset": 0
  }
  for await (const msg of client.queryData(dreq)) {
    console.log(msg)
  }
}
void main();


