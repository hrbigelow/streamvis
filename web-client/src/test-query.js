import { getClient } from "./funcs.js";
import { inspect } from "util";

async function main() {
  const client = getClient("http://localhost:8081") 
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
    // console.log(msg)
    if (msg.value.case == "data") {
      const datum = msg.value.value;
      console.log(datum.axes)
      for (const values of datum.axes) {
        console.log(values.data.value.value);
        console.log("result of inspect: ", values.data.value.value.constructor.name);
      }
    }
  }
}
void main();


