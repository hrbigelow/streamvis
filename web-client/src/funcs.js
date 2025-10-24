import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { Service } from "../streamvis/v1/data_pb.js";


export function getClient(url) {
  const transport = createConnectTransport({
    baseUrl: url,
    httpVersion: "1.1"
  });
  return createClient(Service, transport);
}

