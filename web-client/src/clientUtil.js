import { Service } from './gen/streamvis/v1/data_pb.js';  
import { createConnectTransport } from "@connectrpc/connect-web";
import { createClient } from "@connectrpc/connect";

/**
 * create a gRPC client
 * @param {string} host - string with host:port for gRPC server
 */
function getServiceClient(url) {
  const transport = createConnectTransport({
    baseUrl: url,
    httpVersion: "1.1"
  });
  return createClient(Service, transport);
}

export {
  getServiceClient,
};


