import { Service, DType } from '../streamvis/v1/data_pb.js';  
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

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

function optParseInt(value, prev) {
  return parseInt(value);
}

/**
 * Converts data returned from the service client, converting fields based on their
 * declared type, and extracting just the named axes.
 * @param {streamvis.v1.Name} name - the pb.Name object returned by the gRPC service 
 * @param {streamvis.v1.Data} data - the pb.Data object returned by the gRPC service 
 * @param {array of string} axes - the names of the axes to retrieve
 * @returns object with keys the axes, values TypedArray of appropriate subtype
*/
function getAxes(name, data, axes) {
  const isLE = new Uint8Array(new Uint32Array([0x01020304]).buffer)[0] === 0x04;
  if (! isLE) {
    throw new Error(`Only supported on little-endian systems`);
  }
  const sources = Object.fromEntries(Object.values(axes).map(k => [k, undefined])); 
  for (const [fieldIndex, field] of name.fields.entries()) {
    if (! field.name in sources) {
      continue;
    }
    const axis = data.axes[fieldIndex];
    switch (axis.dtype) {
      case DType.UNSPECIFIED: {
        throw new Error(`Unspecified DType received for axis ${field.name}`);
      }
      case DType.F32: {
        sources[field.name] = new Float32Array(axis.data.buffer);
        break;
      }
      case DType.I32: {
        sources[field.name] = new Int32Array(axis.data.buffer);
        break;
      }
      default: {
        throw new Error(`Unknown DType for axis ${field.name}`);
      }
    }
  }
  return sources;
}


// call if window changes size and canvas is full window size
function resizeToWindow(window, renderer, camera) {
  const { innerWidth: w, innerHeight: h } = window;
  const canvas = renderer.domElement;
  renderer.setSize(w, h, true); // true needed here to actually set canvas size
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
  // console.log(`resized to ${w} x ${h}`);
  // console.log(`canvas: ${canvas.clientWidth} x ${canvas.clientHeight}`);
}

// call if canvas size was changed 
function resizeToCanvas(renderer) {
  const canvas = renderer.domElement;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const needsResize = canvas.width !== width || canvas.height !== canvas.height;
  if (needsResize) {
    renderer.setSize(width, height, );
  }
  return needsResize;
}

export {
  getServiceClient,
  getAxes,
  resizeToWindow,
  resizeToCanvas,
  optParseInt,
};


