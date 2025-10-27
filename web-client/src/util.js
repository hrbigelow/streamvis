import { Service, DType } from '../streamvis/v1/data_pb.js';  
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

/**
 * create a gRPC client
 * @param {string} host - string with host:port for gRPC server
 */
export function getServiceClient(url) {
  const transport = createConnectTransport({
    baseUrl: url,
    httpVersion: "1.1"
  });
  return createClient(Service, transport);
}


/**
 * extractData from the data object according to the name schema
 * @param {streamvis.v1.Name} name - the name object describing the data
 * @param {streamvis.v1.Data} data - the data object holding the numerical data
 * @param {list} axes - a list of strings holding the axes to extract
 * returns: Float32Array with data points flattened [axis1[0], axis2[0], ..., axis1[i], axis2[i], ...] 
*/
export function extractData(name, data, axes) {
  const isLE = new Uint8Array(new Uint32Array([0x01020304]).buffer)[0] === 0x04;
  if (! isLE) {
    throw new Error(`Only supported on little-endian systems`);
  }
  const outIndexes = Object.fromEntries(axes.map((ent, idx) => [ent, idx])); 
  const sources = new Array(axes.length);
  for (let [fieldIndex, field] of name.fields.entries()) {
    if (field.name in outIndexes) {
      const outIndex = outIndexes[field.name];
      const axis = data.axes[fieldIndex];
      switch (axis.dtype) {
        case DType.UNSPECIFIED: {
          throw new Error(`Unspecified DType received for axis ${field.name}`);
        }
        case DType.F32: {
          sources[outIndex] = new Float32Array(axis.data.buffer);
          break;
        }
        case DType.I32: {
          sources[outIndex] = new Int32Array(axis.data.buffer);
          break;
        }
        default: {
          throw new Error(`Unknown DType for axis ${field.name}`);
        }
      }
    }
  }
  if (sources.some(v => v === undefined)) {
    throw new Error(`Missing one or more axes in data`);
  }
  const lengths = sources.map(ary => ary.length);
  if (lengths.some(v => v !== lengths[0])) {
    throw new Error(`Axes have different lengths: ${lengths}`);
  }
  const out = new Float32Array(sources.length * lengths[0]);

  // TODO: optimize
  for (let s = 0; s < sources.length; s++) {
    let source = sources[s];
    for (let i = 0, j = s; i != source.length; i++, j+= sources.length) {
      out[j] = source[i];
    }
  }
  return out;
}


