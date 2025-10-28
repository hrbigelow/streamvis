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


/**
 * extractData from the data object according to the name schema
 * @param {streamvis.v1.Name} name - the name object describing the data
 * @param {streamvis.v1.Data} data - the data object holding the numerical data
 * @param {list} axes - a list of strings holding the axes to extract
 * returns: Float32Array with data points flattened [axis1[0], axis2[0], ..., axis1[i], axis2[i], ...] 
*/
function extractData(name, data, axes) {
  const isLE = new Uint8Array(new Uint32Array([0x01020304]).buffer)[0] === 0x04;
  if (! isLE) {
    throw new Error(`Only supported on little-endian systems`);
  }
  const outIndexes = Object.fromEntries(axes.map((ent, idx) => [ent, idx])); 
  const sources = new Array(3);
  let itemCount;

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
      if (itemCount === undefined) {
        itemCount = sources[outIndex].length;
      } else {
        if (itemCount != sources[outIndex].length) {
          throw new Error(`Axes have different lengths: ${itemCount} vs ${sources[outIndex].length}`);
        }
      }
    }
  }

  for (let s = 0; s != 3; s++) {
    if (sources[s] === undefined) {
      sources[s] = new Float32Array(itemCount).fill(0.0);
    }
  }

  const out = new Float32Array(itemCount * 3);

  for (let i = 0, j = 0; i != itemCount; i++, j+=3) {
    out[j] = sources[0][i];
    out[j+1] = sources[1][i];
    out[j+2] = sources[2][i];
  }
  return out;
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
  extractData,
  resizeToWindow,
  resizeToCanvas,
};


