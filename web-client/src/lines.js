import {Deck, OrthographicView} from '@deck.gl/core';
import {PathLayer} from '@deck.gl/layers';
import {Buffer} from '@luma.gl/core';

import {getClient} from './funcs.js';

const client = getClient("http://localhost:8080")

const deck = new Deck({
  canvas: 'lines-canvas',
  views: new OrthographicView(),
  initialViewState: { },
  controller: true,
  onDeviceInitialized
});


function onDeviceInitialized(device) {
  const buffer = device.createBuffer(new ArrayBuffer(100));
  const positions = {buffer, type: 'float32', size: 2, offset: 0, stride: 8};
  
  const layers = [
    new PathLayer({
      id: 'paths',
      data: {
        length: 0,
        startIndices: [0],
        attributes: {
          getPath: positions, 
        }
      },
      _pathType: 'open' // skip normalization
    })
  ]
  deck.setProps({layers})
}
