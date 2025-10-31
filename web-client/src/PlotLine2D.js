import { Line } from 'three';
import { PlotBufferAttribute } from './PlotBufferAttribute.js';

class PlotLine2D extends Line {
  constructor(material = new LineBasicMaterial({ color: 0xff0000 })) {
    super(new BufferGeometry(), material);
    const array = new Float32Array(0);
    self.geometry.setAttribute('position', new PlotbufferAttribute(array, 3));
  }

}



