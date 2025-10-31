import { Line } from 'three';
import { PlotBufferAttribute } from './PlotBufferAttribute.js';

const transforms {
  'xlog': function(x, y) {
    return [Math.log(x), y];
  },

  'ylog': function(x, y) {
    return [x, Math.log(y)];
  }

  'xylog': function(x, y) {
    return [Math.log(x), Math.log(y)];
  }
}

/*
 * An object for plotting 2D lines which can toggle xlog
*/
class PlotLine2D extends Line {
  constructor(material = new LineBasicMaterial({ color: 0xff0000 })) {
    super(new BufferGeometry(), material);
    const attr = new PlotBufferAttribute(new Float32Array(0), 3);
    for (const [name, fn] of Object.entries(attr)) {
      attr.addTransform(name, fn);
    }
    this.geometry.setAttribute('position', attr);
  }

  appendPoints(points) {
    if (points.length % 3 !== 0) {
      throw new Error(`points.length={points.length} not divisible by 3`);
    }
    const attr = this.geometry.getAttribute('position');
    attr.set(points, attr.size);
    if (attr.needsDispose) {
      this.geometry.dispose();
      attr.needsDispose = false;
    }
    this.geometry.setDrawRange(0, attr.size / 3);
  }

  

  dispose() {
    this.geometry.dispose();
  }

}



