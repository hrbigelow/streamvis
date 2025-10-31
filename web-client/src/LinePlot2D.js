import { 
  Line,
  BufferGeometry
} from 'three';
import { PlotBufferAttribute } from './PlotBufferAttribute';

const transforms = {
  xlog: function(x, y) {
    return [Math.log(x), y];
  },

  ylog: function(x, y) {
    return [x, Math.log(y)];
  },

  xylog: function(x, y) {
    return [Math.log(x), Math.log(y)];
  }
}

/*
 * An object for plotting 2D lines which can toggle xlog
*/
class LinePlot2D extends Line {

  constructor(material = new LineBasicMaterial({ color: 0xff0000 })) {
    super(new BufferGeometry(), material);
    const attr = new PlotBufferAttribute(new Float32Array(0), 3);
    for (const [name, fn] of Object.entries(attr)) {
      attr.addTransform(name, fn);
    }
    this.geometry.setAttribute('position', attr);
  }

  get xLogMode() {
    const attr = this.geometry.getAttribute('position');
    return ['xlog', 'xylog'].includes(attr.activeTransformName);
  }

  get yLogMode() {
    const attr = this.geometry.getAttribute('position');
    return ['ylog', 'xylog'].includes(attr.activeTransformName);
  }

  toggleXAxisMode() {
    const attr = this.geometry.getAttribute('position');
    const mode = attr.activeTransformName;
    const newMode = { xlog: 'none', ylog: 'xylog', xylog: 'xlog', none: 'xlog' }[mode];
    if (newMode === 'none') {
      attr.unsetTransform();
    } else {
      attr.setTransform(newMode);
    }
  }

  toggleYAxisMode() {
    const attr = this.geometry.getAttribute('position');
    const mode = attr.activeTransformName;
    const newMode = { xlog: 'xylog', ylog: 'none', xylog: 'xlog', none: 'ylog' }[mode];
    if (newMode === 'none') {
      attr.unsetTransform();
    } else {
      attr.setTransform(newMode);
    }
  }

  appendPoints(points) {
    if (points.length % 3 !== 0) {
      throw new Error(`points.length={points.length} not divisible by 3`);
    }
    const attr = this.geometry.getAttribute('position');
    attr.set(points, attr.size);
    if (attr.needsDispose) {
      this.geometry.dispose();
      attr.updateArrayRef();
    }
    this.geometry.setDrawRange(0, attr.count);
  }

    


  /*
   * set the transform name to 
  */
  setTransform(name) {
    debugger;
    const attr = this.geometry.getAttribute('position');
    if (! name in attr.transforms) {
      throw new Error(`transform name ${name} not among the available transforms`);
    }
    attr.setTransform(name);
  }

  dispose() {
    this.geometry.dispose();
  }

}


export {
  LinePlot2D
};

