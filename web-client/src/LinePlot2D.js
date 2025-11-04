import { 
  Line,
  BufferGeometry
} from 'three';

import { 
  PlotBufferAttribute, 
  PointwiseTransform 
} from './PlotBufferAttribute.js';

/*
 * An object for plotting 2D lines which can toggle xlog or ylog
 *
*/
class LinePlot2D extends Line {

  constructor(material = new LineBasicMaterial({ color: 0xff0000 })) {
    super(new BufferGeometry(), material);
    const attr = new PlotBufferAttribute(3);
    this.geometry.setAttribute('position', attr);
    this.transforms = {
      log: new PointwiseTransform('log', Math.log),
      none: new PointwiseTransform('none', undefined),
    };
  }

  _toggleAxisLog(axisIndex) {
    const attr = this.geometry.getAttribute('position');
    const funName = attr.axes[axisIndex].transform.name;
    const newFunName = { log: 'none', none: 'log' }[funName];
    attr.setTransform(axisIndex, this.transforms[newFunName]);
  }

  toggleXAxisLog() {
    this._toggleAxisLog(0);
  }

  toggleYAxisLog() {
    this._toggleAxisLog(1);
  }

  appendPoints(xdata, ydata) {
    // console.log(`appendPoints...`);
    if (xdata.length !== ydata.length) {
      throw new Error(`xdata.length ${xdata.length} != ydata.length ${ydata.length}`);
    }
    const attr = this.geometry.getAttribute('position');
    debugger;
    attr.append(xdata, 0);
    attr.append(ydata, 1);

    if (attr.needsDispose) {
      this.geometry.dispose();
      attr.needsDispose = false;
    }
    this.geometry.setDrawRange(0, attr.count);
  }

  dispose() {
    this.geometry.dispose();
  }

}


export {
  LinePlot2D
};

