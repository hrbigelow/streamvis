import { 
  Line,
  BufferGeometry,
  Box3,
  Vector3
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

  toggleAxisLog(axisIndex) {
    const attr = this.geometry.getAttribute('position');
    const funName = attr.axes[axisIndex].transform.name;
    const newFunName = { log: 'none', none: 'log' }[funName];
    attr.setTransform(axisIndex, this.transforms[newFunName]);
    this.rescale();
  }

  appendPoints(xdata, ydata) {
    // console.log(`appendPoints...`);
    if (xdata.length !== ydata.length) {
      throw new Error(`xdata.length ${xdata.length} != ydata.length ${ydata.length}`);
    }
    const attr = this.geometry.getAttribute('position');
    attr.append(xdata, 0);
    attr.append(ydata, 1);
    // console.log(`appendPoints: appending ${xdata.length} points, needsDispose: ${attr.needsDispose}, Setting DrawRange to ${attr.count}`);

    if (attr.needsDispose) {
      this.geometry.dispose();
      attr.needsDispose = false;
    }
    // console.dir(attr);
    this.geometry.setDrawRange(0, attr.count);
    this.rescale();
  }

  _getAttributeBox() {
    const attr = this.geometry.getAttribute('position');
    const array = attr.array;
    let minx = Infinity;
    let miny = Infinity;
    let minz = Infinity;
    let maxx = - Infinity;
    let maxy = - Infinity;
    let maxz = - Infinity;
    for (let i = 0; i != array.length; i += 3) {
      if (! (Number.isFinite(array[i]) && Number.isFinite(array[i+1]) && Number.isFinite(array[i+2]))) {
        continue;
      }
      minx = Math.min(minx, array[i]);
      miny = Math.min(miny, array[i+1]);
      minz = Math.min(minz, array[i+2]);
      maxx = Math.max(maxx, array[i]);
      maxy = Math.max(maxy, array[i+1]);
      maxz = Math.max(maxz, array[i+2]);
    }
    return new Box3(new Vector3(minx, miny, minz), new Vector3(maxx, maxy, maxz));
  }

  rescale() {
    const bbox = this._getAttributeBox()
    const size = new Vector3();
    bbox.getSize(size);
    this.scale.x = 1 / size.x;
    this.scale.y = 1 / size.y;
    this.updateMatrix();
    // console.log('rescale: ');
    // console.dir(this.scale);
  }


  getBoundingBox() {
    const abox = this._getAttributeBox();
    return abox.applyMatrix4(this.matrix);
  }


  dispose() {
    this.geometry.dispose();
  }

}


export {
  LinePlot2D
};

