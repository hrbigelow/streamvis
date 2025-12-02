import { 
  Line,
  BufferGeometry,
  Box3,
  Vector3
} from 'three';

import { Line2 } from 'three/examples/jsm/lines/Line2.js';
// import { LineMaterial } from 'three/examples/jsm/lines/LineMaterial.js';
import { Line2NodeMaterial } from 'three/webgpu'; 
import { LineGeometry } from 'three/examples/jsm/lines/LineGeometry.js';


import { 
  PlotBufferAttribute, 
  PointwiseTransform 
} from './PlotBufferAttribute.js';

/*
 * An object for plotting 2D lines which can toggle xlog or ylog
 *
*/
class Line2Plot2D extends Line2 {

  constructor(material) {
    super(new LineGeometry(), material);
    this.positionData = new PlotBufferAttribute(3);
    this.transforms = {
      log: new PointwiseTransform('log', Math.log),
      none: new PointwiseTransform('none', undefined),
    };
  }

  toggleAxisLog(axisIndex) {
    const funName = this.positionData.axes[axisIndex].transform.name;
    const newFunName = { log: 'none', none: 'log' }[funName];
    this.positionData.setTransform(axisIndex, this.transforms[newFunName]);
    this.rescale();
  }

  appendPoints(xdata, ydata) {
    // console.log(`appendPoints...`);
    if (xdata.length !== ydata.length) {
      throw new Error(`xdata.length ${xdata.length} != ydata.length ${ydata.length}`);
    }
    this.positionData.append(xdata, 0);
    this.positionData.append(ydata, 1);
    this.geometry.setPositions(this.positionData.array);

    if (this.positionData.needsDispose) {
      this.geometry.dispose();
      this.positionData.needsDispose = false;
    }
    this.geometry.setDrawRange(0, this.positionData.count);
    this.rescale();
    this.computeLineDistances();
    this.scale.set(1, 1, 1);
  }

  _getAttributeBox() {
    const array = this.positionData.array;
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
  Line2Plot2D
};


