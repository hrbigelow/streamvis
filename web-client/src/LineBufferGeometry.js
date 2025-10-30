import { BufferGeometry } from 'three'; 
import { ResizableBufferAttribute } from './ResizableBufferAttribute.js';

/*
 * LineBufferGeometry represents a line in 2D space.  It can also change its shape so
 * that x and/or y coordinates are log transformed or not.
 * 
*/
class LineBufferGeometry extends BufferGeometry {
  constructor() {
    super();
    this.type = 'LineBufferGeometry';
    this.xAxisLogMode = false;
    this.yAxisLogMode = false;
    this._baseAttribute = new ResizableBufferAttribute(new Float32Array(0), 3);
    this.setAttribute('position', this._baseAttribute);
  }

  /*
   * sets x and y axes modes
   * @param {bool} xlog - whether to set the x axis to log mode
   * @param {bool} ylog - whether to set the y axis to log mode
  */
  setAxisMode(xlog, ylog) {
    if (xlog == this.xAxisLogMode && ylog == this.yAxisLogMode) {
      return;
    }
    const array = new Float32Array(this._baseAttribute.capacity);
    for (let i = 0; i != this._baseAttribute.length; i++) {
    }
    const newAttribute = ResizableBufferAttribute(new Float32Array(capacity), 3);

  }
}


