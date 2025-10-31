import {
  BufferAttribute
} from 'three';
import { ResizableArray } from './ResizableArray';


/*
 * A BufferAttribute which allows resizing and custom function application
 *
*/
class PlotBufferAttribute extends BufferAttribute {
  /*
   * @param {TypedArray} array -> The array holding the attribute data.
   * @param {number} itemSize - The item size.
   * @param {boolean} [normalized=false] - Whether the data are normalized or not.
  */
  constructor(array, itemSize, normalized=false) {
    super(array, itemSize, normalized); 
    this._sourceData = new ResizableArray(array);
    this._altData = new ResizableArray(new array.constructor(0));
    this.transforms = {}
    this.activeTransform = undefined
    this.prevArrayRef = undefined
  }

  /*
   * updates the main .array property while keeping track of the previous reference
  */
  updateArrayRef() {
    this.prevArrayRef = this.array;
    if (this.activeTransform !== undefined) {
      this.array = this._altData.array;
    } else {
      this.array = this._sourceData.array;
    }
  }

  // call geometry.dispose() if this is true
  get needsDispose() {
    return this.array !== this.prevArrayRef;
  }

  /*
   * gets the logical size of the data in this attribute
  */
  get size() {
    return this._sourceData.size;
  }

  get activeTransformName() {
    for (const [name, fn] of Object.entries(this.transforms)) {
      if (this.activeTransform === fn) {
        return name;
      }
    }
    return 'none';
  }

  /**
   * register a point transformation function under a name
   * @param {string} name - the name of the transformation
   * @param {function} fun(source, dest, beg, end) - perform in-place modification of dest
  */
  addTransform(name, fun) {
    this.transforms[name] = fun;
  }

  /**
   * sets a previously registered transform function as the one to be applied for the
   */
  setTransform(name) {
    if (! name in this.transforms) {
      throw new Error(`No transform by the name of "${name}"`);
    }
    if (this.activeTransform === this.transforms[name]) {
      return;
    }
    this.activeTransform = this.transforms[name];
    this.syncTransformed(0, this._altData.size);
    this.updateArrayRef();
  }

  unsetTransform() {
    this.activeTransform = undefined;
    this._altData.resize(0, 0);
    this.updateArrayRef();
  }

  /**
   * update the alternate data from source data in the range [beg, end), using the
   * current transform.
   * @param {number} beg - the start of the range
   * @param {number} end - the end of the range (exclusive)
   */
  syncTransformed(beg, end) {
    if (beg < 0 || end < 0 || beg >= end || end > this._sourceData.size) {
      throw new Error(`Bad range: [${beg}, ${end})`);
    }
    if (this.activeTransform === undefined) {
      return;
    }
    this._altData.resize(this._sourceData.capacity);
    const src = this._sourceData.array;
    const trg = this._altData.array;
    for (let i = beg; i != end; i += 3) {
      const [x, y] = this.activeTransform(src[i], src[i+1]);
      trg[i] = x;
      trg[i+1] = y;
    }
    this._altData.size = Math.max(this._altData.size, end);
    this.updateArrayRef();
    if (! this.needsDispose) { 
      this.needsUpdate = true;
      this.addUpdateRange(beg, end - beg);
    }
  }

  /**
   * Sets the given array data in the buffer attribute.
   *
   * @param {TypedArray} value - The array data to set.
   * @param {number} [offset=0] - The offset in this buffer attribute's array.
   * @return {ResizableBufferAttribute} A reference to this instance.
   */
  set(value, offset=0) {
    this._sourceData.set(value, offset);
    this.count = this.size / this.itemSize;
    this.syncTransformed(offset, offset + value.length);
    if (! this.needsDispose) {
      this.needsUpdate = true;
      this.addUpdateRange(offset, value.length);
    }
    return this;
  }

}

export {
  PlotBufferAttribute
};

