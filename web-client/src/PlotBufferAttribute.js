import {
  BufferAttribute
} from 'three';

import { ResizableArray } from './ResizableArray.js';


/*
 * Uses welford algorithm https://www.jstor.org/stable/1266577 to update:
 * m_n := n^{-1} sum^n(x_i)
 * s_n := sum((x_i - m_n)^2)
 *
 * @param {array} values - the values to incorporate into the running statistics
 * @param {number} n - current count
 * @param {number} m - current mean value
 * @param {number} s - current s value
 *
 * By definition, the starting values for n, m, and s are all zero.  (before seeing
 * any values)
 *
*/
class RunningStats {
  constructor() {
    this.count = 0;
    this.m = 0;
    this.s = 0;
  }

  update(values) {
    let mprev;
    for (let v of values) {
      if (!Number.isFinite(v)) continue;
      this.count++;
      mprev = this.m;
      this.m += (v - this.m) / this.count;
      this.s += (v - mprev) * (v - this.m); 
    }
  }

  stats() { 
    return { 
      mean: this.m, 
      variance: this.s / this.count,
      stddev: Math.sqrt(this.s / this.count),
    };
  }

  clear() {
    this.count = 0;
    this.s = 0;
    this.m = 0;
  }
}


function computeStats(data) {
  const get = (el, other) => Number.isFinite(el) ? el : other;
  const sum = data.reduce((acc, el) => acc + get(el, 0), 0);
  const mean = sum / data.length;
  const sq = data.reduce((acc, el) => acc + (get(el, mean) - mean) ** 2, 0);
  const variance = sq / data.length;
  const stddev = Math.sqrt(variance);
  return { mean, stddev };
}


function validStats({ mean, stddev }) {
  return mean - stddev > -3 && mean + stddev < 3;
}

/**
 * A class for representing transforms 
*/
class PointwiseTransform {
  constructor(name, fun) {
    this.name = name;
    this.fun = fun;
  }

  // apply the function inplace
  call(data) {
    for (let i = 0; i != data.length; i++) {
      data[i] = this.fun(data[i]);
    }
  }
}


class Axis {
  constructor(axisIndex, ctor) {
    this.index = axisIndex;
    this.array = new ResizableArray(new ctor(0)); 
    this.targetStats = new RunningStats(); // stats on transformed data 
    this.transform = new PointwiseTransform('none', undefined);
    this.currentWhitening = undefined; // currently used whitening parameters
    this._offset = 0; // offset into source array to read from
    this._targetOffset = 0; // offset into target array to resume writing
  }

  get arrayType() {
    return Object.getPrototypeOf(this.array.array);
  }

  // append ground-truth data to this axis
  append(data) {
    return this.array.append(data);
  }

  _preprocess() {
    let data = new Float32Array(this.array.subarray(this._offset, this.array.size));
    if (this.transform.fun !== undefined) {
      this.transform.call(data);
    }
    /*
    if (this.currentWhitening === undefined) {
      this.currentWhitening = computeStats(data);
    }
    const { mean, stddev } = this.currentWhitening;
    const stddevInv = stddev ** -1
    for (let i = 0; i != data.length; i++) {
      data[i] = (data[i] - mean) * stddevInv
    }
    */
    return data;
  }

  /**
   * called when all target data needs to be re-computed
  */
  resetForTarget() {
    this.targetStats.clear();
    this._offset = 0;
    this._targetOffset = 0;
    this.currentWhitening = undefined;
  }

  /**
   * send new ground-truth data range [this._offset, this.size) to the target array
   * starting at this._targetOffset. The target array is external to this axis.
   * This method will call target.resize to ensure sufficient space. 
   *
   * @param {ResizableArray} target - the interleaved target
   * @return {object} - fields (realloc, beg, end) indicating whether target array
   * was realloced, and the [beg, end) vertex range of the array that was affected 
  */
  send(target) {
    // console.log(`Sending axis ${this.index}...`);
    // console.log(`Before: `);
    // console.dir(this);
    // console.dir(target.array.slice(0, 10));
    let data = this._preprocess();
    // this.targetStats.update(data);

    /*
    if (! validStats(this.targetStats.stats())) {
      this.resetForTarget();
      data = this._preprocess();
    }
    */
    target.size = this._targetOffset;
    const count = data.length * 3;
    const targetEnd = this._targetOffset + count;
    const info = { realloc: false, beg: this._targetOffset, count: count};
    info.realloc = target.reserveAdditional(count);

    for (let i = 0, j = this._targetOffset + this.index; i < data.length; i++, j+=3) {
      target.setAt(j, data[i]);
    }
    target.size = targetEnd;
    this._offset = this.array.size;
    this._targetOffset = targetEnd;
    return info;
  }

}


/*
 * PlotBufferAttribute supports adding / removing data and will manage reallocation.
 * It also supports registering and setting custom transforms on the data.
 * The ground-truth source data is stored in .sourceData.  No transformations are
 * done on it - in particular, the data type (int32, float32) is preserved as well.
 * Any registered transformation functions must take in .sourceData and compute
 * intercalated output in Float32Array form.
 *
 * For maximizing precision, the transformed data should be scaled independently for
 * x and y axes so that all of the intercalated data are roughly in the same dynamic
 * range.  This implies that as new source data accumulates and thus the dynamic
 * range changes, the existing transform may need to be renormalized.
 * 
 *
*/
class PlotBufferAttribute extends BufferAttribute {
  /*
   * @param {TypedArray} array -> The array holding the attribute data.
   * @param {number} ndim - the number of dimensions (2 or 3) being tracked 
   * @param {boolean} [normalized=false] - Whether the data are normalized or not.
  */
  constructor(ndim) {
    console.log(`Constructing new PlotBufferAttribute`);
    if (! ndim in [2, 3]) {
      throw new Error(`ndim must be 2 or 3.  Got ndim=${ndim}`);
    }
    const dummy = new Float32Array(0); // keep the super class happy
    super(dummy, 3, false); 
    this.axes = new Array(ndim); // initialized on first data
    // this._zvalue = 0 // TODO: deal with z axis
    this._array = new ResizableArray(new dummy.constructor(0));

    // this flag is set to true whenever the underlying array has been reallocated.
    this.needsDispose = false;
  }

  /*
   * gets the logical size of the data in this attribute
  */
  get size() {
    return this.array.size;
  }

  /**
   * sets a previously registered transform function as the one to be applied for the
   * @param {number} axisIndex - which axis to set the transform on 
   * @param {PointwiseTransform} fun - the function to set
   */
  setTransform(axisIndex, fun) {
    const axis = this._getAxis(axisIndex);
    if (axis.transform === fun) {
      return; // nothing to do
    } 
    axis.transform = fun;
    axis.resetForTarget();
    this._synch(axisIndex);
  }

  _getAxis(axisIndex) {
    if (axisIndex < 0 || axisIndex >= this.axes.length) {
      throw new Error(`axis ${axisIndex} does not exist`);
    }
    return this.axes[axisIndex];
  }


  /**
   * should be called after this._array has been updated
  */
  _updateBuffers(info) {
    if (info.realloc) {
      this.needsDispose = true;
      this.array = this._array.array;
      this.clearUpdateRanges();
    } else {
      this.needsUpdate = true;
      this.addUpdateRange(info.beg, info.count); // index and count (in this.array indices, not vertexes)
    }
    this.count = this._array.size / 3;
  }

  /**
   * Appends data to one axis
   * @param {TypedArray} data - the data to append
   * @param {number} axis - the axis to append the data to
  */
  _appendAxis(data, axisIndex) {
    // console.log(`In _appendAxis with axis ${axisIndex}`);
    let axis = this._getAxis(axisIndex);
    if (axis === undefined) {
      axis = new Axis(axisIndex, data.constructor);
      this.axes[axisIndex] = axis;
    }
    if (axis.arrayType !== Object.getPrototypeOf(data)) {
      throw new Error(
        `data type ${data.constructor.name} does not match ` +
        `axis type ${axis.arrayType}`);
    }
    axis.append(data);
  }

  /**
   * synchronize the content of the axis with the main attribute array 
  */
  _synch(axisIndex) {
    const axis = this._getAxis(axisIndex);
    const info = axis.send(this._array);
    this._updateBuffers(info);
  }

  append(data, axisIndex) {
    this._appendAxis(data, axisIndex);
    this._synch(axisIndex)
  }

}

export {
  PlotBufferAttribute,
  PointwiseTransform
};

