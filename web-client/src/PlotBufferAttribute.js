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
      this.count++;
      mprev = this.m;
      this.m += (v - this.m) / this.count;
      this.s += (v - mprev) * (v - this.m); 
    }
  }

  stats() { 
    return { mean: this.m, variance: this.s / this.count };
  }

  clear() {
    this.count = 0;
    this.s = 0;
    this.m = 0;
  }
}


/**
 * A class for representing transforms 
*/
class PointwiseTransform {
  constructor(name, fun) {
    this.name = name;
    this.fun = fun;
  }
}


class Axis {
  constructor(axisIndex, ctor) {
    this.index = axisIndex;
    this.array = new ResizableArray(new ctor(0)); 
    this.stats = new RunningStats(); // stats on transformed data 
    this.transform = new PointwiseTransform('none', undefined);
    this.currentWhitening = { mean: 0, variance: 1}; // currently used whitening parameters
    this._offset = 0; // offset into source array to read from
    this._targetOffset = 0; // offset into target array to resume writing
  }

  get arrayType() {
    return Object.getPrototypeOf(this.array.array);
  }

  // append ground-truth data to this axis
  append(data) {
    return this.array.set(data, this.array.size);
  }

  _whitenOne(stats, val) {
    return (val - stats.mean) / Math.sqrt(stats.variance);
  }

  /**
   * Do a pass over all data on target array, rewhitening and collecting
   * new stats.
  */
  _rewhiten(target) {
    const { mean, variance } = this.currentWhitening = this.stats.stats();
    const stddev = Math.sqrt(variance);
    for (let i = this.index; i < this._targetOffset; i += 3) {
      target[i] = (target[i] - mean) / stddev;
    }
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
    let data = this.array.subarray(this._offset);
    if (this.transform.fun !== undefined) {
      data = this.transform.fun(data);
    }
    this.stats.update(data);
    const targetEnd = (this._targetOffset + data.length) * 3;
    const info = { realloc: false, beg: this._targetOffset / 3, end: targetEnd / 3};
    info.realloc = target.resize(targetEnd);

    const { mean, variance } = this.currentWhitening; 
    const stddev = Math.sqrt(variance);
    for (let i = this._offset, j = this._targetOffset * 3 + this.index; 
      i != this.array.size; i++, j += 3) {
      target.setAt(j, (data[i] - mean) / stddev);
    }
    this._offset = this.array.size;
    this._targetOffset = targetEnd;

    const ts = this.stats.stats();
    const adjmean = this._whitenOne(this.currentWhitening, ts.mean);
    const adjvar = this._whitenOne(this.currentWhitening, ts.variance);
    console.log(`Before whitening with index ${this.index}:`, target._array.slice(0, 10));
    if (adjmean - adjvar < -5 || adjmean + adjvar > 5) { // TODO: tweak this
      this._rewhiten(target._array);
      console.log('whitened:', target._array.slice(0, 10));
      info.beg = 0;
    }
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
    if (axis < 0 || axis >= this.axes.length) {
      throw new Error(`axis ${axis} not valid`);
    }
    if (this.axes[axis].transform === fun) {
      return; // nothing to do
    } 
    this.axes[axis].transform = fun;
    this._synch(axisIndex);
  }

  _getAxis(axisIndex) {
    if (axisIndex < 0 || axisIndex >= this.axes.length) {
      throw new Error(`axis ${axisIndex} does not exist`);
    }
    return this.axes[axisIndex];
  }


  /**
   * called to synchronize any 
  */
  _updateBuffers(info) {
    if (info.realloc) {
      this.needsDispose = true;
      this.array = this._array.array;
    } else {
      this.needsUpdate = true;
      this.addUpdateRange(info.beg, info.end);
    }
    this.count = info.end;
  }

  /**
   * Appends data to one axis
   * @param {TypedArray} data - the data to append
   * @param {number} axis - the axis to append the data to
  */
  _appendAxis(data, axisIndex) {
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
    const info = axis.send(this._array);
    this._updateBuffers(info);
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

