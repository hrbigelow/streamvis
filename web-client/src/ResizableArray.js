// threshold under which we grow by 2x
const smallThreshold = 65536;
const growthFactor = 1.5;

/*
 * A container for any TypedArray which is resizable on-demand
 * @param {TypedArray} array - the array holding the initial data
 */
class ResizableArray {
  constructor(array) {
    this.ctor = array.constructor;
    this._array = array; 
    this.size = array.length; // logical size
  }

  get capacity() {
    return this._array.length;
  }

  get array() {
    return this._array;
  }

  /*
   * return the subarray in the range [begin, end)
   * @param {number} begin - the start of the range
   * @param {number} end - the end of the range
   */
  subarray(begin = 0, end = this.size) {
    return this._array.subarray(begin, end);
  }

  /**
   * compute an optimal new capacity if we were to append `appendSize` elements.
  */
  _newCapacity(appendSize) {
    const needed = this.size + appendSize;
    if (needed < this.capacity) {
      return this.capacity;
    }
    if (this.capacity < smallThreshold) {
      return Math.max(needed, this.capacity * 2);
    }
    const headroom = needed * 0.25;
    return Math.max(
      Math.round(needed + headroom),
      Math.round(this.capacity * growthFactor)
    );
  }
  
  /**
   * Ensures that the capacity can accommodate an append of size `appendSize
   * @returns: whether a reallocation was performed
  */
  reserveAdditional(appendSize) {
    const newCapacity = this._newCapacity(appendSize);
    if (newCapacity > this.capacity) {
      // console.log(`reserveAdditional: resizing from ${this.capacity} to ${newCapacity}`);
      const newArray = new this.ctor(newCapacity);
      newArray.set(this._array);
      this._array = newArray;
      return true;
    }
    return false;
  }


  /*
   * appends data to the array, reallocating if necessary
   * @param {TypedArray}
   * @return {bool} true if there was a reallocation
   */
  append(data) {
    if (data.constructor !== this.ctor) {
      throw new Error(
        `set requires source data type to match: ${data.constructor} !== ${this.ctor}`);
    }
    const realloc = this.reserveAdditional(data.length);
    this._array.set(data, this.size);
    this.size += data.length; 
    return realloc;
  }

  setAt(index, value) {
    this._array[index] = value;
  }

}


export {
  ResizableArray
};
