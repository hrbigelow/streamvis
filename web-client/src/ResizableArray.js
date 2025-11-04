
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

  /*
   * maybe re-allocate an array to bring capacity inside the range.
   * if so, the new capacity will be the smallest change requred to do so.
   * @param {number} minCapacity - the minimum for the new capacity
   * @param {number} maxCapacity - the maximum for the new capacity
   * @return {bool} true if there was a reallocation
  */
  resize(minCapacity, maxCapacity=Infinity) {
    if (minCapacity > maxCapacity || minCapacity < 0) {
      throw new Error(`Must provide a valid and non-negative range for resizing.  Got [${minCapacity}, ${maxCapacity}]`)
    }
    const newCapacity = Math.min(Math.max(minCapacity, this.capacity), maxCapacity)
    if (newCapacity === this.capacity) {
      return false;
    }
    const newArray = new this.ctor(newCapacity);
    newArray.set(this._array);
    this._array = newArray;
    return true;
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
    const minCapacity = Math.max(data.length + this.size, this.capacity * 2);
    const realloc = this.resize(minCapacity);
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
