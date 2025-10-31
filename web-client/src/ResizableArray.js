
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
   * maybe grow the capacity of the array
   * @param {number} minCapacity - the minimum desired capacity
   */
  grow(minCapacity) {
    if (minCapacity <= this.capacity) {
      return;
    }
    const newCapacity = Math.max(minCapacity, this.capacity * 2);
    const newArray = new this.ctor(newCapacity);
    newArray.set(this._array.subarray(0, this.size));
    this._array = newArray;
  }

  /*
   * maybe re-allocate an array to bring capacity inside the range.
   * if so, the new capacity will be the smallest change requred to do so.
   * @param {number} minCapacity - the minimum for the new capacity
   * @param {number} maxCapacity - the maximum for the new capacity
  */
  resize(minCapacity, maxCapacity=Infinity) {
    if (minCapacity > maxCapacity || minCapacity < 0) {
      throw new Error(`Must provide a valid and non-negative range for resizing.  Got [${minCapacity}, ${maxCapacity}]`)
    }
    const newCapacity = Math.min(Math.max(minCapacity, this.capacity), maxCapacity)
    if (newCapacity === this.capacity) {
      return;
    }
    const newArray = new this.ctor(newCapacity);
    newArray.set(this._array.subarray(0, this.size));
    this._array = newArray;
  }

  /*
   * set the range [offset, offset+data.length] to the content of data
   * @param {TypedArray}
   */
  set(data, offset) {
    if (data.constructor !== this.ctor) {
      throw new Error(
        `set requires source data type to match: ${data.constructor} !== ${this.ctor}`);
    }
    const minCapacity = Math.max(data.length + offset, this.capacity * 2);
    this.resize(minCapacity);
    this._array.set(data, offset);
    this.size = Math.max(this.size, minCapacity);
  }

  /*
   * construct a new array containing just the range [begin, end)
   * @param {number} begin - the start of the range
   * @param {number} end - the end of the range
   */
  subarray(begin, end) {
    return this._array(begin, end);
  }
}


export {
  ResizableArray
};
