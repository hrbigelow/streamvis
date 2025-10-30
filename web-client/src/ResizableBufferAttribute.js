import {
  BufferAttribute
} from 'three';
import { ResizableArray } from './src/ResizableArray.js';


class ResizableBufferAttribute extends BufferAttribute {
  /*
   * @param {TypedArray} array -> The array holding the attribute data.
   * @param {number} itemSize - The item size.
   * @param {boolean} [normalized=false] - Whether the data are normalized or not.
  */
  constructor(array, itemSize, normalized=false) {
    super(array, itemSize, normalized); 
    this._array = new ResizableArray(array);

    /**
     * This is a public property which must provide a TypedArray to callers
     */
    Object.defineProperty(this, 'array', {
      get() {
        return this._array.array;
      },
      configurable: false,
      enumerable: true
    });

    get capacity() {
      return this._array.capacity;
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
    this._array.set(value, offset);
  }

}
