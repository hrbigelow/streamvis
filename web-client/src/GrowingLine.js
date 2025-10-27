import * as THREE from 'three';
import {
  BufferGeometry,
  LineBasicMaterial,
  Line,
  Float32BufferAttribute,
  DynamicDrawUsage
} from 'three';

class GrowingLine extends Line {

  /**
   * Constructs a new GrowingLine
   * Since geometry is appended, always starts empty
   * @param {number} itemSize - the size of one logical position item (2 or 3)
   * @param {number} initialCapacity - the initial capacity (number of logical position items)
   * @param {Material} - material to use for this Line
  */
  constructor(itemSize, initialCapacity, material = new LineBasicMaterial({ color: 0xff0000 })) {
    debugger;
    if (itemSize !== 2 && itemSize !== 3) {
      throw new Error("itemSize must be 2 or 3");
    }
    super(new BufferGeometry(), material);
    this.size = 0;
    this.capacity = initialCapacity;
    this.increment = initialCapacity;
    this.itemSize = itemSize;
    this._refreshAttribute();
  }

  /**
   * refreshes the BufferAttribute 
  */
  _refreshAttribute() {
    const positions = new Float32Array(this.capacity);
    const positionAttribute = new Float32BufferAttribute(positions, this.itemSize);
    positionAttribute.setUsage(THREE.DynamicDrawUsage);
    this.geometry.setAttribute('position', positionAttribute);
  }

  /**
   * appends points to the geometry
   * @param {Float32Array} points - The points.
   * @return {void}
  */
  appendPoints(points) {
    debugger;
    if (points.length % this.itemSize !== 0) {
      throw new Error(`points.length={points.length} not divisible by itemSize={this.itemSize}`);
    }
    if (this.size + points.length > this.capacity) {
      const oldGeometry = this.geometry;
      this.capacity = Math.max(this.capacity + this.increment, this.size + points.length);
      this.increment *= 2;
      this.geometry = new BufferGeometry();
      this._refreshAttribute();

      this.geometry.getAttribute('position').array.set(
        oldGeometry.getAttribute('position').array.subarray(this.size)
      );
      oldGeometry.dispose();
    }
    this.geometry.getAttribute('position').array.set(points, this.size);
    this.size += points.length;
  }

  dispose() {
    this.geometry.dispose();
  }

}

export {
  GrowingLine
};



