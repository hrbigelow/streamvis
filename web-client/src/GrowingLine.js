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
   * @param {number} initialCapacity - the initial capacity (number of logical position items)
   * @param {Material} - material to use for this Line
  */
  constructor(initialCapacity, material = new LineBasicMaterial({ color: 0xff0000 })) {
    super(new BufferGeometry(), material);
    this.size = 0;
    this.capacity = initialCapacity;
    this.increment = initialCapacity;
    this._refreshAttribute();
  }

  /**
   * refreshes the BufferAttribute 
  */
  _refreshAttribute() {
    const positions = new Float32Array(this.capacity);
    const positionAttribute = new Float32BufferAttribute(positions, 3);
    positionAttribute.setUsage(THREE.DynamicDrawUsage);
    this.geometry.setAttribute('position', positionAttribute);
    this.geometry.setDrawRange(0, 0);
  }

  /**
   * appends points to the geometry
   * @param {Float32Array} points - interleaved 3D points: [x0, y0, z0, x1, y1, z1, ...]
   * @return {void}
  */
  appendPoints(points) {
    if (points.length % 3 !== 0) {
      throw new Error(`points.length={points.length} not divisible by 3`);
    }
    const oldSize = this.size;
    const newSize = this.size + points.length;

    if (newSize > this.capacity) {
      const oldGeometry = this.geometry;
      this.capacity = Math.max(this.capacity + this.increment, newSize);
      this.increment *= 2;
      this.geometry = new BufferGeometry();
      this._refreshAttribute();
      const attribute = this.geometry.getAttribute('position'); 
      attribute.array.set(
        oldGeometry.getAttribute('position').array.subarray(0, oldSize),
        0
      );
      attribute.array.set(points, oldSize);

      attribute.needsUpdate = true;
      attribute.addUpdateRange(0, newSize);

      oldGeometry.dispose();
    } else {
      const attribute = this.geometry.getAttribute('position');
      attribute.array.set(points, oldSize);

      attribute.needsUpdate = true;
      attribute.addUpdateRange(oldSize, points.length)
    }

    this.size = newSize;
    this.geometry.setDrawRange(0, newSize / 3);
    console.log(`setting Draw Range to (0, ${newSize / 3}`);

  }

  dispose() {
    this.geometry.dispose();
  }

}

export {
  GrowingLine
};



