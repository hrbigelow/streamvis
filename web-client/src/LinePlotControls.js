import { MOUSE, Controls, Vector4 } from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { printMatrix, printVector } from './util.js';

function reportMatrices(event) {
  if (event.code == 'KeyR') {
    const point = new Vector4(10**6, 0, 0, 1);
    const clip = point.clone()
      .applyMatrix4(this.object.matrixWorldInverse)
      .applyMatrix4(this.object.projectionMatrix)

    const ndc = clip.clone().divideScalar(clip.w);

    // printMatrix(this.object.projectionMatrix, 'projectionMatrix');
    printMatrix(this.object.matrixWorld, 'matrixWorld');
    printVector(clip, 'clip');
    printVector(ndc, 'ndc');
    console.log(`left: ${this.object.left}`);
    console.log(`right: ${this.object.right}`);
    console.log(`top: ${this.object.top}`);
    console.log(`bottom: ${this.object.bottom}`);
  }
}


class LinePlotControls extends OrbitControls {
  
  constructor(object, domElement) {
    if (! object.isOrthographicCamera) {
      throw new Error(`LinePlotControls only provided for OrthographicCamera`);
    }
    super(object, domElement);
    this._reportMatrices = reportMatrices.bind(this);
    this.zoomToCursor = true;
    this.enableRotate = false;
    // this.enablePan = false;
    // this.screenSpacePanning = false;
    this.mouseButtons = { LEFT: MOUSE.PAN, MIDDLE: MOUSE.DOLLY, RIGHT: MOUSE.ROTATE };
    this.listenToKeyEvents(window);
    
    if (this.domElement !== null) {
      this.connect(this.domElement);
    }

  }

  update(deltaTime = null){
    super.update(deltaTime)
  }



  /*
  _pan(deltaX, deltaY) {
    console.log(`in _pan: ${deltaX}, ${deltaY}`);
    const element = this.domElement;
    const zoom = this.object.zoom;
    const frustumWidth = this.object.right - this.object.left;
    const frustumHeight = this.object.top - this.object.bottom;
    const offsetX = (deltaX / element.clientWidth) * frustumWidth / zoom;
    const offsetY = (deltaY / element.clientHeight) * frustumHeight / zoom;
    this.object.position.x -= offsetX;
    this.object.position.y += offsetY;
    this.target.x -= offsetX;
    this.target.y += offsetY;
    console.dir(this.object);
    // console.log(`LinePlotControls:`);
    // console.dir(this.object);
  }
  */

  connect(element) {
    super.connect(element);
    // window.addEventListener('keydown', this._reportMatrices);
  }

  disconnect() {
    super.disconnect();
  }


}



export {
  LinePlotControls,
};


