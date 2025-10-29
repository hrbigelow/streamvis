import { MOUSE } from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';

class LinePlotControls extends OrbitControls {
  constructor(object, domElement) {
    if (! object.isOrthographicCamera) {
      throw new Error(`LinePlotControls only provided for OrthographicCamera`);
    }
    super(object, domElement);
    this.zoomToCursor = true;
    this.enableRotate = false;
    // this.enablePan = false;
    // this.screenSpacePanning = false;
    this.mouseButtons = { LEFT: MOUSE.PAN, MIDDLE: MOUSE.DOLLY, RIGHT: MOUSE.ROTATE };
    this.listenToKeyEvents(window);
  }

  _pan(deltaX, deltaY) {
    // console.log(`in _pan: ${deltaX}, ${deltaY}`);
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
  }
}

export {
  LinePlotControls
};

