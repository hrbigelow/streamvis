import { MOUSE, Controls } from 'three';
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

function onToggleAxes(event) {
  if (this.enabled === false) return;
  if (this.keys.TOGGLE_XLOG.includes(event.code)) {
    for (const object of Object.values(this.object.objects)) {
      object.toggleXAxisMode();
    }
  }
  if (this.keys.TOGGLE_YLOG.includes(event.code)) {
    for (const object of Object.values(this.object.objects)) {
      object.toggleXAxisMode();
    }
  }
}


class ToggleLogControls extends Controls {

  /*
   * @param {LineSceneReplicator} object - the object being managed by this control
   * @param {HTMLDomElement} domElement - the target element to listen on.
  */
  constructor(object, domElement) {
    super(object, domElement);
    this.keys = {
      TOGGLE_XLOG: ['KeyF', 'KeyJ'],
      TOGGLE_YLOG: ['KeyD', 'KeyK'],
    };
    this._onToggleAxes = onToggleAxes.bind(this);

    if (this.domElement !== null) {
      this.connect(this.domElement);
    }
  }

  connect(element) {
    super.connect(element);
    window.addEventListener('keydown', this._onToggleAxes);
  }

  disconnect() {
    this.domElement.removeEventListener('keydown', this._onToggleAxes);
  }
}




export {
  LinePlotControls,
  ToggleLogControls
};

