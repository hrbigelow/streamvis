import { Controls } from 'three';

function onToggleAxes(event) {
  if (this.enabled === false) return;
  if (this.keys.TOGGLE_XLOG.includes(event.code)) {
    this.object.toggleLogMode(0);
  }
  if (this.keys.TOGGLE_YLOG.includes(event.code)) {
    this.object.toggleLogMode(1);
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
  ToggleLogControls
};
