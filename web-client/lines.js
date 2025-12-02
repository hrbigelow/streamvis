import * as THREE from 'three';
import { resizeToWindow, getServiceClient, cameraAutoFit, optParseInt } from './src/util.js';
import { LineSceneReplicator } from './src/LineSceneReplicator.js'; 
import { LinePlotControls } from './src/LinePlotControls.js';
import { ToggleLogControls } from './src/ToggleLogControls.js';
import { LineMaterial } from 'three/examples/jsm/lines/LineMaterial.js';

const params = new URLSearchParams(window.location.search);
const scopePattern = params.get('s');
const namePattern = params.get('n');
const windowSize = parseInt(params.get('w'));
const stride = parseInt(params.get('t'));
const far = parseFloat(params.get('far'));

const canvas = document.querySelector('#plot-canvas');
// const renderer = new THREE.WebGLRenderer({
  // antialias: true,
  // canvas
// });

const renderer = new THREE.WebGPURenderer({
  antialias: true,
  canvas
});

renderer.setSize(window.innerWidth, window.innerHeight);
renderer.setPixelRatio(window.devicePixelRatio);

// far plane means far from the scene
const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, far);
camera.position.z = far / 2; 

window.addEventListener('resize', () => resizeToWindow(window, renderer, camera));


// see vite.config.js proxy forwarding
const client = getServiceClient('/');
const refreshSeconds = 600;
// const sampling = { windowSize: 100, stride: 500 }
let sampling = undefined; 

if (Number.isFinite(windowSize) && Number.isFinite(stride)) {
  sampling = { windowSize, stride };
}
console.log('sampling: ');
console.dir(sampling);

/*
const line2Material = new LineMaterial({
  color: 0xff0000,
  linewidth: 2.0,
  vertexColors: false,
  alphaToCoverage: true,
});
*/


const scene = new LineSceneReplicator(
  client, scopePattern, namePattern, sampling, refreshSeconds, 'x', 'y'); 


const controls = new LinePlotControls(camera, renderer.domElement);
// cameraAutoFit(camera, controls, scene.getBoundingBox());
await scene.update();
scene.toggleLogMode(0);
scene.toggleLogMode(1);
cameraAutoFit(camera, controls, scene.getBoundingBox());

scene.background = new THREE.Color(0xffffff)

scene.addEventListener('boundsChanged', (event) => {
  cameraAutoFit(camera, controls, event.box);
});


function render() {
  renderer.render(scene, camera);
  console.log('render');
}

controls.addEventListener('change', (event) => {
  render();
});


const keyControls = new ToggleLogControls(scene, renderer.domElement);
// const sceneStart = scene.start();

// renderer.setAnimationLoop(animate);

