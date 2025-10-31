import * as THREE from 'three';
import { resizeToWindow, getServiceClient } from './src/util.js';
import { LineSceneReplicator } from './src/LineSceneReplicator.js'; 
import { LinePlotControls, ToggleLogControls } from './src/LinePlotControls.js';


const canvas = document.querySelector('#plot-canvas');
const renderer = new THREE.WebGLRenderer({
  antialias: true,
  canvas
});

renderer.setSize(window.innerWidth, window.innerHeight);

const camera = new THREE.OrthographicCamera(
  1, 10000, 2, -2, 0, 2000 
);
camera.position.z = 100;

// const controls = new LinePlotControls(camera, renderer.domElement);

window.addEventListener('resize', () => resizeToWindow(window, renderer, camera));
// camera.position.set(0, 0, 1000);
// camera.lookAt(20000, 0, 0);

// see vite.config.js proxy forwarding
const client = getServiceClient('/');
// const scopePattern = '4xmess3-c100-noise0.01';
// const namePattern = 'loss-kldiv|probe-kldiv';
const scopePattern = 'test1';
const namePattern = 'sinusoidal';
const refreshSeconds = 5;

const lineMaterial = new THREE.LineBasicMaterial({
  color: 0xff0000,
  linewidth: 1.2,
});

const scene = new LineSceneReplicator(
  client, scopePattern, namePattern, 10, 'x', 'y', lineMaterial); 

const keyControls = new ToggleLogControls(scene, renderer.domElement);
const sceneStart = scene.start();

function animate() {
  /*
  if (resizeRendererToDisplaySize(renderer)) {
    const canvas = renderer.domElement;
    camera.aspect = canvas.clientWidth / canvas.clientHeight;
    camera.updateProjectionMatrix();
  }
  */
  renderer.render(scene, camera);
}

renderer.setAnimationLoop(animate);

