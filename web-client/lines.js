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

const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.0, 10);
// const camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 1000);
camera.position.z = 10;

const controls = new LinePlotControls(camera, renderer.domElement);

window.addEventListener('resize', () => resizeToWindow(window, renderer, camera));
// camera.position.set(0, 0, 1000);
// camera.lookAt(20000, 0, 0);

// see vite.config.js proxy forwarding
const client = getServiceClient('/');
const scopePattern = '4xmess3-c100-noise0.01';
// const namePattern = 'loss-kldiv|probe-kldiv';
const namePattern = 'loss-kldiv';
// const namePattern = 'participation-ratio';
// const scopePattern = 'test-100$';
// const namePattern = 'sinusoidal';
const refreshSeconds = 5;

const lineMaterial = new THREE.LineBasicMaterial({
  color: 0xff0000,
  linewidth: 1.3,
});

const scene = new LineSceneReplicator(
  client, scopePattern, namePattern, 10, 'x', 'y', lineMaterial); 


// const scene = new THREE.Scene();

/*
const box = new THREE.BoxGeometry(1, 1, 1);
const boxMaterial = new THREE.MeshBasicMaterial({ color: 0x00ff00 });
const cube = new THREE.Mesh(box, boxMaterial);
scene.add(cube);
*/


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

