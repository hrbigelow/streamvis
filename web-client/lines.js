import * as THREE from 'three';
import { resizeToWindow, getServiceClient } from './src/util.js';
import { LineSceneReplicator } from './src/LineSceneReplicator.js'; 


const canvas = document.querySelector('#plot-canvas');
const renderer = new THREE.WebGLRenderer({
  antialias: true,
  canvas
});

renderer.setSize(window.innerWidth, window.innerHeight);


// const camera = new THREE.PerspectiveCamera(
  // 75, window.innerWidth / window.innerHeight, 0.1, 1000
// );

const camera = new THREE.OrthographicCamera(
  1, 10000, 2, -2, -1, 1
);
camera.position.z = 1;

window.addEventListener('resize', () => resizeToWindow(window, renderer, camera));
// camera.position.set(0, 0, 1000);
// camera.lookAt(20000, 0, 0);

// see vite.config.js proxy forwarding
const client = getServiceClient('/');
// const scopePattern = '4xmess3-c100-noise0.01';
// const namePattern = 'loss-kldiv';
const scopePattern = 'test3';
const namePattern = 'sinusoidal';
const refreshSeconds = 5;

/*
const geometry = new THREE.BoxGeometry(1, 1, 1);
const material = new THREE.MeshBasicMaterial({ color: 0x00ff00 });
const cube = new THREE.Mesh(geometry, material);
*/

const lineMaterial = new THREE.LineBasicMaterial({
  color: 0xff0000,
  linewidth: 2,
});

const scene = new LineSceneReplicator(
  client, scopePattern, namePattern, 10, 'x', 'y', lineMaterial); 
const sceneStart = scene.start();
// scene.add(cube);

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

