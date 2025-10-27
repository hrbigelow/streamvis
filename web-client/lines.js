import * as THREE from 'three';
import { getServiceClient } from './src/util.js';
import { LineSceneReplicator } from './src/LineSceneReplicator.js'; 

const renderer = new THREE.WebGLRenderer();
renderer.setSize(window.innerWidth, window.innerHeight);
document.body.appendChild(renderer.domElement);

// const camera = new THREE.PerspectiveCamera(
  // 75, window.innerWidth / window.innerHeight, 0.1, 1000
// );

const camera = new THREE.OrthographicCamera(
  0, 4000, 2, -2, -1, 1
);
camera.position.z = 1;

// camera.position.set(0, 0, 1000);
// camera.lookAt(20000, 0, 0);

// see vite.config.js proxy forwarding
const client = getServiceClient('/');
// const scopePattern = '4xmess3-c100-noise0.01';
// const namePattern = 'loss-kldiv';
const scopePattern = 'test';
const namePattern = 'sinusoidal';
const refreshSeconds = 5;

const geometry = new THREE.BoxGeometry(1, 1, 1);
const material = new THREE.MeshBasicMaterial({ color: 0x00ff00 });
const cube = new THREE.Mesh(geometry, material);

const lineMaterial = new THREE.LineBasicMaterial({
  color: 0xff0000,
  linewidth: 5,
});

const scene = new LineSceneReplicator(
  client, scopePattern, namePattern, 10, 'x', 'y', lineMaterial); 
const sceneStart = scene.start();
scene.add(cube);

function animate() {
  cube.rotation.x += 0.1;
  cube.rotation.y += 0.1;
  renderer.render(scene, camera);
}

renderer.setAnimationLoop(animate);


