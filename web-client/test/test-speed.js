import { Box3 } from 'three';

const arr = new Float32Array(3000000);

for (let i = 0; i != arr.length; i++) {
  arr[i] = Math.random();
}


const box = new Box3()

console.time('setFromArray');
box.setFromArray(arr);
console.timeEnd('setFromArray');


