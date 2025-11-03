import * as THREE from 'three';
import { Command } from 'commander';
import { LineSceneReplicator } from '../src/LineSceneReplicator.js';
import { getServiceClient } from '../src/util.js';

const program = new Command();
program
  .option('-s, --scope <string>', 'scope to display')
  .option('-n, --name <string>', 'name to display')
  .option('-h, --host <string>', 'gRPC host:port string')

async function main() {

  program.parse(process.argv);
  const options = program.opts();
  const url = `http://${options.host}`;
  const client = getServiceClient(url);
  const lineMaterial = new THREE.LineBasicMaterial({
    color: 0xff0000,
    linewidth: 1.5,
  });

  const scene = new LineSceneReplicator(
    client, options.scope, options.name, 10, 'x', 'y', lineMaterial);

  scene.start();
}

await main();

