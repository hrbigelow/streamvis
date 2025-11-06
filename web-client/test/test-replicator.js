import * as THREE from 'three';
import { Command } from 'commander';
import { LineSceneReplicator } from '../src/LineSceneReplicator.js';
import { getServiceClient, optParseInt } from '../src/util.js';

const program = new Command();
program
  .option('-s, --scope <string>', 'scope to display')
  .option('-n, --name <string>', 'name to display')
  .option('-h, --host <string>', 'gRPC host:port string')
  .option('-w, --window-size <int>', 'window size for summarizing data (maybe null)', optParseInt)
  .option('-t, --stride <int>', 'stride for windowing results', optParseInt)

async function main() {

  program.parse(process.argv);
  const options = program.opts();
  const url = `http://${options.host}`;
  const client = getServiceClient(url);
  const lineMaterial = new THREE.LineBasicMaterial({
    color: 0xff0000,
    linewidth: 1.5,
  });
  let sampling;
  debugger;
  if (options.windowSize !== undefined && options.stride !== undefined) {
    sampling = { windowSize: options.windowSize, stride: options.stride };
  }

  const scene = new LineSceneReplicator(
    client, options.scope, options.name, sampling, 10, 'x', 'y', lineMaterial);

  scene.start();
}

await main();

