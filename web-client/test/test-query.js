import { create, toJson, toJsonString } from '@bufbuild/protobuf';
import { inspect } from 'util';
import { Command } from 'commander';

import { 
  DataRequestSchema, 
  DataResultSchema,
  ScopeRequestSchema,
  NamesRequestSchema,
  SamplingSchema,
  Reduction
} from '../streamvis/v1/data_pb.js';

import { getServiceClient, optParseInt } from '../src/util.js';

const inspectOpts = { depth: null, colors: true, maxArrayLength: 10 };


const program = new Command();

program
  .option('-s, --scope <string>', 'scope to display')
  .option('-n, --name <string>', 'name to display')
  .option('-h, --host <string>', 'gRPC host:port string')
  .option('-w, --window-size <int>', 'window size for summarizing data (maybe null)', optParseInt)
  .option('-t, --stride <int>', 'stride for windowing results', optParseInt)
  .option('-d, --dry-run', 'if true, do not print results')

async function main() {

  program.parse(process.argv);
  const options = program.opts();
  const url = `http://${options.host}`;
  const client = getServiceClient(url);
  debugger;

  console.log('All Scopes');
  const req = create(ScopeRequestSchema, {});
  for await (const scope of client.scopes(req)) {
    console.log(inspect(scope, inspectOpts));
  }

  console.log(`\nNames in scope ${options.scope}`);
  const nreq = create(NamesRequestSchema, {scope: options.scope});
  for await (const name of client.names(nreq)) {
    console.log(inspect(name, inspectOpts));
  }

  console.log(`\nData in scope ${options.scope}, name ${options.name}`);

  let sampling = undefined;
  if (options.windowSize !== undefined && options.stride !== undefined) {
    sampling = create(SamplingSchema, {
      stride: options.stride,
      reduction: Reduction.REDUCTION_MEAN,
      windowSize: options.windowSize,
    });
  }

  const dreq = create(DataRequestSchema, {
    scopePattern: options.scope,
    namePattern: options.name,
    fileOffset: 0n,
    sampling: sampling
  });

  for await (const dataResult of client.queryData(dreq)) {
    if (options.dryRun) {
      continue;
    }
    console.log(inspect(dataResult, inspectOpts));
  }
}

await main();


