import { create, toJson, toJsonString } from '@bufbuild/protobuf';
import { inspect } from 'util';
import { Command } from 'commander';

import { 
  DataRequestSchema, 
  DataResultSchema,
  ScopeRequestSchema,
  NamesRequestSchema
} from './streamvis/v1/data_pb.js';

import { getServiceClient } from './src/util.js';


const inspectOpts = { depth: null, colors: true, maxArrayLength: 10 };

const program = new Command();

program
  .option('-s, --scope <string>', 'scope to display')
  .option('-n, --name <string>', 'name to display')
  .option('-h, --host <string>', 'gRPC host:port string')


async function main() {

  program.parse(process.argv);
  const options = program.opts();
  const client = getServiceClient(options.host);

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
  const dreq = create(DataRequestSchema, {
    scopePattern: options.scope,
    namePattern: options.name,
    fileOffset: 0n
  });

  for await (const dataResult of client.queryData(dreq)) {
    console.log(inspect(dataResult, inspectOpts));
    continue;
  }
}

await main();
// if (process.stdout.writableNeedDrain) {
//    await new Promise(resolve => process.stdout.once('drain', resolve));
// }


