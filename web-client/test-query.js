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

  program.parse(process.argv)
  const options = program.opts() 
  const client = getServiceClient(options.host);

  const req = create(ScopeRequestSchema, {});
  for await (const scope of client.scopes(req)) {
    console.log(inspect(scope, inspectOpts));
  }

  const nreq = create(NamesRequestSchema, {scope: "test-scope"});
  for await (const name of client.names(nreq)) {
    console.log(inspect(name, inspectOpts));
  }

  const dreq = create(DataRequestSchema, {
    scopePattern: options.scope,
    namePattern: options.name,
    fileOffset: 0n
  });

  for await (const dataResult of client.queryData(dreq)) {
    console.log(inspect(dataResult, inspectOpts));
    continue;
    /*
    const { value } = dataResult;
    switch (value?.case) {
      case 'record': {
        const recordResult = value.value;
        for (const name of Object.values(recordResult.names)) {
          console.log(name);
        }
        break;
      }
      case 'data': {
        const pbData = value.value;
        for (const values of pbData.axes) {
          const { data } = values; 
          console.log(data);
          switch (data?.case) {
            case 'floats': {
              const fl = data.value;
              console.log(`floats of length ${fl.value.length}`);
              break;
            }
            case 'ints': {
              const il = data.value;
              console.log(`ints of length ${il.value.length}`);
              break;
            }
            default: {
              console.log(`Warning: Unexpected streamvis.v1.Data axes type: ${data?.case}`);
            }
          }
        }
        break;
      }
      default: {
        throw new Error(`Unexpected queryData response type: ${value?.case}`)
      }
    }
    */
  }
}
await main();


