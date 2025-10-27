import { Scene } from 'three';
import { create } from '@bufbuild/protobuf';
import { DataRequestSchema } from '../streamvis/v1/data_pb.js';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * This class periodically queries a gRPC service, using its configured query
 * parameters.  Each query is incremental due to the increasing fileOffset.  It
 * processes any updates or deletes, keeping its local copy in synch, and
 * converting the data into a more specific format for rendering to a scene.
*/
class SceneReplicator extends Scene {
  /**
   * Constructs a new SceneReplicator.
   *
   * @param {protobuf-es Client} rpcClient - the connect-go rpc client
   * @param {string} scopePattern - a regex for filtering scopes
   * @param {string} namePattern - a regex for filtering names
  */

  constructor(rpcClient, scopePattern, namePattern, refreshSeconds) {
    super();
    this.client = rpcClient
    this.scopePattern = scopePattern
    this.namePattern = namePattern
    this.fileOffset = 0
    this.refreshSeconds = refreshSeconds
    this.objects = {} // objectId => Object3D
    this.names = {}
  }

  /**
   * returns {Object3D} - a new object to be stored in this.objects
  */
  createObject() {
    throw new Error('Unimplemented');
  }


  /**
   * Releases any resources owned by object
  */
  destroyObject(object) {
    throw new Error('Unimplemented');
  }

  /**
   * Adds data to object
  */
  addData(object, data) {
    throw new Error('Unimplemented');
  }

  _makeObjectId(nameId, index) {
    return `${nameId},${index}`;
  }

  _getNameId(objectId) {
    return objectId.split(',')[0];
  }

  /**
   * Updates local state from gRPC service
  */
  async update() {
    const request = create(DataRequestSchema, 
      {
        scopePattern: this.scopePattern,
        namePattern: this.namePattern,
        fileOffset: this.fileOffset 
      });

    let recordResult = null;
    const dataItems = [];
    for await (const resp of this.client.queryData(request)) {
      const { value } = resp;
      switch (value?.case) {
        case 'record': {
          recordResult = value.value;
          break;
        }
        case 'data': {
          dataItems.push(value.value);
          break;
        }
        default: {
          throw new Error(`Unexpected queryData response type: ${value?.case}`);
        }
      }
    }
    console.log('request:');
    console.dir(request);
    console.log('result:');
    console.dir(recordResult);

    if (recordResult === null) {
      throw new Error(`Got null recordResult from request`);
    }
    this.fileOffset = recordResult.fileOffset;
    this.names = recordResult.names;

    // delete any non-represented objects and detach from this scene
    for (const [objectId, obj] of Object.entries(this.objects)) {
      const nameId = this._getNameId(objectId);
      if (! (nameId in this.names)) {
        this.objects[objectId].removeFromParent();
        this.destroyObject(this.objects[objectId]);
        delete this.objects[objectId];
      }
    }

    // create any new objects and attach to this scene
    for (let data of dataItems) {
      debugger;
      const objectId = this._makeObjectId(data.nameId, data.index);
      if (! (objectId in this.objects)) {
        this.objects[objectId] = this.createObject();
        this.add(this.objects[objectId]);
      }
      const object = this.objects[objectId];
      const nameId = this._getNameId(objectId)
      const name = this.names[nameId];
      this.addData(object, name, data);
    }
  }

  /**
   * Start the infinite periodic refresh cycle
  */
  async start() {
    while (true) {
      await this.update();
      console.log('after update');
      await sleep(this.refreshSeconds * 1000);
    }
  }

}

export {
  SceneReplicator
};

