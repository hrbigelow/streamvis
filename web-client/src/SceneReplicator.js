import { Scene, Box3, Vector3 } from 'three';
import { create } from '@bufbuild/protobuf';
import { 
  DataRequestSchema,
  SamplingSchema,
  Reduction
} from '../streamvis/v1/data_pb.js';

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
   * @param {object} sampling - object having windowSize and stride fields (optional)
   *
   * If `sampling` is defined, the returned data will be sampled accordingly.
  */

  constructor(
    rpcClient, scopePattern, namePattern, sampling, refreshSeconds) {
    super();
    this.background = 0xffffff;
    this.client = rpcClient
    this.scopePattern = scopePattern
    this.namePattern = namePattern
    this.sampling = sampling === undefined
      ? undefined  
      : create(SamplingSchema, {
        stride: sampling.stride,
        reduction: Reduction.REDUCTION_MEAN,
        windowSize: sampling.windowSize,
      });
    this.fileOffset = 0
    this.refreshSeconds = refreshSeconds
    this.objects = {} // objectId => Object3D.
    this.names = {}
    this._boundingBox = undefined
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
        fileOffset: this.fileOffset,
        sampling: this.sampling
      });

    for await (const resp of this.client.queryData(request)) {
      const { value } = resp;
      switch (value?.case) {
        case 'record': {
          this.updateClientState(value.value);
          break;
        }
        case 'data': {
          this.addDataItem(value.value);
          break;
        }
        default: {
          throw new Error(`Unexpected queryData response type: ${value?.case}`);
        }
      }
    }

    this.sendBoundsChanged();
  }

  sendBoundsChanged() {
    this.dispatchEvent({
      type: 'boundsChanged',
      box: this.getBoundingBox(),
    });
  }

  getBoundingBox() {
    if (Object.values(this.objects).length === 0) {
      return new Box3(new Vector3(0, 0, 0), new Vector3(1, 1, 1));
    }
    const box = new Box3();
    for (const obj of Object.values(this.objects)) {
      box.union(obj.getBoundingBox());
    }
    return box;
  }

  updateClientState(record) {
    this.names = record.names;
    this.fileOffset = record.fileOffset;

    // delete any non-represented objects and detach from this scene
    for (const [objectId, obj] of Object.entries(this.objects)) {
      const nameId = this._getNameId(objectId);
      if (! (nameId in this.names)) {
        this.objects[objectId].removeFromParent();
        this.destroyObject(this.objects[objectId]);
        delete this.objects[objectId];
      }
    }
  }

  addDataItem(data) {
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

  /**
   * Start the infinite periodic refresh cycle
  */
  async start() {
    while (true) {
      await this.update();
      console.log('after update');
      // console.dir(this.objects);
      await sleep(this.refreshSeconds * 1000);
    }
  }

}

export {
  SceneReplicator
};

