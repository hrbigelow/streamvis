/**
 * This class periodically queries a gRPC service, using its configured query
 * parameters.  Each query is incremental due to the increasing fileOffset.  It
 * processes any updates or deletes, keeping its local copy in synch, and
 * converting the data into a more specific format for rendering to a scene.
*/
class SceneReplicator {
  /**
   * Constructs a new SceneReplicator.
   *
   * @param {protobuf-es Client} rpcClient - the connect-go rpc client
   * @param {string} scopePattern - a regex for filtering scopes
   * @param {string} namePattern - a regex for filtering names
   * @param {function} createObjectFn - a nullary function for creating a new Object3D
   * @param {function} destroyObjectFn - a nullary function for destroying an Object3D
   * @param {function} addDataFn - a binary function for adding data to an existing object
  */

  constructor(rpcClient, scopePattern, namePattern, createObjectFn, destroyObjectFn, addDataFn) {
    this.client = rpcClient
    this.scopePattern = scopePattern
    this.namePattern = namePattern
    this.fileOffset = 0
    this.createObjectFn = createObjectFn
    this.destroyObjectFn = destroyObjectFn
    this.addDataFn = addDataFn
    this.objects = {} // objectId => Object3D
  }

  _makeObjectId(nameId, index) {
    return `${nameId},${index}`;
  }


  /**
   * Updates local state from gRPC service
  */
  async update() {
    const request = {
      scope_pattern: this.scopePattern,
      name_pattern: this.namePattern,
      file_offset: this.fileOffset 
    };
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
    this.fileOffset = recordResult.fileOffset;

    // delete any non-represented objects
    for (const [objectId, obj] of Object.entries(this.objects)) {
      const nameId = this._getNameId(objectId);
      if (! (nameId in recordResult.names)) {
        this.destroyObjectFn(this.objects[objectId]);
        delete this.objects[objectId];
      }
    }

    // create any new objects
    for (const data in dataItems) {
      objectId = this._makeObjectId(data.nameId, data.index);
      if (! (objectId in this.objects)) {
        this.objects[objectId] = this.createObjectFn();
      }
      const object = this.objects[objectId];
      this.addDataFn(object, data);
    }
}


