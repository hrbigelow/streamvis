import { SceneReplicator } from './SceneReplicator';
import { GrowingLine } from './GrowingLine';
import { extractData } from './util';

class LineSceneReplicator extends SceneReplicator {
  /**
   * Constructs a new LineSceneReplicator.
   *
   * @param {protobuf-es Client} rpcClient - the connect-go rpc client
   * @param {string} scopePattern - a regex for filtering scopes
   * @param {string} namePattern - a regex for filtering names
   * @param {list} fieldNames - 
  */
  constructor(rpcClient, scopePattern, namePattern, refreshSeconds, xField, yField,
    material,
  ) {
    super(rpcClient, scopePattern, namePattern, refreshSeconds);
    this.xField = xField;
    this.yField = yField;
    this.material = material;
  }

  createObject() {
    return new GrowingLine(100, this.material);
  }

  destroyObject(object) {
    object.dispose();
  }

  /**
   * extracts the (x, y) fields from data, flattens them and appends them to the
   * object
   * @param {Object3D} object - the target object where data is added
   * @param {streamvis.v1.Name} name - the name object describing the data
   * @param {streamvis.v1.Data} data - the source data
  */
  addData(object, name, data) {
    const vals = extractData(name, data, [this.xField, this.yField]);
    object.appendPoints(vals);
  }

}

export {
  LineSceneReplicator
};


