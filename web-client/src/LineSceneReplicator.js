import { SceneReplicator } from './SceneReplicator.js';
import { LinePlot2D } from './LinePlot2D.js';
import { Line2Plot2D } from './Line2Plot2D.js';
import { getAxes } from './util.js';

class LineSceneReplicator extends SceneReplicator {
  /**
   * Constructs a new LineSceneReplicator.
   *
   * @param {protobuf-es Client} rpcClient - the connect-go rpc client
   * @param {string} scopePattern - a regex for filtering scopes
   * @param {string} namePattern - a regex for filtering names
   * @param {object} sampling - an object with windowSize and stride fields
   * @param {list} fieldNames - 
  */
  constructor(rpcClient, scopePattern, namePattern, sampling, refreshSeconds, xField, yField,
    material,
  ) {
    super(rpcClient, scopePattern, namePattern, sampling, refreshSeconds);
    this.xField = xField;
    this.yField = yField;
    this.material = material;
  }

  createObject() {
    return new Line2Plot2D(this.material);
  }

  destroyObject(object) {
    object.dispose();
  }

  toggleLogMode(axisIndex) {
    for (const object of Object.values(this.objects)) {
      object.toggleAxisLog(axisIndex);
    }
    this.sendBoundsChanged();
  }


  /**
   * extracts the (x, y) fields from data, flattens them and appends them to the
   * object
   * @param {LinePlot2D} object - the target object where data is added
   * @param {streamvis.v1.Name} name - the name object describing the data
   * @param {streamvis.v1.Data} data - the source data
  */
  addData(object, name, data) {
    const axesData = getAxes(name, data, [this.xField, this.yField]);
    object.appendPoints(axesData[this.xField], axesData[this.yField]);
    // console.log(`added data ${data.entryId}`);
    // console.dir(object.geometry);
  }


}

export {
  LineSceneReplicator
};


