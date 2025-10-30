import { BufferGeometry } from 'three'; 

/*
 * LineBufferGeometry represents a line in 2D space.  It can also change its shape so
 * that x and/or y coordinates are log transformed or not.
 * 
*/
class LineBufferGeometry extends BufferGeometry {
  constructor() {
    super();
    this.type = 'LineBufferGeometry';
    this.xAxisLogMode = false;
    this.yAxisLogMode = false;
  }

  /*
   * sets x and y axes modes
   * @param {bool} xlog - whether to set the x axis to log mode
   * @param {bool} ylog - whether to set the y axis to log mode
  */
  setAxisMode(xlog, ylog) {
  }
}


