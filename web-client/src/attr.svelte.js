function toHex(str) {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(str);
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

class UIAttrState {
  #runs;   // the full catalog of runs under this filter's control
  #handle; // the field handle of the attribute this filter represents
  #active; // whether this overall filter is active
  #values; // value => bool
  #valuesProxy; 
  #valuesActive;
  
  constructor(runs, matchState, handle, fieldIndex) {
    this.#runs = runs;
    this.matchState = matchState;
    this.#handle = handle;
    this.filterIndex = fieldIndex + 2; // after date and tag filters
    this.#active = $state(false);
    this.#valuesActive = $state({}); // attrValueHandle => active

    this.#valuesProxy = new Proxy(this.#valuesActive, {
      set: (target, prop, value) => {
        // console.log(`attr.values.set: ${target}, ${prop}, ${value}`);
        target[prop] = value;
        this.#update();
        return true;
      },
      get: (target, prop) => {
        return target[prop];
      },
      deleteProperty: (target, prop) => {
        // console.log(`attr.values.delete: ${target}, ${prop}`);
        let ret = false;
        if (prop in target) {
          ret = true;
          delete target[prop];
        }
        this.#update();
        return ret;
      }
    });
  }

  get active() {
    return this.#active;
  }

  set active(isActive) {
    this.#active = isActive;
    this.#update();
  }

  get values() {
    return this.#valuesProxy;
  }

  #update() {
    for (let r = 0; r < this.#runs.length; r++) {
      const match = this.match(this.#runs[r]);
      this.matchState.update(r, this.filterIndex, match);
    }
    this.matchState.signalUpdated();
  }

  match(run) {
    if (!(this.#handle in run.attrVals)) { return -1; }
    if (! this.#active) { return 1; }
    const val = run.attrVals[this.#handle];
    return +this.values[val.handle];
  }

}


export { UIAttrState }

