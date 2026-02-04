class UIAttrState {
  #runs;   // the full catalog of runs under this filter's control
  #handle; // the field handle of the attribute this filter represents
  #active; // whether this overall filter is active
  #values; // value => bool
  #valuesProxy; 
  
  constructor(runs, matchState, handle, fieldIndex) {
    this.#runs = runs;
    this.matchState = matchState;
    this.#handle = handle;
    this.filterIndex = fieldIndex + 2; // after date and tag filters
    this.#active = $state(false);
    this.#values = $state({});

    this.#valuesProxy = new Proxy(this.#values, {
      set: (target, prop, value) => {
        target[prop] = value;
        this.#update();
        return true;
      },
      get: (target, prop) => {
        return target[prop];
      },
      deleteProperty: (target, prop) => {
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
    if (!(this.#handle in run.attrs)) { return -1; }
    if (! this.#active) { return 1; }
    const val = run.attrs[this.handle];
    return +this.values[val];
  }

}


export { UIAttrState }

