/* Represents the global state of the UI representing tag filtering */
class UITagState {
  #runs;
  #filterIndex;
  #matchAll;
  #tagMap;
  #tagMapProxy;

  constructor(runs, matchState) {
    this.#runs = runs;
    this.matchState = matchState;
    this.#filterIndex = matchState.tagIndex;
    this.#matchAll = $state(false);
    this.#tagMap = $state({});
    this.#tagMapProxy = new Proxy(this.#tagMap, {
      set: (target, prop, value) => {
        target[prop] = value;
        this.#update();
        return true;
      },
      get: (target, prop) => {
        return target[prop];
      },
      deleteProperty: (target, prop) => {
        console.log(`In deleteProperty: ${target} ${prop}`);
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

  get matchAll() {
    return this.#matchAll;
  }

  set matchAll(val) {
    this.#matchAll = val;
    this.#update();
  }

  get tagMap() {
    return this.#tagMapProxy;
  }

  get activeTags() {
    return Object.entries(this.#tagMap).filter(([tag, active]) => active).map(p => p[0]);
  }

  #update() {
    // console.log(`tag.udpate: ${this.#runs}`);
    for (let r = 0; r < this.#runs.length; r++) {
      const match = this.match(this.#runs[r]);
      this.matchState.update(r, this.#filterIndex, match);
    }
    this.matchState.signalUpdated();
  }

  match(run) {
    // see query.sql filter_by_tags
    const tagSet = new Set(this.activeTags);
    // console.log(`tag.match: run.tags: ${run.tags}, tagMap keys: ${Object.keys(this.#tagMap)}, activeTags: ${Array.from(tagSet)}`);
    if (tagSet.size == 0) {
      return 1;
    } else if (this.#matchAll) {
      return +run.tags.isSupersetOf(tagSet);
    } else {
      return +(run.tags.intersection(tagSet).size > 0);
    }
  }
}

export { UITagState }

