// defines getter and setter for 
class UIDateState {
  #minRunIndex; // minimum run index included by this filter
  #maxRunIndex; // maximum run index included by this filter
  #filterIndex;

  constructor(matchState) {
    this.matchState = matchState;
    this.#filterIndex = matchState.dateIndex;
    this.#minRunIndex = $state(0);
    this.#maxRunIndex = $state(0);
  }

  get min() {
    return this.#minRunIndex;
  }

  set min(newMin) {
    const prev = this.#minRunIndex;
    this.#minRunIndex = newMin;
    this.#update(prev, newMin, true);
  }

  get max() {
    return this.#maxRunIndex;
  }

  set max(newMax) {
    const prev = this.#maxRunIndex;
    this.#maxRunIndex = newMax;
    this.#update(prev, newMax, false);
  }

  #update(prev, curr, isLeft) {
    if (prev === curr) { return; }
    let beg, end, value;

    if (prev < curr) {
      beg = prev;
      end = curr;
      value = isLeft ? 0 : 1;
    } else {
      beg = curr;
      end = prev;
      value = isLeft ? 1 : 0;
    }

    // console.log(`date.update(${prev}, ${curr}, ${isLeft}): beg=${beg}, end=${end}, value=${value}`);
    for (let r = beg; r !== end; r++) {
      this.matchState.update(r, this.#filterIndex, value);
    }
    this.matchState.signalUpdated();
  }

  match(run) {
    // console.log(`date.match: min=${this.min}, max=${this.max}, run.index=${run.index}`);
    return +(run.index >= this.min && run.index < this.max);
  }

}


export { UIDateState }

