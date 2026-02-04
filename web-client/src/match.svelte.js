class MatchState {
  #version;

  constructor() {
    this.#version = $state(0);
    this.nRuns = 0;
    this.nFilters = 0;
    this.dateIndex = 0;
    this.tagIndex = 1;

    /* a Int8Array representing a [run, filter] matrix.  
     * 0 = no match (run is excluded)
     * 1 = match (run is included) */
    this.matrix = new Int8Array(0);
      
    // filterCount[filterIdx] = number of runs included by this filter
    this.filterCount = [];

    // runCount[runIdx] = count of number of filters excluded it (0 means included)
    this.runCount = [];
  }

  get(run, filter) {
    return this.matrix[run * this.nFilters + filter];
  }

  update(run, filter, currVal) {
    // console.log(`MatchState.update(${run}, ${filter}, ${currVal})`);
    const index = run * this.nFilters + filter;
    const prevVal = this.matrix[index];
    this.matrix[index] = currVal;
    if (prevVal === 0 && currVal === 1) {
      this.filterCount[filter] += 1;
      this.runCount[run] -= 1;
    } else if (prevVal === 1 && currVal === 0) {
      this.filterCount[filter] -= 1;
      this.runCount[run] += 1;
    }
    /*
    console.log(
      `match.update:  run: ${run}, filter: ${filter}, currVal: ${currVal}` +
      `runCount: ${this.runCount[run]}, filterCount: ${this.filterCount[filter]}`);
    */
  }

  set(run, filter, value) {
    this.matrix[run * this.nFilters + filter] = value;
  }

  tally() {
    this.runCount.fill(0);
    this.filterCount.fill(0);

    let idx = 0;
    for (let run = 0; run != this.nRuns; run++) {
      for (let filter = 0; filter != this.nFilters; filter++) {
        const value = this.matrix[idx];
        if (value === 0) { // not matched
          this.runCount[run] += 1;
        } else if (value === 1) {
          this.filterCount[filter] += 1;
        }
        idx++;
      }
    }
  }

  resize(nRuns, nFilters) {
    this.nRuns = nRuns;
    this.nFilters = nFilters;
    this.matrix = new Int8Array(nRuns * nFilters);
    this.filterCount = new Array(nFilters);
    this.runCount = new Array(nRuns);
  }

  clear() {
    this.matrix.fill(0);
    this.filterCount.fill(0);
    this.runCount.fill(0);
  }

  signalUpdated() {
    this.#version = (this.#version + 1) % 100000;
  }

  get version() {
    return this.#version;
  }

}

export { MatchState }

