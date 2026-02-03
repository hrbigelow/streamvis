import { create } from '@bufbuild/protobuf';

import {
  ListFieldsRequestSchema,
  ListSeriesRequestSchema,
  ListCommonAttributesRequestSchema,
  ListCommonSeriesRequestSchema,
  ListRunsRequestSchema,
} from './gen/streamvis/v1/data_pb.js';


function timestampToDate(timestamp) {
  const milliseconds = BigInt(timestamp.seconds) * 1000n + BigInt(timestamp.nanos) / 1000000n;
  return new Date(Number(milliseconds));
}

class Run {
  constructor(msg, index) {
    this.index = index; 
    this.handle = msg.handle;
    this.tags = new Set(msg.tags);
    this.startedAt = timestampToDate(msg.startedAt);
    this.attrs = Object.fromEntries(msg.attrs.map(m => [m.handle, m]));
    this.seriesHandles = msg.seriesHandles;
  }
}

class Field {
  constructor(msg, index) {
    this.index = index;
    this.handle = msg.handle;
    this.name = msg.name;
    this.dataType = msg.dataType;
    this.description = msg.description;
  }
}


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

  set(run, filter, value) {
    console.log(`MatchState::set(${run}, ${filter}, ${value})`);
    this.matrix[run * this.nFilters + filter] = value;
    switch (value) {
      case 1:
        this.filterCount[filter] += 1;
        break;
      case 0:
        this.runCount[run] += 1;
        break;
      case -1:
        break;
    }
  }

  add(run, filter, value) {
    console.log(`MatchState::add(${run}, ${filter}, ${value})`);
    this.matrix[run * this.nFilters + filter] += value;
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

    let beg, end, delta;
    if (prev < curr) {
      beg = prev;
      end = curr;
      delta = isLeft ? -1 : 1;
    } else {
      beg = curr;
      end = prev;
      delta = isLeft ? 1 : -1;
    }
    for (let r = beg; r != end; r++) {
      this.matchState.add(r, this.#filterIndex, delta);
      this.matchState.runCount[r] += delta;
    }
    this.matchState.signalUpdated();
  }

  match(run) {
    return +(
      run.index >= this.minRunIndex
      && run.index > this.maxRunIndex
    );
  }

}

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

  get activetags() {
    return Object.entries(this.#tagMap).filter(([tag, active]) => active).keys().toArray();
  }

  #update() {
    for (let r = 0; r < this.#runs.length; r++) {
      const match = this.match(this.#runs[r]);
      this.matchState.set(r, this.#filterIndex, match);
    }
    this.matchState.signalUpdated();
  }

  match(run) {
    // see query.sql filter_by_tags
    const tagSet = new Set(this.activeTags);
    if (tagSet.size == 0) {
      return 1;
    } else if (this.#matchAll) {
      return +run.tags.isSupersetOf(tagSet);
    } else {
      return +(run.tags.intersection(tagSet).size > 0);
    }
  }
}


class UIAttrState {
  #runs;   // the full catalog of runs under this filter's control
  #handle; // the field handle of the attribute this filter represents
  #index;  // the index into the matchState.matrix representing this filter's match state 
  #active; // whether this overall filter is active
  #values; // value => bool
  #valuesProxy; 
  
  constructor(runs, matchState, handle, index) {
    this.#runs = runs;
    this.matchState = matchState;
    this.#handle = handle;
    this.#index = index;
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
      this.matchState.set(r, this.#index, match);
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


class StateManager {
  runs = $state([]);
  fields = $state({}); // handle -> field (proto message) 
  series = $state({});
  selectedSeries = $state(null);

  tags = $derived.by(() => {
    let _tags = new Set();
    for (const run of this.runs) {
      _tags = run.tags.union(_tags);
    }
    return Array.from(_tags);
  });

  startTimes = $derived.by(() => {
    const times = this.runs.map(r => r.startedAt);
    times.push(new Date());
    return times;
  });

  numFilteredRuns = $derived.by(() => {
    return this.matchState.runCount.filter(ct => ct === 0).length;
  });

  // tags appearing in any run filtered by date and series
  filteredTags = $derived.by(() => {
    this.matchState.version; // access for reactivity
    const tagCounts = {} // tag => numRuns
    if (this.selectedSeries === null) { return tagCounts; }
    for (let r = 0; r != this.runs.length; r++) {
      const run = this.runs[r];
      if (this.matchState.get(r, this.matchState.dateIndex) === 1) {
        for (const tag of run.tags) {
          tagCounts[tag] = (tagCounts[tag] || 0) + 1;
        }
      } else {
        for (const tag of run.tags) {
          if (this.uiState.tags.tagMap[tag]) {
            tagCounts[tag] = (tagCounts[tag] || 0);
          }
        }
      }
    }
    return tagCounts;
  });

  constructor(client, uiState) {
    this.client = client;
    this.uiState = uiState;
    this.matchState = new MatchState();
    this.uiState.tags = new UITagState(this.runs, this.matchState);
    this.uiState.dates = new UIDateState(this.matchState);
    this.uiState.attrs = {}; // handle => UIAttrState
  }

  async fetchRuns() {
    const emptyFilter = {
      attributeFilters: [],
      tagFilter: {
        tags: [],
        matchAll: false
      },
      minStartedAt: undefined,
      maxStartedAt: undefined 
    }
    const runs = [];
    const req = create(ListRunsRequestSchema, { runFilter: emptyFilter })
    let runIndex = 0;
    for await (const msg of this.client.listRuns(req)) {
      const run = new Run(msg, runIndex);
      runs.push(run);
      runIndex++;
    }
    this.runs = runs;
  }

  async fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const newFields = {};
    let fieldIndex = 0;
    for await (const field of this.client.listFields(req)) {
      newFields[field.handle] = new Field(field, fieldIndex);
      fieldIndex++;
    }

    for (const key in this.fields) {
      if (!(key in newFields)) {
        delete this.fields[key];
      }
    }
    Object.assign(this.fields, newFields);
  }

  /* synchronize uiState after fields and runs are refreshed */
  syncUiState() {
    this.syncAttrState();
    this.syncTagState();
    this.syncDateState();
  }

  syncAttrState() {
    for (const key in this.uiState.attrs) {
      if (!(key in this.fields)) {
        delete this.uiState.attrs[key];
      }
    }
    for (const key in this.fields) {
      if (!(key in this.uiState.attrs)) {
        this.uiState.attrs[key] = new UIAttrState(
          this.runs, this.matchState, key, this.fields[key].index
        )
      }
    }
    // get distinct values for each attribute
    for (const key in this.uiState.attrs) {
      const newVals = new Set();
      for (const run of this.runs) {
        if (key in run.attrs) {
          newVals.add(run.attrs[key]);
        }
      }
      const attrState = this.uiState.attrs[key];
      for (const val in attrState.values) {
        if (! newVals.has(val)) {
          delete attrState.values[val]; // this will be rare
        }
      }
      for (const val of newVals) {
        if (!(val in attrState.values)) {
          attrState.values[val] = false;
        }
      }
    }
  }

  syncTagState() {
    const runTagSet = new Set(this.tags);
    for (const tag in this.uiState.tags.tagMap) {
      if (! runTagSet.has(tag)) {
        delete this.uiState.tags.tagMap[tag]; // this will be rare
      }
    }
    for (const tag of this.tags) {
      if (!(tag in this.uiState.tags.tagMap)) {
        this.uiState.tags.tagMap[tag] = false;
      }
    }
  }

  syncDateState() {
    const uiDate = this.uiState.dates;
    if (uiDate.min >= this.startTimes.length) {
      uiDate.min = this.startTimes.length - 1;
    }
    if (uiDate.max >= this.startTimes.length) {
      uiDate.max = this.startTimes.length - 1;
    }
  }

  /* synchronizes matchState to any new values for runs and fields, using current
    * settings of uiState. 
    * */
  syncMatchState() {
    const nFilters = Object.keys(this.fields).length + 2;
    const nRuns = this.runs.length;
    if (this.matchState.nRuns != nRuns || this.matchState.nFilters !== nFilters) {
      this.matchState.resize(nRuns, nFilters);
    }
    this.matchState.clear();

    // tags
    for (let r = 0; r < this.runs.length; r++) {
      const match = this.uiState.tags.match(this.runs[r]);
      this.matchState.set(r, this.matchState.tagIndex, match);
    }
    
    // date range
    for (let r = 0; r < this.runs.length; r++) {
      const match = this.uiState.dates.match(this.runs[r]);
      this.matchState.set(r, this.matchState.dateIndex, match);
    }

    // attribute-based filters 
    for (const handle in this.fields) {
      const uiAttr = this.uiState.attrs[handle];
      for (let r = 0; r < this.runs.length; r++) {
        const match = uiAttr.match(this.runs[r]);
        this.matchState.set(r, uiAttr.index + 2, match);
      }
    }
    this.matchState.signalUpdated();
  }

  async fetchSeries() {
    const req = create(ListSeriesRequestSchema, {});
    const newSeries = {};
    for await (const s of this.client.listSeries(req)) {
      newSeries[s.handle] = s;
    }
    for (const key in this.series) {
      if (!(key in newSeries)) {
        delete this.series[key];
      }
    }
    Object.assign(this.series, newSeries);
  }

  async refresh() {
    await Promise.all([
      this.fetchRuns(),
      this.fetchSeries(),
      this.fetchFields()
    ]);
    this.syncUiState();
    this.syncMatchState();
  }


  filteredAttributes(minStartedAt, maxStartedAt, selectedSeries, selectedTags, matchAllTags) {
    runFilter = new RunFilter(selectedTags, matchAllTags, minStartedAt, maxStartedAt, []);
    const commonAttributes = {}; // fieldHandle => Set(fieldValue, ...)
    for (const run of this.runs) {
      if (! runFilter.filter(run) || !run.seriesHandles.includes(selectedSeries)) {
        continue;
      }

    }
  }
}

export { StateManager }

