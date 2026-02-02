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
  constructor(msg) {
    this.handle = msg.handle;
    this.tags = new Set(msg.tags);
    this.startedAt = timestampToDate(msg.startedAt);
    this.attrs = Object.fromEntries(msg.attrs.map(m => [m.handle, m]));
    this.seriesHandles = msg.seriesHandles;
  }
}

class TagFilter {
  constructor(tags, matchAll) {
    this.tags = new Set(tags);
    this.matchAll = matchAll;
  }

  match(run) {
    // see query.sql filter_by_tags
    if (this.tags.size == 0) {
      return true;
    } else if (this.matchAll) {
      return run.tags.isSupersetOf(this.tags);
    } else {
      return run.tags.intersection(this.tags).size > 0;
    }
  }
}

class DateFilter {
  constructor(minDate, maxDate) {
    this.minDate = minDate;
    this.maxDate = maxDate;
  }

  match(run) {
    return (
      run.startedAt >= this.minDate
      && run.startedAt > this.maxDate
    );
  }
}

// defines getter and setter for 
class UIDateState {
  #minRunIndex; // minimum run index included by this filter
  #maxRunIndex; // maximum run index included by this filter
  #filterIndex = 0;

  constructor(matchState, filterIndex) {
    this.matchState = matchState;
    this.#filterIndex = filterIndex;
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
    if (beg < end) {
      beg = prev;
      end = curr;
      delta = isLeft ? -1 : 1;
    } else {
      beg = curr;
      end = prev;
      delta = isLeft ? 1 : -1;
    }
    for (let r = beg; r != end; r++) {
      this.matchState.matrix[r * nfilters + this.#filterIndex] += delta;
      this.matchState.runCount[r] += delta;
    }
  }

}

class UITagState {
  #filterIndex;
  #matchAll;
  #tags;
  #tagsProxy;

  constructor(matchState, filterIndex) {
    this.matchState = matchState;
    this.#filterIndex = filterIndex;
    this.#matchAll = $state(false);
    this.#tags = $state({});
    this.#tagsProxy = new Proxy(this.#tags, {
      set: (target, prop, value) => {
        target[prop] = value;
        this.#update();
      },
      get: (target, prop) => {
        return target[prop];
      }
    }
  }

  get matchAll() {
    return this.#matchAll;
  }

  set matchAll(val) {
    this.matchAll = val;
    this.#update();
  }

  get tags() {
    return this.#tagsProxy;
  }

  #update() {
    const selectedTags = [];
    for (const tag in tags) {
      if (tags[tag]) {
        selectedTags.push(tag);
      }
    }
    const tagFilter = new TagFilter(selectedTags, matchall);
    for (let r = 0; r < this.runs.length; r++) {
      const match = tagFilter.match(this.runs[r]);
      this.matchState.matrix[r * nFilters + this.#filterIndex] = match; 
      if (match) {
        this.matchState.filterCount[this.tagFilterIndex] += 1;
      } else {
        this.matchState.runCount[r] += 1;
      }
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
    this.#active = false;
    this.#values = {};

    this.valuesProxy = new Proxy(#this.values, {
      set: (target, prop, value) => {
        target[prop] = value;
        this.#update();
      },
      get: (target, prop) => {
        return target[prop];
      }
    }
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
      const attrVal = this.#runs[r].attrs[this.#handle];
      if (attrVal === undefined) { continue; }
      const match = (!this.#active || this.#values[attrVal]);
      this.matchState.matrix[r * nFilters + this.#index] = match;
      if (match) {
        this.matchState.filterCount[this.#index] += 1;
      } else {
        this.matchState.runCount[r] += 1;
      }
    }
  }
}


class RunFilter {
  constructor(tags, matchAllTags, minStartedAt, maxStartedAt, attributeFilters) {
    this.attributeFilters = attributeFilters;
    this.tagFilter = { 
      tags: new Set(tags),
      matchAll: matchAllTags
    }
    this.minStartedAt = minStartedAt;
    this.maxStartedAt = maxStartedAt;
  }
  
  _tagFilter(run) {
    // see query.sql filter_by_tags
    if (this.tagFilter.tags.size == 0) {
      return true;
    } else if (this.tagFilter.matchAll) {
      return run.tags.isSupersetOf(this.tagFilter.tags);
    } else {
      return run.tags.intersection(this.tagFilter.tags).size > 0;
    }
  }

  filter(run) {
    if (run.startedAt < this.minStartedAt
      || run.startedAt > this.maxStartedAt) {
      return false;
    } else if (! this._tagFilter(run)) {
      return false;
    } else {
      // TODO attributeFilters
      return true;
    }
  }
}

class StateManager {
  runs = $state([]);
  fields = $state({});
  series = $state({});

  tags = $derived.by(() => {
    let _tags = new Set();
    for (const run of this.runs) {
      _tags = run.tags.union(_tags);
    }
    return Array.from(_tags);
  });

  startTimes = $derived.by(() => {
    return this.runs.map(r => r.startedAt)
  });

  constructor(client, uiState) {
    this.client = client;
    this.uiState = uiState;

    this.matchState = {
      /* a Int8Array representing a [run, filter] matrix.  
       * 0 = no match (run is excluded)
       * 1 = match (run is included) */
      matrix: null,
      
      // filterCount[filterIdx] = number of runs included by this filter
      filterCount: null,

      // runCount[runIdx] = count of number of filters excluded it (0 means included)
      runCount: null
    }

    this.uiState.tags = new UITagState(this.matchState, 0);
    this.uiState.dates = new UIDateState(this.matchState, 1);
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
    for await (const msg of this.client.listRuns(req)) {
      const run = new Run(msg);
      runs.push(run);
    }
    this.runs = runs;
  }

  async fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const newFields = {};
    for await (const field of this.client.listFields(req)) {
      newFields[field.handle] = field;
    }
    for (const key in this.fields) {
      if (!(key in newFields)) {
        delete this.fields[key];
        delete this.uiState[key];
      }
    }
    Object.assign(this.fields, newFields);
  }

  syncUiState() {
    // delete unbacked attributes
    for (const key in this.uiState.attrs) {
      if (!(key in this.fields)) {
        delete this.uiState.attrs[key];
      }
    }
    // create missing attributes
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
      const valFilter = this.uiState.attrs[key];
      for (const val in valFilter) {
        if (!(val in newVals)) {
          delete valFilter[val]; // this will be rare
        }
      }
      for (const val in newVals) {
        if (!(val in valFilter)) {
          valFilter[val] = false;
        }
      }
    }

    this.syncTagState();

    // date ranges
    this.uiState.dates.min = 0;
    this.uiState.dates.max = this.startTimes.length - 1;
  }

  syncTagState() {
    // tags
    const runTagSet = new Set(this.tags);
    for (const tag in this.uiState.tags) {
      if (!(tag in runTagSet)) {
        delete this.uiState.tags[tag]; // this will be rare
      }
    }
    for (const tag in this.tags) {
      if (!(tag in this.uiState.tags)) {
        this.uiState.tags[tag] = new UITagFilter(this.matchState);
      }
    }
  }

  /* Called when runs and fields are refreshed, to synchronize
     this.matchState and the two summary statistics for it (runNonMatchCount and
     filterMatchCount).  
   */
  fullUpdateMatch() {
    const nFilters = this.fields.length + 2;
    const nCells = this.runs.length * nFilters;
    if (this.filterState === null || this.filterState.length != nCells) {
      this.matchState = new Int8Array(nCells); // 0 = no match, 1 = match, -1 = missing attr
      this.runNonMatchCount = Array(this.runs.length);
      this.filterMatchCount = Array(nFilters);
    }
    this.runExcludedCount.fill(0);
    this.filtereIncludedCount.fill(0);

    // tags
    this.updateTagMatch(this.uiState.tags, this.uiState.matchAll);

    // date range
    const dateFilter = new DateFilter(this.uiState.date.min, this.uiState.date.max);
    for (let r = 0; r < this.runs.length; r++) {
      const match = dateFilter.match(this.runs[r]);
      this.matchState[r * nFilters + this.dateFilterIndex] = match; 
      if (match) {
        this.filterMatchCount[this.dateFilterIndex] += 1;
      } else {
        this.runNonMatchCount[r] += 1;
      }
    }

    // attribute-based filters 
    for (let r = 0; r < this.runs.length; r++) {
      const attrVals = this.runs[r].attrs;
      for (const fieldHandle in attrVals) {
        const filter = this.uiState.attrs[fieldHandle];
        this.updateActiveAttr(filter, fieldHandle, attrVals[fieldHandle]);
      }
    }
  }

  // call for both partial and full

  updateActiveAttrMatch(uiAttrState, fieldHandle, attrVal) {
    const match = (!uiAttrState.active || uiAttrState.vals[attrVal]);
    const filterIndex = this.fields[fieldHandle].index; + 2; // first two are tags and date
    this.matchState[r * nFilters + filterIndex] = match;
    if (match) {
      this.filterMatchCount[filterIndex] += 1;
    } else {
      this.runNonMatchCount[r] += 1;
    }
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

  async fetchAll() {
    await Promise.all([
      this.fetchRuns(),
      this.fetchSeries(),
      this.fetchFields()
    ]);
  }

  /* Return a map of tag => numRuns.
  */
  filteredTags(minStartedAt, maxStartedAt, selectedSeries, selectedTags) {
    const visibleTags = {} // tag => numRuns
    if (selectedSeries === null) {
      return visibleTags;
    }
    for (const run of this.runs) {
      if (run.startedAt >= minStartedAt 
        && run.startedAt <= maxStartedAt
        && run.seriesHandles.includes(selectedSeries)
      ) {
        for (const tag of run.tags) {
          visibleTags[tag] = (visibleTags[tag] || 0) + 1;
        }
      } else {
        for (const tag of run.tags) {
          if (selectedTags.has(tag)) {
            visibleTags[tag] = (visibleTags[tag] || 0);
          }
        }
      }
    }
    return visibleTags;
  }

  countFilteredRuns(runFilter, selectedSeries) {
    let numFilteredRuns = 0;
    for (const run of this.runs) {
      if (runFilter.filter(run) && run.seriesHandles.includes(selectedSeries)) {
        numFilteredRuns += 1;
      }
    }
    return numFilteredRuns;
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

export { StateManager, RunFilter }

