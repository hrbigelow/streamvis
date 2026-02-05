import { create } from '@bufbuild/protobuf';
import { UIAttrState } from './attr.svelte.js';
import { MatchState } from './match.svelte.js';
import { UIDateState } from './date.svelte.js';
import { UITagState } from './tag.svelte.js';

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

// from https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
function fnvHash(msg) {
  const FNV_OFFSET_BASIS = 14695981039346656037n;
  const FNV_PRIME = 1099511628211n;

  let hash = FNV_OFFSET_BASIS;
  for (let i = 0; i != msg.length; i++) {
    hash ^= BigInt(msg.charCodeAt(i));
    hash = BigInt.asUintN(64, hash * FNV_PRIME);
  }
  return hash.toString(16).padStart(16, '0');
}

class AttrValue {
  constructor(val) {
    this.handle = fnvHash(`${typeof val}:${val}`);
    this.value = val;
  }
}

class Run {
  constructor(msg, index) {
    this.index = index; 
    this.handle = msg.handle;
    this.tags = new Set(msg.tags);
    this.startedAt = timestampToDate(msg.startedAt);
    this.attrVals = Object.fromEntries(msg.attrs.map(m => [m.handle, new AttrValue(m.value.value)])); // handle 
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
    this.matchState.version;
    // console.log(`numFilteredRuns.derived: ${this.matchState.runCount}`);
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

  filteredAttributes = $derived.by(() => {
    this.matchState.version;
    const attrCounts = {} // attrHandle => (attrValue => numRuns)
    if (this.selectedSeries === null) { return attrCounts; }
    for (let r = 0; r != this.runs.length; r++) {
      if (this.matchState.get(r, this.matchState.dateIndex) === 1
        && this.matchState.get(r, this.matchState.tagIndex) === 1) {
        const run = this.runs[r];
        for (const attrHandle in run.attrVals) {
          if (!(attrHandle in attrCounts)) {
            attrCounts[attrHandle] = [];
          }
          const valMap = attrCounts[attrHandle];
          const attrVal = run.attrVals[attrHandle];
          if (!(attrVal.handle in valMap)) {
            valMap[attrVal.handle] = [attrVal.value, 0];
          }
          valMap[attrVal.handle][1] += 1;
        }
      }
    }
    return attrCounts;
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
    this.runs.length = 0;
    this.runs.push(...runs);
    // console.log(`fetchRuns: received runs: ${this.runs.length}`);
    // this.runs = runs;
  }

  async fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const newFields = {};
    let fieldIndex = 0;
    for await (const field of this.client.listFields(req)) {
      newFields[field.handle] = new Field(field, fieldIndex);
      fieldIndex++;
    }
    this.fields = newFields;
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
      const newValHandles = new Set();
      for (const run of this.runs) {
        for (const attrHandle in run.attrVals) {
          newValHandles.add(run.attrVals[attrHandle].handle);
        }
      }
      const attrState = this.uiState.attrs[key];
      for (const valHandle in attrState.values) {
        if (! newValHandles.has(valHandle)) {
          delete attrState[valHandle];
        }
      }
      for (const valHandle of newValHandles) {
        attrState[valHandle] = false;
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
    
    // date range
    for (let r = 0; r < this.runs.length; r++) {
      const match = this.uiState.dates.match(this.runs[r]);
      // console.log(`matchState.set(${r}, ${this.matchState.dateIndex}, ${match})`);
      // console.log(`uiState.dates: ${this.uiState.dates.min}, ${this.uiState.dates.max}`);
      this.matchState.set(r, this.matchState.dateIndex, match);
    }

    // tags
    for (let r = 0; r < this.runs.length; r++) {
      const match = this.uiState.tags.match(this.runs[r]);
      this.matchState.set(r, this.matchState.tagIndex, match);
    }

    // attribute-based filters 
    for (const handle in this.fields) {
      const uiAttr = this.uiState.attrs[handle];
      for (let r = 0; r < this.runs.length; r++) {
        const match = uiAttr.match(this.runs[r]);
        this.matchState.set(r, uiAttr.filterIndex, match);
      }
    }
    this.matchState.tally();
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

}

export { StateManager }

