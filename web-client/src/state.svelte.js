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

class StateManager {
  allFields = $state({});
  allSeries = $state({});
  allRuns = $state([]);
  allTags = $state([]);

  constructor(client) {
    this.client = client;
  }

  seriesStartTimes(seriesHandle) {
    const starts = [];
    for (const run of this.allRuns) {
      if (run.seriesHandles.includes(seriesHandle)) {
        starts.push(run.startedAt);
      }
    }
    return starts;
  }

  async fetchRuns() {
    const emptyFilter = {
      attributeFilters: [],
      tagFilter: {
        tags: [],
        matchAny: true
      },
      minStartedAt: undefined,
      maxStartedAt: undefined 
    }
    const req = create(ListRunsRequestSchema, { runFilter: emptyFilter })
    const runs = [];
    const tags = new Set();
    for await (const run of this.client.listRuns(req)) {
      run.startedAt = timestampToDate(run.startedAt);
      runs.push(run);
      tags.add(run.tags);
    }
    this.allRuns = runs;
    this.allTags = Array.from(tags);
  }

  async fetchSeries() {
    const req = create(ListSeriesRequestSchema, {});
    const newSeries = {};
    for await (const s of this.client.listSeries(req)) {
      newSeries[s.handle] = s;
    }
    for (const key in this.allSeries) {
      if (!(key in newSeries)) {
        delete this.allSeries[key];
      }
    }
    Object.assign(this.allSeries, newSeries);
  }

  async fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const fields = {};
    for await (const field of this.client.listFields(req)) {
      fields[field.handle] = field;
    }
    this.allFields = fields;
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
    for (const run of this.allRuns) {
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
}

export { StateManager }

