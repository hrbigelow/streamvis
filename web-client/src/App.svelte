<script>
  import { onMount } from 'svelte';
  import { getServiceClient } from './clientUtil.js';
  import { create } from '@bufbuild/protobuf';
  import {
    ListFieldsRequestSchema,
    ListSeriesRequestSchema,
    ListCommonAttributesRequestSchema,
    ListCommonSeriesRequestSchema,
    ListRunsRequestSchema,
    ListAttributeValuesRequestSchema,
  } from './gen/streamvis/v1/data_pb.js';
  let { streamvisUrl } = $props();

  let allFields = $state({});
  let allSeries = $state({});
  let allRuns = $state([]);
  let allTags = $state([]);
  let allStartTimes = $state([]);
  let allAttributeValues = $state({});

  let commonSeries = $state({}); // handle -> pb.Series
  let commonAttributes = $state([]);
  let includedRuns = $state([]);
  let selectedSeries = $state(null);
  let plotType = $state(null);
  let axisBindings = $state({
    x: null,
    y: null,
    order: null,
    color: null,
    group: null,
  });

  let runFilter = $state({
    attributeFilters: [],
    tagFilter: {
      tags: [],
      matchAny: true
    },
    minStartedAt: undefined,
    maxStartedAt: undefined 
  });
  let minStartedAtIndex = $state(0);
  let maxStartedAtIndex = $state(200);

  let filterUI = $state({
    attributeHandles: [null, null, null, null, null]
  });

  $effect(() => {
    runFilter.attributeFilters = [];
  });

  $effect(() => {
    if (minStartedAtIndex > maxStartedAtIndex) {
      maxStartedAtIndex = minStartedAtIndex;
    }
  });

  $effect(() => {
    if (maxStartedAtIndex < minStartedAtIndex) {
      minStartedAtIndex = maxStartedAtIndex;
    }
  });

  const axisNames = new Map();
  axisNames.set('x', 'X axis');
  axisNames.set('y', 'Y axis');
  axisNames.set('group', 'Group axis');
  axisNames.set('color', 'Color axis');
  axisNames.set('order', 'Order axis');

  let visibleFields = $state([]);
  let plotTitle = $state('');
  let xAxisLabel = $state('');
  let yAxisLabel = $state('');
  let axesMode = $state('lin');
  let glyphKind = $state('line');
  let selectedFilters = $state(new Set());
  let selectedGroups = $state(new Set());
  let windowSize = $state(null);
  let stride = $state(null);
  let link = $derived.by(computeLink);

  let client;
  let mounted = false;

  function timestampToDate(timestamp) {
    const milliseconds = BigInt(timestamp.seconds) * 1000n + BigInt(timestamp.nanos) / 1000000n;
    return new Date(Number(milliseconds));
  }

  async function fetchRuns() {
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
    const startTimes = [];
    for await (const run of client.listRuns(req)) {
      runs.push(run);
      tags.add(run.tags);
      startTimes.push(timestampToDate(run.startedAt));
    }
    allRuns = runs;
    allTags = Array.from(tags);
    allStartTimes = startTimes;
  }

  async function fetchSeries() {
    const req = create(ListSeriesRequestSchema, {});
    const series = [];
    for await (const resp of client.listSeries(req)) {
      series.push(resp);
    }
    allSeries = series;
  }

  async function fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const fields = {};
    for await (const field of client.listFields(req)) {
      fields[field.handle] = field;
    }
    allFields = fields;
  }

  async function fetchAll() {
    await fetchRuns();
    await fetchSeries();
    await fetchFields();
  }

  async function fetchAttributeValues() {
    const req = create(ListAttributeValuesRequestSchema, {});
    const vals = {};
    for await (const resp of client.listAttributeValues(req)) {
      vals[resp.field.handle] = resp
    }
    allAttributeValues = vals;
  }

  async function fetchCommonSeries() {
    const req = create(ListCommonSeriesRequestSchema, { runFilter: runFilter });
    const series = {};
    for await (const resp of client.listCommonSeries(req)) {
      series[resp.handle] = resp;
    }
    commonSeries = series;
    console.log(`fetchCommonSeries with ${Object.keys(series).length}`);
  }

  async function fetchCommonAttributes() {
    const req = create(ListCommonAttributesRequestSchema, { runFilter: runFilter });
    const attrs = [];
    for await (const resp of client.listCommonAttributes(req)) {
      attrs.push(resp);
    }
    commonAttributes = attrs;
  }

  async function updateSelectedRuns() {
    const req = create(ListRunsRequestSchema, { runFilter: runFilter });
    const handles = [];
    for await (const resp of client.listRuns(req)) {
      handles.push(resp.handle);
    }
    includedRuns = handles;
  }

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async function refreshEvery(task, delay_ms) {
    while (true) {
      try {
        await task();
      } catch(err) {
        console.error(`refresh: task error: ${err}`);
      }
      await sleep(delay_ms);
    }
  }

  function computeLink() {
    const obj = {
      scopes: [...selectedScopes].filter(el => allScopes.includes(el)),
      names: [...selectedNames].filter(el => visibleNames.includes(el)),
      plotTitle,
      xAxisLabel: xAxisLabel || xAxisField,
      yAxisLabel: yAxisLabel || yAxisField,
      xAxisField,
      yAxisField,
      orderField: orderField || null,
      filters: [...selectedFilters].filter(el => visibleFields.includes(el)),
      groups: [...selectedGroups].filter(el => visibleFields.includes(el)),
      axesMode,
      glyphKind,
      window: windowSize,
      stride: stride,

    };
    const payload = encodeURIComponent(JSON.stringify(obj));
    return `${streamvisUrl}/?query=${payload}`;
  }


  onMount(() => {
    client = getServiceClient('/');
    refreshEvery(fetchAll, 10_000);
  });

  /*
  $effect(() => {
    updateSelectedRuns();
  });

  $effect(() => {
    fetchCommonSeries();
    fetchCommonAttributes();
  });

  */

</script>

<div class="filter-run-table">
  <div class="guide">Series:</div>
  <select bind:value={selectedSeries}>
    <option value={null}>(none)</option>
    {#each Object.entries(allSeries) as [handle, series]}
      <option value={handle}>{series.name}</option>
    {/each}
  </select>
  <div></div>
  {#if selectedSeries !== null}
    <div class="report">{allSeries[selectedSeries].name}</div>
    <div></div>
    <div></div>
    {#each allSeries[selectedSeries].fields as field}
      <div class="report">{field.name}</div>
      <div class="report">{field.dataType}</div>
      <div></div>
    {/each}
  {/if}

  <div class="range-slider">
    <input type="range" 
           class="min-input" 
           min="0" 
           max="1000"
           bind:value={minStartedAtIndex}
           >
    <input type="range" 
           class="max-input" 
           min="0" 
           max="1000" 
           bind:value={maxStartedAtIndex}>
    <div class="slider-track"></div>
  </div>

  <div class="guide">Filter Run:</div>
  <label class="guide">Tag List (csv)</label>
  <div></div>
  <input on:change={(e) => {
         runFilter.tagFilter.tags = e.target.value
           .split(/,\s*/)
           .filter(tag => tag.trim() !== '');
         }} 
  />
  <div></div>
  <div></div>
  <label class="guide">Require All Tags</label>
  <input type="checkbox"
         checked={!runFilter.tagFilter.matchAny}
         on:change={(e) => runFilter.tagFilter.matchAny = !e.target.checked}
  />
  <div></div>
  <label class="guide" for="started-at-after">
    Started At After:
  </label>
  <!--
  <input id="started-at-after" 
         type="range" 
               bind:value={minStartedAtIndex}
         min="-1" 
         max={includedRuns.length - 1}
         step="1"/>
  <div class="timestamp-display">
    {minStartedAtIndex === -1 ? "No minimum" : 
    new Date(allStartTimes[minStartedAtIndex]).toLocaleString()}
  </div>

  <label class="guide" for="started-at-before">
    Started At Before:
  </label>
    <input id="started-at-before" 
           type="range" 
           bind:value={maxStartedAtIndex} 
           min="0"
           max={includedRuns.length} 
           step="1"
           />
  <div class="timestamp-display">
    {maxStartedAtIndex === includedRuns.length ? "No maximum" :
    new Date(allStartTimes[maxStartedAtIndex]).toLocaleString()}
  </div>
  -->
  <label class="guide">Attribute Filters:</label>
  {#each [0, 1, 2, 3, 4] as i}
    <select bind:value={filterUI.attributeHandles[i]}>
      <option value={null}>(None)</option>
      {#each Object.entries(allAttributeValues) as [fieldHandle, attrValue]}
        <option value={fieldHandle}>
        {attrValue.field.name}
        </option>
      {/each}
    </select>
    <div></div>
    <div></div>
  {/each}
</div>


<div class="selected-runs-table">
  <div class="guide">Included Runs</div>
  <div>{includedRuns.length}</div>
</div>

<div class="plot-configuration-table">
  <label class="guide">Plot Type:</label>
  <select bind:value={plotType}>
    <option value="line">Line Plot</option>
    <option value="scatter">Scatter Plot</option>
  </select>

  <!--
  {#each axisNames.entries() as [key, val]} 
    <label class="guide" for="axis-{key}">{val}</label>
    <select id="axis-{key}" bind:value={axisBindings[key]}>
      {#each commonAttributes as attr}
        <option value={attr.handle}>
        attr:{attr.name}
        </option>
      {/each}
      {#if chosenSeriesHandle && commonSeries[chosenSeriesHandle]}
        {#each commonSeries[chosenSeriesHandle].fields as coord}
          <option value={coord.handle}>
          {coord.name}
          </option>
        {/each}
      {/if}
    </select>
  {/each}
  -->

</div>




<!--
<div class="top-table">
  <label class="guide" for="plot-title">Plot Title</label>
  <input id="plot-title" bind:value={plotTitle} />
  <div></div>
  <label class="guide" for="x-axis-label">X-Axis Label</label>
  <input id="x-axis-label" bind:value={xAxisLabel} />
  <div></div>
  <label class="guide" for="y-axis-label">Y-Axis Label</label>
  <input id="y-axis-label" bind:value={yAxisLabel} />
  <div></div>
  <label class="guide" for="window-size">Window Size</label>
  <input id="window-size" type="number" bind:value={windowSize} />
  <a href={link} target="_blank">Open Plot (new tab)</a>
  <label class="guide" for="stride">Stride</label>
  <input id="stride" type="number" bind:value={stride} />
  <div></div>
  <label class="guide" for="axes-mode">Axes Mode</label>
  <select id="axes-mode" bind:value={axesMode}>
    <option value="lin">Both Linear</option> 
    <option value="xlog">X-Axis Log Scale</option> 
    <option value="ylog">Y-Axis Log Scale</option> 
    <option value="xylog">Both Log Scale</option> 
  </select>
  <div></div>
  <label class="guide" for="glyph-kind">Glyph Kind</label>
  <select id="glyph-kind" bind:value={glyphKind}>
    <option value="line">Line Plot</option>
    <option value="scatter">Scatter Plot</option>
  </select>
  <div></div>
  <div></div>
  <button type="button" onclick={() => clearSelections()}>Clear Selections</button>
  <div></div>
</div>


<div class="table">
  <div class="header">Scopes</div>
  <div class="header">Names</div>
  <div class="header">X-axis</div>
  <div class="header">Y-axis</div>
  <div class="header">Order By</div>
  <div class="header">Filters</div>
  <div class="header">Group By</div>

  <div class="cell">
    {#each allScopes as scope}
      <div class="field-row">
        <label class="item">
          <input
              type="checkbox"
              checked={selectedScopes.has(scope)}
              onchange={() => toggleScope(scope)}
              />
          <span>{scope}</span>
        </label>
      </div>
    {/each}
  </div>

  <div class="cell">
    {#each visibleNames as name}
      <div class="field-row">
        <label class="item">
          <input
              type="checkbox"
              checked={selectedNames.has(name)}
              onchange={() => toggleName(name)}
              />
          <span>{name}</span>
        </label>
      </div>
    {/each}
  </div>

  <div class="cell">
    <select bind:value={xAxisField}>
      <option value="">Select X-Axis field</option>
    {#each visibleFields as field}
      <option value={field}>{field}</option>
    {/each}
    </select>
  </div>

  <div class="cell">
    <select bind:value={yAxisField}>
      <option value="">Select Y-Axis field</option>
    {#each visibleFields as field}
      <option value={field}>{field}</option>
    {/each}
    </select>
  </div>

  <div class="cell">
    <select bind:value={orderField}>
      <option value="">Select order field</option>
      {#each visibleFields as field}
      <option value={field}>{field}</option>
    {/each}
    </select>
  </div>

  <div class="cell">
    {#each visibleFields as field}
      <div class="field-row">
        <label class="item">
          <input
              type="checkbox"
              checked={selectedFilters.has(field)}
              onchange={() => toggleFilter(field)}
              />
          <span>{field}</span>
        </label>
      </div>
    {/each}
  </div>

  <div class="cell">
    {#each visibleFields as field}
      <div class="field-row">
        <label class="item">
          <input
              type="checkbox"
              checked={selectedGroups.has(field)}
              onchange={() => toggleGroup(field)}
              />
          <span>{field}</span>
        </label>
      </div>
    {/each}
  </div>

</div>

-->

<style>

  .filter-run-table {
    display: grid;
    grid-template-rows: repeat(5, min-content);
    /* grid-template-columns: 25% 15% 15% 15% 15% 15%; */
    grid-template-columns: repeat(3, min-content);
    gap: 1ch 5ch;
    width 100vw;
    box-sizing: border-box;
  }

  .selected-runs-table {
    display: grid;
    grid-template-rows: repeat(2, min-content);
    /* grid-template-columns: 25% 15% 15% 15% 15% 15%; */
    grid-template-columns: repeat(2, min-content);
    gap: 1ch 5ch;
    width 100vw;
    box-sizing: border-box;
  }

  .plot-configuration-table {
    display: grid;
    grid-template-rows: repeat(7, 1fr);
    grid-template-columns: repeat(2, min-content); 
    width: fit-content;
    gap: 10px;
    box-sizing: border-box;
    padding: 8px;
  }

  .header {
    font-size: 120%;
    font-weight: bold;
    white-space: nowrap;
    text-align: left;
  }

  .guide {
    font-size: 110%;
    font-weight: bold;
    white-space: nowrap;
    text-align: left;
  }

  .report {
    font-size: 90%;
    font-weight: bold;
    white-space: nowrap;
    text-align: left;
    color: black;
  }

  .field-row {
    display: flex;
    flex-direction: column;
  }

  .cell {
    white-space: nowrap;
  }

  .range-slider {
    position: relative;
    width: 300px;
    height: 50px;
    display: flex;
    align-items: center;
  }

  input[type="range"] {
    position: absolute;
    width: 100%;
    appearance: none;
    background: none;
    /* 1. Tell the input to ignore clicks */
    pointer-events: none; 
    margin: 0;
    z-index: 2;
  }

  /* 2. Tell the thumb to be clickable again */
  input[type="range"]::-webkit-slider-thumb {
    appearance: none;
    height: 20px;
    width: 20px;
    border-radius: 50%;
    background: #2563eb;
    cursor: pointer;
    pointer-events: auto; /* Re-enable clicks just for the handle */
  }

  /* 3. Do the same for Firefox */
  input[type="range"]::-moz-range-thumb {
    height: 20px;
    width: 20px;
    border-radius: 50%;
    background: #2563eb;
    cursor: pointer;
    pointer-events: auto;
    border: none;
  }

  /* The Track background */
  .slider-track {
    position: absolute;
    top: 20%;
    transform: translateY(-50%);
    width: 100%;
    height: 6px;
    background: #e5e7eb;
    border-radius: 3px;
    z-index: 1;
  }

</style>

