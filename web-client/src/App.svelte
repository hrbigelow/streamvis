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
  } from './gen/streamvis/v1/data_pb.js';
  let { streamvisUrl } = $props();

  let allFields = $state([]); 
  let allSeries = $state([]);
  let commonSeries = $state([]); 
  let commonAttributes = $state([]);
  let tagsLogic = $state(''); // 
  let tagsList = $state('');
  let minStartedAt = $state('');
  let maxStartedAt = $state('');
  let attributeFilter = $state([]);
  let includedRuns = $state([]);

  let visibleFields = $state([]);
  let xAxisField = $state('');
  let yAxisField = $state('');
  let orderField = $state('');
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

  async function fetchFields() {
    const req = create(ListFieldsRequestSchema, {});
    const fields = [];
    for await (const resp of client.listFields(req)) {
      fields.push(resp);
    }
    allFields = fields;
  }

  async function fetchSeries() {
    const req = create(ListSeriesRequestSchema, {});
    const series = [];
    for await (const resp of client.listSeries(req)) {
      series.push(resp);
    }
    allSeries = series;
  }

  async function fetchCommonSeries() {
    const req = create(ListCommonSeriesRequestSchema, {
      runHandles: includedRuns,
    });
    const series = [];
    for await (const resp of client.listCommonSeries(req)) {
      series.push(resp);
    }
    commonSeries = series;
  }

  async function fetchCommonAttributes() {
    const req = create(ListCommonAttributesRequestSchema, {
      runHandles: includedRuns,
    });
    const attrs = [];
    for await (const resp of client.listCommonAttributes(req)) {
      attrs.push(resp);
    }
    commonAttributes = attrs;
  }

  async function updateSelectedRuns() {
    const req = create(ListRunsRequestSchema, {
      attributeFilters: [],
      tagFilter: {
        hasAnyTag: []
      },
      minStartedAt: null,
      maxStartedAt: null
    });
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

  async function toggleGroup(field) {
    const next = new Set(selectedGroups);
    if (next.has(field)) {
      next.delete(field);
    } else {
      next.add(field);
    }
    selectedGroups = next;
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
    refreshEvery(fetchFields, 10_000);
  });

  $effect(() => {
    updateSelectedRuns();
  });

  $effect(() => {
    fetchCommonSeries();
    fetchCommonAttributes();
  });


</script>

<div class="filter-run-table">
  <div class="guide">Filter Run:</div>
  <label class="guide" for="tags">Tags:
    {#each ["has any", "has all"] as condition}
      <label>
        <input type="radio" name="tags" value={condition} bind:group={tagsLogic} />
        {condition}
      </label>
    {/each}
    <input id="tags-list" bind:value={tagsList} />
  </label>
  <label class="guide" for="started-at-after">Started At After:
    <input id="started-at-after" type="range" bind:value={minStartedAt} />
  </label>
  <label class="guide" for="started-at-before">Started At Before:
    <input id="started-at-before" type="range" bind:value={maxStartedAt} />
  </label>
  <label class="guide">Attribute Filters:</label>
  {#each [0, 1, 2, 3, 4] as it}
    <select bind:value={attributeFilter[it]}>
      {#each allFields as attr}
        <option value={attr.handle}>
        {attr.name}
        </option>
      {/each}
    </select>
  {/each}
</div>

<div class="selected-runs-table">
  <div class="guide">Included Runs</div>
  <div>{includedRuns.length}</div>
  <div class="guide">Series:</div>
  {#each commonSeries as series}
    <div>{series.name}</div>
  {/each}
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
    grid-template-columns: repeat(1, min-content);
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

  .top-table {
    display: grid;
    grid-template-rows: repeat(7, 1fr);
    grid-template-columns: auto minmax(30ch, auto) 1fr;
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

  .field-row {
    display: flex;
    flex-direction: column;
  }

  .cell {
    white-space: nowrap;
  }


</style>

