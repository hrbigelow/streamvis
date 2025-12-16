<script>
  import { onMount } from 'svelte';
  import { getServiceClient } from './util.js';
  import { create } from '@bufbuild/protobuf';
  import {
    ScopeRequestSchema,
    NamesRequestSchema
  } from '../streamvis/v1/data_pb.js';
  let { streamvisUrl } = $props();

  let allScopes = $state([]);
  let selectedScopes = $state(new Set());

  let visibleNames = $state([]);
  let selectedNames = $state(new Set());

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

  function clearSelections() {
    selectedScopes = new Set();
    selectedNames = new Set();
    selectedFilters = new Set();
    selectedGroups = new Set();
  }

  async function fetchScopes() {
    const req = create(ScopeRequestSchema, { scopeRegex: ".*" });
    const newScopes = [];
    for await (const resp of client.scopes(req)) {
      const { scope } = resp;
      newScopes.push(scope);
    }
    allScopes = newScopes.sort(); // for Svelte to catch
    const next = new Set();
    for (const scope of selectedScopes) {
      if (allScopes.includes(scope)) {
        next.add(scope);
      }
    }
    selectedScopes = next;
  }

  function makeRegex(items) {
    const ary = Array.from(items);
    if (ary.length == 0) {
      return "^$";
    } else {
      return ary.map(s => `^${s}$`).join("|");
    }
  }

  async function fetchNames() {
    const scopeRegex = makeRegex(selectedScopes)
    const req = create(NamesRequestSchema, { scopeRegex, nameRegex: ".*" });
    const newNames = new Set();
    for await (const resp of client.names(req)) {
      const { name } = resp;
      newNames.add(name);
    }
    visibleNames = [...newNames].sort();
    await fetchFields()
  }

  async function fetchFields() {
    const scopeRegex = makeRegex(selectedScopes);
    const activeNames = [...selectedNames].filter(el => visibleNames.includes(el));
    const newFields = new Set();
    const nameRegex = makeRegex(activeNames);
    const req = create(NamesRequestSchema, { scopeRegex, nameRegex });
    for await (const resp of client.names(req)) {
      const { fields } = resp;
      for (const field of fields) {
        newFields.add(field.name);
      }
    }
    visibleFields = [...newFields].sort();
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

  async function toggleScope(scope) {
    const next = new Set(selectedScopes);
    if (next.has(scope)) {
      next.delete(scope);
    } else {
      next.add(scope);
    }
    selectedScopes = next;
    await fetchNames();
  }

  async function toggleName(name) {
    const next = new Set(selectedNames);
    if (next.has(name)) {
      next.delete(name);
    } else {
      next.add(name);
    }
    selectedNames = next;
    await fetchFields();
  }

  async function toggleFilter(field) {
    const next = new Set(selectedFilters);
    if (next.has(field)) {
      next.delete(field);
    } else {
      next.add(field);
    }
    selectedFilters = next;
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
    refreshEvery(fetchScopes, 10_000);
  });



</script>

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



<style>

  .table {
    display: grid;
    grid-template-rows: repeat(2, min-content);
    /* grid-template-columns: 25% 15% 15% 15% 15% 15%; */
    grid-template-columns: repeat(7, min-content);
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
    text-align: right;
  }

  .field-row {
    display: flex;
    flex-direction: column;
  }

  .cell {
    white-space: nowrap;
  }


</style>

