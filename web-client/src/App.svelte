<script>
  import { onMount } from 'svelte';
  import { getServiceClient } from './util.js';
  import { create } from '@bufbuild/protobuf';
  import {
    ScopeRequestSchema,
    NamesRequestSchema
  } from '../streamvis/v1/data_pb.js';
  let { streamvisUrl } = $props();

  let scopes = $state([]);
  let selectedScopes = $state(new Set());

  let names = $state([]);
  let selectedNames = $state(new Set());

  let fields = $state([]);
  let xAxisField = $state('');
  let yAxisField = $state('');
  let plotTitle = $state('');
  let xAxisLabel = $state('');
  let yAxisLabel = $state('');
  let selectedFilters = $state(new Set());
  let selectedGroups = $state(new Set());
  let windowSize = $state(null);
  let stride = $state(null);
  let link = $derived.by(computeLink);

  let client;
  let mounted = false;

  async function fetchScopes() {
    const req = create(ScopeRequestSchema, { scopeRegex: ".*" });
    const newScopes = [];
    for await (const resp of client.scopes(req)) {
      const { scope } = resp;
      newScopes.push(scope);
    }
    scopes = newScopes.sort(); // for Svelte to catch
  }

  async function fetchNames() {
    const scopeRegex = [...selectedScopes].map(s => `^${s}$`).join("|");
    const req = create(NamesRequestSchema, { scopeRegex, nameRegex: ".*" });
    const newNames = new Set();
    for await (const resp of client.names(req)) {
      const { name } = resp;
      newNames.add(name);
    }
    names = [...newNames].sort();
  }

  async function fetchFields() {
    const scopeRegex = [...selectedScopes].map(s => `^${s}$`).join("|") 
    const nameRegex = [...selectedNames].map(n => `^${n}$`).join("|")
    const req = create(NamesRequestSchema, { scopeRegex, nameRegex });
    const newFields = new Set();
    for await (const resp of client.names(req)) {
      const { fields } = resp;
      for (const field of fields) {
        newFields.add(field.name);
      }
    }
    fields = [...newFields].sort();
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
      scopes: [...selectedScopes],
      names: [...selectedNames],
      plotTitle,
      xAxisLabel,
      yAxisLabel,
      xAxisField,
      yAxisField,
      filters: [...selectedFilters],
      groups: [...selectedGroups],
      window: windowSize,
      stride: stride
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
  <label for="plot-title">Plot Title</label>
  <input id="plot-title" bind:value={plotTitle} />
  <label for="x-axis-label">X-Axis Label</label>
  <input id="x-axis-label" bind:value={xAxisLabel} />
  <label for="y-axis-label">Y-Axis Label</label>
  <input id="y-axis-label" bind:value={yAxisLabel} />
  <label for="window-size">Window Size</label>
  <input id="window-size" bind:value={windowSize} />
  <label for="stride">Stride</label>
  <input id="stride" bind:value={stride} />
  <div></div>
  <a href={link} target="_blank">Open Plot (new tab)</a>
</div>


<div class="table">
  <div class="header">Scopes</div>
  <div class="header">Names</div>
  <div class="header">X-axis</div>
  <div class="header">Y-axis</div>
  <div class="header">Filters</div>
  <div class="header">Group By</div>

  <div class="cell">
    {#each scopes as scope}
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
    {#each names as name}
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
    {#each fields as field}
      <option value={field}>{field}</option>
    {/each}
    </select>
  </div>

  <div class="cell">
    <select bind:value={yAxisField}>
      <option value="">Select Y-Axis field</option>
    {#each fields as field}
      <option value={field}>{field}</option>
    {/each}
    </select>
  </div>

  <div class="cell">
    {#each fields as field}
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
    {#each fields as field}
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
    grid-template-rows: auto 1fr;
    grid-template-columns: 25% 15% 15% 15% 15% 15%;
    gap: 4px;
    width 100vw;
    box-sizing: border-box;
    padding: 8px;
  }

  .top-table {
    display: grid;
    grid-template-rows: 1fr 1fr 1fr 1fr 1fr;
    grid-template-columns: auto minmax(40ch, auto);
    width: fit-content;
    gap: 10px;
    box-sizing: border-box;
    padding: 8px;
  }

  .header {
    padding: 6px;
    text-align: left;
  }

  .field-row {
    display: flex;
    flex-direction: column;
  }

  .cell {
  }


</style>

