<script>
  import { onMount } from 'svelte';
  import { getServiceClient } from './clientUtil.js';
  import { create } from '@bufbuild/protobuf';
  import {
    ListCommonAttributesRequestSchema,
  } from './gen/streamvis/v1/data_pb.js';

  import { StateManager, RunFilter } from './state.svelte.js'; 
  let client = getServiceClient('/');
  let uiState = $state({});
  let stateManager = new StateManager(client, uiState);
  stateManager.connect();

  let selectedSeries = $state(null);
  let selectedTagsArray = $state([]);
  let selectedTags = $derived(new Set(selectedTagsArray));

  let startTimes = $derived.by(() => {
    const times = stateManager.startTimes;
    times.push(new Date());
    return times;
  });

  let minStartedAtIndex = $state(0);
  let maxStartedAtIndex = $state(0);

  let filteredTags = $derived.by(() => {
    return stateManager.filteredTags(
      startTimes[minStartedAtIndex],
      startTimes[maxStartedAtIndex],
      selectedSeries, 
      selectedTags
    )
  });


  let commonAttributes = $state([]);
  let filteredRuns = $state([]);
  let matchAllTags = $state(true);

  // main output object
  let runFilter = $derived.by(() => {
    return new RunFilter(
      selectedTags, 
      matchAllTags, 
      startTimes[minStartedAtIndex],
      startTimes[maxStartedAtIndex],
      []
    );
  });

  let numFilteredRuns = $derived.by(() => {
    return stateManager.countFilteredRuns(runFilter, selectedSeries);
  });

  let filterUI = $state({
    attributeHandles: [null, null, null, null, null]
  });

  function handleMinInput(e) {
    const val = Number(e.currentTarget.value);
    if (val > maxStartedAtIndex) {
      minStartedAtIndex = maxStartedAtIndex;
    } else {
      minStartedAtIndex = val;
    }
  }

  function handleMaxInput(e) {
    const val = Number(e.currentTarget.value);
    if (val < minStartedAtIndex) {
      maxStartedAtIndex = minStartedAtIndex;
    } else {
      maxStartedAtIndex = val;
    }
  }

  let { streamvisUrl } = $props();

  let plotType = $state(null);
  let axisBindings = $state({
    x: null,
    y: null,
    order: null,
    color: null,
    group: null,
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

  let mounted = false;

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
    refreshEvery(() => stateManager.fetchAll(), 10_000);
  });

</script>

<div class="filter-run-table">
  <div class="guide">Series:</div>
  <select bind:value={selectedSeries}>
    <option value={null}>(none)</option>
    {#each Object.entries(stateManager.series) as [handle, series]}
      <option value={handle}>{series.name}</option>
    {/each}
  </select>
  <!--
  {#if selectedSeries !== null}
    <div class="report">{stateManager.series[selectedSeries].name}</div>
    <div></div>
    {#each stateManager.series[selectedSeries].fields as field}
      <div class="report">{field.name}</div>
      <div class="report">{field.dataType}</div>
    {/each}
  {/if}
  -->

  <div class="guide">Started at:</div>

  <div class="range-slider">
    <input type="range" 
           class="min-input" 
           min="0" 
           max="{startTimes.length - 1}"
           bind:value={minStartedAtIndex}
           oninput={handleMinInput}
           />
    <input type="range" 
           class="max-input" 
           min="0" 
           max="{startTimes.length - 1}" 
           bind:value={maxStartedAtIndex}
           oninput={handleMaxInput}
           />
     <div class="slider-track"></div>
  </div>

  <div class="guide">Date From:</div>
  <div>{runFilter.minStartedAt.toLocaleString()}</div>
  <div class="guide">Date To:</div>
  <div>{runFilter.maxStartedAt.toLocaleString()}</div>

  <div class="guide">Tag Matching</div>
  <div>
    <label>
      <input type="radio" bind:group={matchAllTags} value={false} />
      match any
    </label>
    <label>
      <input type="radio" bind:group={matchAllTags} value={true} />
      match all
    </label>
  </div>

  <div class="guide">Tags</div>

  {#each Object.entries(filteredTags) as [tag, numRuns]}
    <label>
      <input type="checkbox"
             bind:group={selectedTagsArray} 
             value={tag}
             />
      {tag} ({numRuns})
    </label>
    <div></div>
  {/each}
  <div></div>
  <div class="guide">Filtered Runs</div>
  <div>{numFilteredRuns}</div>
  <div class="guide">Attributes</div>
  <div></div>
  {#each Object.entries(allFields) as [handle, field]}
    <details>
      <summary>
        <input id="{handle}" 
               type="checkbox" 
               checked={uiState[handle].active} 
               onchange={(e) => stateManager.updateAttrMatch()}
               />
        <label for="{handle}">{field.name}</label>
      </summary>
      <div>Values...</div>
    </details>
  {/each}
  <div></div>

</div>

<!--
<div class="plot-configuration-table">
  <label class="guide">Plot Type:</label>
  <select bind:value={plotType}>
    <option value="line">Line Plot</option>
    <option value="scatter">Scatter Plot</option>
  </select>

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

</div>

-->



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
-->

<style>

  .filter-run-table {
    display: grid;
    grid-template-rows: repeat(5, min-content);
    /* grid-template-columns: 25% 15% 15% 15% 15% 15%; */
    grid-template-columns: repeat(2, min-content);
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
    border-color: red;
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    width: 100%;
    height: 6px;
    background: #e5e7eb;
    border-radius: 3px;
    z-index: 1;
  }

</style>

