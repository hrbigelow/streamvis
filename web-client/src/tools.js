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

export { refreshEvery }
