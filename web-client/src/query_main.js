import { mount } from 'svelte';
import App from './App.svelte';

const app = mount(App, { 
  target: document.getElementById('app'),
  props: {
    streamvisUrl: __WEB_URI__ 
  }
});

export default app;

