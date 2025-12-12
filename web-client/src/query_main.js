import { mount } from 'svelte';
import App from './App.svelte';

const app = mount(App, { 
  target: document.getElementById('app'),
  props: {
    streamvisUrl: "http://localhost:8080"
  }
});

export default app;

