import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      '/streamvis.v1.Service': {
        target: 'http://100.65.34.72:8001',
        changeOrigin: true,
      }
    }
  }
});


