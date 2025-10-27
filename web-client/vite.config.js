import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      '/streamvis.v1.Service': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      }
    }
  }
});


