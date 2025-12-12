import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

if (!process.env.GRPC_URI || !process.env.WEB_URI) {
  console.error("GRPC_URI and WEB_URI environment variables must be set");
  process.exit(1);
}

const grpc_target = `http://${process.env.GRPC_URI}`
const web_uri = `http://${process.env.WEB_URI}`

export default defineConfig({
  define: {
    __WEB_URI__: JSON.stringify(web_uri),
  },
  server: {
    port: 5173,
    proxy: {
      '/streamvis.v1.Service': {
        target: grpc_target,
        changeOrigin: true,
      },
    },
  },
  plugins: [
    {
      name: 'log-grpc-proxy',
      configureServer() {
        console.log(`[vite] Creating proxy: /streamvis.v1.Service -> ${grpc_target}`);
      },
    },
    svelte(),
  ]
});


