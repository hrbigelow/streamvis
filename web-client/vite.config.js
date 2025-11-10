import { defineConfig } from 'vite';

if (!process.env.GRPC_URI) {
  console.error("GRPC_URI environment variable must be set");
  process.exit(1);
}

const target = `http://${process.env.GRPC_URI}`

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      '/streamvis.v1.Service': {
        target: target,
        changeOrigin: true,
      }
    },
  },
  optimizeDeps: {
    exclude: ['three']  // Don't pre-bundle three.js
  },
  resolve: {
    alias: {
      'three/examples/jsm/': '/home/henry/ai/projects/three.js/examples/jsm/',
      'three': '/home/henry/ai/projects/three.js/src/Three.js',
    }
  },
  plugins: [
    {
      name: 'log-grpc-proxy',
      configureServer() {
        console.log(`[vite] Creating proxy: /streamvis.v1.Service -> ${target}`);
      },
    },
  ]
});


