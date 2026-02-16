import { defineConfig } from 'vite';
import { nkzModulePreset } from '@nekazari/module-builder';
import path from 'path';

export default defineConfig(nkzModulePreset({
  moduleId: 'catastro-spain', // Must match database ID
  entry: 'src/moduleEntry.ts',

  // Custom alias configuration for this specific module
  viteConfig: {
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    // Proxy for local development (optional, but helpful for `npm run dev`)
    server: {
      port: 5004,
      proxy: {
        '/api': {
          target: 'https://nkz.robotika.cloud',
          changeOrigin: true,
          secure: true,
        },
      },
    }
  }
}));
