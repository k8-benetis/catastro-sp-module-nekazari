import type { ModuleViewerSlots } from '@nekazari/sdk';
import { moduleSlots } from './slots/index';
import pkg from '../package.json';

// Use strict module ID that matches database
const MODULE_ID = 'catastro-spain';

console.log(`[${MODULE_ID}] Initializing module v${pkg.version}`);

// Self-register with the host runtime
if (window.__NKZ__) {
    window.__NKZ__.register({
        id: MODULE_ID,
        viewerSlots: moduleSlots,
        // potential future use: provider if not already handled in slots
        version: pkg.version,
    });
} else {
    console.error(`[${MODULE_ID}] window.__NKZ__ not found! Module registration failed.`);
}
