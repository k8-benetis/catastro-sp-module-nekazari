import { CadastralMapClickHandler } from '../components/CadastralMapClickHandler';
import type { ModuleViewerSlots, SlotWidgetDefinition } from '@nekazari/sdk';

/**
 * Catastro Spain Module Slots Configuration
 * 
 * This module adds click-to-add functionality for cadastral parcels
 * on the /entities page. When a user clicks on the map, it queries
 * the cadastral service and automatically creates a parcel if found.
 */
export const moduleSlots: ModuleViewerSlots = {
  'map-layer': [
    {
      id: 'catastro-spain-click-handler',
      component: 'CadastralMapClickHandler',
      priority: 100, // High priority to ensure it's loaded
      localComponent: CadastralMapClickHandler,
    },
  ],
};

/**
 * Export as viewerSlots for host integration
 */
export const viewerSlots = moduleSlots;

