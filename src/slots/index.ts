import React from 'react';
import { CadastralMapClickHandler } from '../components/CadastralMapClickHandler';
import { CadastralClickToggle } from '../components/CadastralClickToggle';
import { CadastralProvider } from '../context/CadastralContext';
import type { ModuleViewerSlots, SlotWidgetDefinition } from '@nekazari/sdk';

// Module identifier - used for all slot widgets
const MODULE_ID = 'catastro-spain';

/**
 * Catastro Spain Module Slots Configuration
 * 
 * All widgets include explicit moduleId for proper host integration.
 * This module adds click-to-add functionality for cadastral parcels
 * on the /entities page. When a user clicks on the map, it queries
 * the cadastral service and shows a confirmation dialog before creating the parcel.
 */
export const moduleSlots: ModuleViewerSlots = {
  'map-layer': [
    {
      id: 'catastro-spain-click-handler',
      moduleId: MODULE_ID,
      component: 'CadastralMapClickHandler',
      priority: 100,
      localComponent: CadastralMapClickHandler,
    },
  ],
  'layer-toggle': [
    {
      id: 'catastro-spain-click-toggle',
      moduleId: MODULE_ID,
      component: 'CadastralClickToggle',
      priority: 20,
      localComponent: CadastralClickToggle,
    },
  ],
  'context-panel': [],
  'bottom-panel': [],
  'entity-tree': [],

  // Provider for sharing state between components
  moduleProvider: CadastralProvider,
};

/**
 * Export as viewerSlots for host integration
 */
export const viewerSlots = moduleSlots;

// Also export as default for Module Federation compatibility
export default viewerSlots;
