import React from 'react';
import { CadastralMapClickHandler } from '../components/CadastralMapClickHandler';
import { CadastralClickToggle } from '../components/CadastralClickToggle';
import { CadastralProvider } from '../context/CadastralContext';
import type { ModuleViewerSlots, SlotWidgetDefinition } from '@nekazari/sdk';

/**
 * Catastro Spain Module Slots Configuration
 * 
 * This module adds click-to-add functionality for cadastral parcels
 * on the /entities page. When a user clicks on the map, it queries
 * the cadastral service and shows a confirmation dialog before creating the parcel.
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
  'layer-toggle': [
    {
      id: 'catastro-spain-click-toggle',
      component: 'CadastralClickToggle',
      priority: 20, // Lower priority = higher in list
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

