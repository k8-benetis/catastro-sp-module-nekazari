import React from 'react';
import { MapPin, MapPinOff } from 'lucide-react';
import { useCadastral } from '../context/CadastralContext';

/**
 * Control toggle for enabling/disabling cadastral click-to-add functionality
 * Appears in the layer-toggle slot (sidebar layer manager)
 */
export const CadastralClickToggle: React.FC = () => {
  const { isClickEnabled, toggleClickEnabled } = useCadastral();

  console.log('[CadastralClickToggle] Render', { isClickEnabled });

  return (
    <button
      onClick={toggleClickEnabled}
      className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-all ${
        isClickEnabled
          ? 'bg-blue-50 text-blue-700 border border-blue-200'
          : 'hover:bg-slate-50 text-slate-600'
      }`}
      title={isClickEnabled ? 'Desactivar clic catastral' : 'Activar clic catastral'}
    >
      {isClickEnabled ? (
        <MapPin className="w-4 h-4 text-blue-600" />
      ) : (
        <MapPinOff className="w-4 h-4 text-slate-400" />
      )}
      <span className="flex-1 text-left text-sm">
        {isClickEnabled ? 'Clic Catastral Activo' : 'Clic Catastral'}
      </span>
      <div
        className={`w-3 h-3 rounded-full transition-colors ${
          isClickEnabled ? 'bg-blue-500' : 'bg-slate-300'
        }`}
      />
    </button>
  );
};

