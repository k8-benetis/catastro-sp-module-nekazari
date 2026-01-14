import React, { useEffect, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { useViewerOptional } from '@nekazari/sdk';
import { cadastralApi, CadastralData } from '../services/cadastralApi';
import { parcelApi } from '../services/parcelApi';
import { CheckCircle, XCircle, Loader2 } from 'lucide-react';
import { useCadastral } from '../context/CadastralContext';
import { CadastralConfirmDialog } from './CadastralConfirmDialog';

/**
 * Component that intercepts map clicks on the /entities page
 * and allows adding parcels with a single click
 */
export const CadastralMapClickHandler: React.FC = () => {
  const location = useLocation();
  const viewerContext = useViewerOptional();
  const cesiumViewer = viewerContext?.cesiumViewer;
  const { isClickEnabled } = useCadastral();
  const [isProcessing, setIsProcessing] = useState(false);
  const [notification, setNotification] = useState<{
    type: 'success' | 'error';
    message: string;
  } | null>(null);
  const [pendingParcel, setPendingParcel] = useState<{
    data: CadastralData;
    area: number;
  } | null>(null);
  const handlerRef = useRef<any>(null);
  const notificationTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Only activate on /entities page
  const isEntitiesPage = location.pathname === '/entities';

  useEffect(() => {
    console.log('[CadastralMapClickHandler] useEffect triggered', {
      isEntitiesPage,
      hasCesiumViewer: !!cesiumViewer,
      isClickEnabled,
      isProcessing,
      hasPendingParcel: !!pendingParcel
    });

    // Only activate if click is enabled and we're on the entities page
    if (!isEntitiesPage || !cesiumViewer || !isClickEnabled) {
      console.log('[CadastralMapClickHandler] Conditions not met, cleaning up handler', {
        isEntitiesPage,
        hasCesiumViewer: !!cesiumViewer,
        isClickEnabled
      });
      if (handlerRef.current && !handlerRef.current.isDestroyed()) {
        handlerRef.current.destroy();
        handlerRef.current = null;
      }
      return;
    }

    // @ts-ignore
    const Cesium = window.Cesium;
    if (!Cesium) {
      console.warn('[CadastralMapClickHandler] Cesium not available on window');
      return;
    }

    console.log('[CadastralMapClickHandler] Setting up click handler on Cesium viewer', {
      canvas: cesiumViewer.scene?.canvas ? 'found' : 'missing'
    });

    // Create click handler
    const handler = new Cesium.ScreenSpaceEventHandler(cesiumViewer.scene.canvas);
    handlerRef.current = handler;

    handler.setInputAction(async (click: any) => {
      console.log('[CadastralMapClickHandler] Click detected!', {
        isProcessing,
        hasPendingParcel: !!pendingParcel
      });

      // Check if we're processing a previous click or have a pending dialog
      if (isProcessing || pendingParcel) {
        console.log('[CadastralMapClickHandler] Click ignored (processing or pending)');
        return;
      }

      // Check if clicked on an existing entity
      const pickedObject = cesiumViewer.scene.pick(click.position);
      if (Cesium.defined(pickedObject) && pickedObject.id) {
        console.log('[CadastralMapClickHandler] Clicked on existing entity, ignoring');
        // Clicked on an entity, don't handle
        return;
      }

      // Get coordinates from click
      let cartesian = cesiumViewer.scene.pickPosition(click.position);
      if (!cartesian) {
        cartesian = cesiumViewer.camera.pickEllipsoid(click.position, cesiumViewer.scene.globe.ellipsoid);
      }

      if (!cartesian) {
        return;
      }

      const cartographic = Cesium.Cartographic.fromCartesian(cartesian);
      const longitude = Cesium.Math.toDegrees(cartographic.longitude);
      const latitude = Cesium.Math.toDegrees(cartographic.latitude);

      console.log('[CadastralMapClickHandler] Map clicked at:', { longitude, latitude });

      // Process the click
      await handleMapClick(longitude, latitude);
    }, Cesium.ScreenSpaceEventType.LEFT_CLICK);

    return () => {
      if (handlerRef.current && !handlerRef.current.isDestroyed()) {
        handlerRef.current.destroy();
        handlerRef.current = null;
      }
    };
  }, [isEntitiesPage, cesiumViewer, isClickEnabled, isProcessing, pendingParcel]);

  const handleMapClick = async (longitude: number, latitude: number) => {
    setIsProcessing(true);
    setNotification(null);

    try {
      console.log('[CadastralMapClickHandler] Querying cadastral service...');
      const cadastralData = await cadastralApi.queryByCoordinates(longitude, latitude);

      if (!cadastralData.cadastralReference) {
        setNotification({
          type: 'error',
          message: 'No se encontró información catastral para esta ubicación.',
        });
        clearNotificationAfterDelay();
        return;
      }

      // Check if geometry is available
      const hasGeometry = cadastralData.geometry &&
        cadastralData.geometry.type === 'Polygon' &&
        cadastralData.geometry.coordinates &&
        cadastralData.geometry.coordinates.length > 0;

      if (!hasGeometry) {
        setNotification({
          type: 'error',
          message: 'La parcela no tiene geometría disponible. Por favor, use el método de dibujo manual.',
        });
        clearNotificationAfterDelay();
        return;
      }

      // Calculate area in hectares
      const area = calculatePolygonAreaHectares(cadastralData.geometry!);

      // Show confirmation dialog instead of creating immediately
      setPendingParcel({ data: cadastralData, area });
      setIsProcessing(false);

    } catch (error: any) {
      console.error('[CadastralMapClickHandler] Error:', error);
      const errorMessage = error.response?.data?.error || error.response?.data?.message || error.message || 'Error al procesar la parcela';
      setNotification({
        type: 'error',
        message: errorMessage,
      });
      clearNotificationAfterDelay();
    } finally {
      setIsProcessing(false);
    }
  };

  const clearNotificationAfterDelay = () => {
    if (notificationTimeoutRef.current) {
      clearTimeout(notificationTimeoutRef.current);
    }
    notificationTimeoutRef.current = setTimeout(() => {
      setNotification(null);
    }, 5000);
  };

  // Calculate polygon area in hectares using equirectangular projection
  // Same algorithm as used in the platform (nekazari-public/apps/host/src/utils/geo.ts)
  const calculatePolygonAreaHectares = (geometry: { type: 'Polygon'; coordinates: number[][][] }): number => {
    if (!geometry.coordinates || geometry.coordinates.length === 0) {
      return 0;
    }

    const ring = geometry.coordinates[0]; // First ring (exterior)
    if (ring.length < 4) {
      return 0;
    }

    // Remove duplicate closing point for calculations
    const coords = ring.slice(0, -1);
    if (coords.length < 3) {
      return 0;
    }

    // Calculate average latitude for projection
    const lat0 = coords.reduce((sum, [, lat]) => sum + lat, 0) / coords.length;
    const lat0Rad = (lat0 * Math.PI) / 180;

    const EARTH_RADIUS_METERS = 6378137;
    
    // Project coordinates to meters using equirectangular projection
    const projected = coords.map(([lon, lat]) => {
      const x = ((lon * Math.PI) / 180) * EARTH_RADIUS_METERS * Math.cos(lat0Rad);
      const y = ((lat * Math.PI) / 180) * EARTH_RADIUS_METERS;
      return [x, y] as [number, number];
    });

    // Shoelace formula
    let area = 0;
    for (let i = 0; i < projected.length; i++) {
      const [x1, y1] = projected[i];
      const [x2, y2] = projected[(i + 1) % projected.length];
      area += x1 * y2 - x2 * y1;
    }

    const areaSqMeters = Math.abs(area) / 2;
    return Number((areaSqMeters / 10_000).toFixed(4)); // Convert to hectares
  };

  const handleConfirmParcel = async () => {
    if (!pendingParcel) return;

    setIsProcessing(true);
    setPendingParcel(null);

    try {
      const { data: cadastralData, area } = pendingParcel;

      console.log('[CadastralMapClickHandler] Creating parcel...');
      const newParcel = {
        name: cadastralData.cadastralReference || `Parcela ${cadastralData.municipality}`,
        geometry: cadastralData.geometry!,
        municipality: cadastralData.municipality || '',
        province: cadastralData.province || '',
        cadastralReference: cadastralData.cadastralReference,
        cropType: '', // User can edit later
        area: area,
        category: 'cadastral',
        ndviEnabled: true,
      };

      await parcelApi.createParcel(newParcel);

      setNotification({
        type: 'success',
        message: `Parcela ${cadastralData.cadastralReference} añadida correctamente (${area.toFixed(2)} ha)`,
      });

      clearNotificationAfterDelay();

      // Reload entities in the viewer after a short delay
      setTimeout(() => {
        viewerContext?.loadAllEntities?.();
      }, 1500);
    } catch (error: any) {
      console.error('[CadastralMapClickHandler] Error creating parcel:', error);
      const errorMessage = error.response?.data?.error || error.response?.data?.message || error.message || 'Error al crear la parcela';
      setNotification({
        type: 'error',
        message: errorMessage,
      });
      clearNotificationAfterDelay();
    } finally {
      setIsProcessing(false);
    }
  };

  const handleCancelParcel = () => {
    setPendingParcel(null);
    setIsProcessing(false);
  };

  if (!isEntitiesPage) {
    return null;
  }

  return (
    <>
      {/* Processing indicator */}
      {isProcessing && !pendingParcel && (
        <div className="fixed top-4 right-4 z-50 bg-white rounded-lg shadow-lg border border-gray-200 p-4 flex items-center gap-3 min-w-[300px]">
          <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
          <span className="text-sm text-gray-700">Consultando catastro...</span>
        </div>
      )}

      {/* Confirmation Dialog */}
      {pendingParcel && (
        <CadastralConfirmDialog
          cadastralData={pendingParcel.data}
          area={pendingParcel.area}
          onConfirm={handleConfirmParcel}
          onCancel={handleCancelParcel}
          isProcessing={isProcessing}
        />
      )}

      {/* Notification */}
      {notification && (
        <div className={`fixed top-4 right-4 z-50 bg-white rounded-lg shadow-lg border ${
          notification.type === 'success' ? 'border-green-200' : 'border-red-200'
        } p-4 flex items-center gap-3 min-w-[300px]`}>
          {notification.type === 'success' ? (
            <CheckCircle className="w-5 h-5 text-green-600" />
          ) : (
            <XCircle className="w-5 h-5 text-red-600" />
          )}
          <span className={`text-sm ${
            notification.type === 'success' ? 'text-green-700' : 'text-red-700'
          }`}>
            {notification.message}
          </span>
        </div>
      )}
    </>
  );
};

