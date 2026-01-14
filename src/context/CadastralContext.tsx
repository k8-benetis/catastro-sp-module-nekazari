import React, { createContext, useContext, useState, ReactNode } from 'react';

interface CadastralContextType {
  isClickEnabled: boolean;
  toggleClickEnabled: () => void;
  setClickEnabled: (enabled: boolean) => void;
}

const CadastralContext = createContext<CadastralContextType | undefined>(undefined);

export const CadastralProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [isClickEnabled, setIsClickEnabled] = useState(false);

  const toggleClickEnabled = () => {
    setIsClickEnabled(prev => !prev);
  };

  const setClickEnabled = (enabled: boolean) => {
    setIsClickEnabled(enabled);
  };

  return (
    <CadastralContext.Provider
      value={{
        isClickEnabled,
        toggleClickEnabled,
        setClickEnabled,
      }}
    >
      {children}
    </CadastralContext.Provider>
  );
};

export const useCadastral = (): CadastralContextType => {
  const context = useContext(CadastralContext);
  if (!context) {
    throw new Error('useCadastral must be used within a CadastralProvider');
  }
  return context;
};

