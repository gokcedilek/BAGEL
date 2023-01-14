import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { theme } from './Theme';
import { ThemeProvider } from '@mui/material/styles';
import { CssBaseline } from '@mui/material';

import { QueryResultProvider } from './contexts/useQueryResult';
import { QueryClientProvider } from './contexts/useQueryClient';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <QueryClientProvider>
      <QueryResultProvider>
        <ThemeProvider theme={theme}>
          <CssBaseline />
          <App />
        </ThemeProvider>
      </QueryResultProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
