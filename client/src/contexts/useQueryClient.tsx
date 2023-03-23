import React, { createContext, useContext } from 'react';
import { CoordClient } from '../proto/coord_grpc_web_pb';

// example of singleton pattern

type QueryClientState = {
  queryClient: CoordClient;
};

type QueryClientProviderProps = {
  children: React.ReactNode;
};

const QueryClientContext = createContext<QueryClientState>({
  queryClient: {} as CoordClient,
});

const client = new CoordClient('http://bagel-envoy.fly.dev:8080', null, null);

export const QueryClientProvider = ({
  children,
}: QueryClientProviderProps): JSX.Element => (
  <QueryClientContext.Provider value={{ queryClient: client }}>
    {children}
  </QueryClientContext.Provider>
);

export const useQueryClient = (): QueryClientState =>
  useContext(QueryClientContext);
