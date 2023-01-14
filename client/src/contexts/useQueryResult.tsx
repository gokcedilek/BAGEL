import React, { createContext, useContext, useState } from 'react';

type QueryResultState = {
  queryResult: any;
  setQueryResult: (queryResult: any) => void;
};

type QueryResultProviderProps = {
  children: React.ReactNode;
};

const QueryResultContext = createContext<QueryResultState>({
  queryResult: null,
  setQueryResult: () => {},
});

const useQueryResultProvider = (): QueryResultState => {
  const [queryResult, setQueryResult] = useState(null);
  return { queryResult, setQueryResult };
};

export const QueryResultProvider = ({
  children,
}: QueryResultProviderProps): JSX.Element => (
  <QueryResultContext.Provider value={useQueryResultProvider()}>
    {children}
  </QueryResultContext.Provider>
);

export const useQueryResult = (): QueryResultState =>
  useContext(QueryResultContext);
