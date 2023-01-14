import React, { useEffect, useState } from 'react';
import {
  Box,
  Button,
  Container,
  CssBaseline,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  Typography,
  TextField,
  InputBase,
  Theme,
  useTheme,
  OutlinedInput,
} from '@mui/material';
import {
  Query,
  QUERY_TYPE,
  QueryProgressRequest,
  QueryProgressResponse,
  FetchGraphRequest,
  FetchGraphResponse,
  VertexMessage,
} from '../proto/coord_pb';
import { useQueryResult } from '../contexts/useQueryResult';
import { useQueryClient } from '../contexts/useQueryClient';

const styles = (theme: Theme) => ({
  select: {
    // color: 'blue',
  },
  selectDisabled: {
    // color: 'green',
  },
  menuItem: {},
  menuItemHidden: {
    display: 'none',
  },
});

const CreateQuery = () => {
  const { queryResult, setQueryResult } = useQueryResult();
  const { queryClient } = useQueryClient();
  const [queryName, setQueryName] = useState('none');
  const [showPlaceholder, setShowPlaceholder] = useState(queryName === 'none');
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  const theme = useTheme();

  const sx = styles(theme);

  const sendQuery = async () => {
    const queryType =
      queryName === 'shortestpath'
        ? QUERY_TYPE.SHORTEST_PATH
        : QUERY_TYPE.PAGE_RANK;
    const query = new Query([
      'clientId',
      queryType,
      [srcId, destId], // todo: don't send destId if queryType is PAGE_RANK
      'xxx',
      'gokce-test-db',
    ]);
    queryClient.startQuery(query, null, (err, response) => {
      if (err) return console.log('startQuery err: ', err);
      console.log('startQuery response: ', response.toObject());
      setQueryResult(response.toObject().result);
    });
  };

  return (
    <Box
      sx={{
        width: '90%',
        height: '10vh',
        padding: '10px',
        display: 'flex',
        justifyContent: 'space-evenly',
        alignItems: 'center',
      }}>
      <Box className='form'>
        <FormControl fullWidth>
          <Select
            id='select-query'
            value={queryName}
            defaultValue='none'
            onChange={(event) => setQueryName(event.target.value as string)}
            onFocus={() => setShowPlaceholder(false)}
            onClose={(event) =>
              setShowPlaceholder(
                (event.target as HTMLInputElement).value === undefined
              )
            }
            sx={{
              ...sx.select,
              ...(queryName === 'none' && { ...sx.selectDisabled }),
            }}>
            <MenuItem
              sx={{
                ...sx.menuItem,
                ...(!showPlaceholder && { ...sx.menuItemHidden }),
              }}
              value='none'
              disabled>
              Select Query
            </MenuItem>
            <MenuItem sx={sx.menuItem} value='shortestpath'>
              Shortest Path
            </MenuItem>
            <MenuItem sx={sx.menuItem} value='pagerank'>
              Page Rank
            </MenuItem>
          </Select>
        </FormControl>
      </Box>
      {queryName !== 'none' && (
        <Box
          className='query'
          sx={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <Box>
            <TextField
              id='source'
              label='source'
              variant='standard'
              onChange={(event) => setSrcId(event.target.value)}
            />
          </Box>
          {queryName === 'shortestpath' && (
            <Box>
              <TextField
                id='dest'
                label='dest'
                variant='standard'
                onChange={(event) => setDestId(event.target.value)}
              />
            </Box>
          )}
          <Box>
            <Button variant='contained' onClick={sendQuery}>
              Submit
            </Button>
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default CreateQuery;
