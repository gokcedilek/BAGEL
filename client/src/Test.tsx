import React, { useEffect, useState } from 'react';
import {
  Container,
  CssBaseline,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  Typography,
} from '@mui/material';

const Test = () => {
  const [queryName, setQueryName] = useState<string>('shortestpath');
  return (
    <>
      <CssBaseline />
      <Container>
        <Grid
          container
          justifyContent='center'
          alignItems='center'
          sx={{ height: '100vh', border: '1px solid green' }}>
          <Grid item>
            {/* <Typography variant='h1'>Test</Typography> */}
            <FormControl fullWidth>
              <InputLabel id='select-query-label'></InputLabel>
              <Select
                labelId='select-query-label'
                id='select-query'
                value={queryName}
                label='Query'
                onChange={(event) => setQueryName(event.target.value)}>
                <MenuItem value='shortestpath'>Shortest Path</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item>
            <Typography variant='h1'>Test2</Typography>
          </Grid>
        </Grid>
      </Container>
    </>
  );
};

export default Test;
