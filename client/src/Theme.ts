import { createTheme, ThemeProvider } from '@mui/material/styles';

export const theme = createTheme({
  palette: {},
  components: {
    MuiButton: {
      defaultProps: {
        variant: 'outlined',
      },
      styleOverrides: {
        root: {
          // color: 'red',
          // backgroundColor: 'yellow',
        },
      },
    },
  },
});
