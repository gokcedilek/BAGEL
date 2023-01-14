import React from 'react';
import CreateQuery from './components/CreateQuery';
import DrawQuery from './components/DrawQuery';

function App() {
  return (
    <div className='App'>
      <CreateQuery />
      <DrawQuery />
      {/* <header className='App-header'>
        <img src={logo} className='App-logo' alt='logo' />
        <p>
          Edit <code>src/App.tsx</code> and save to reload!
        </p>
        <a
          className='App-link'
          href='https://reactjs.org'
          target='_blank'
          rel='noopener noreferrer'>
          Learn React
        </a>
      </header> */}
    </div>
  );
}

export default App;
