import React, { useState } from 'react';
// import axios from 'axios';
// CoordClient from .grpc.pb
// Query from .pb
import { CoordClient } from './proto/coord_grpc_web_pb';
import { Query, QUERY_TYPE } from './proto/coord_pb';
import protobuf from 'protobufjs';

// const client = new CoordClient('https://localhost:8080', null, null);
const client = new CoordClient('http://localhost:8080', null, null);

const Form = () => {
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const query = new Query([
      'clientId',
      QUERY_TYPE.SHORTEST_PATH,
      [srcId, destId],
      'xxx',
      'gokce-test-db',
    ]);
    client.startQuery(query, null, (err, response) => {
      if (err) return console.log('err: ', err);
      console.log('response: ', response.toObject());
    });
  };

  return (
    <form onSubmit={handleSubmit}>
      <label>
        Source ID:
        <input
          type='text'
          value={srcId}
          onChange={(e) => setSrcId(e.target.value)}
        />
      </label>
      <label>
        Destination ID:
        <input
          type='text'
          value={destId}
          onChange={(e) => setDestId(e.target.value)}
        />
      </label>
      <input type='submit' />
    </form>
  );
};

export default Form;
