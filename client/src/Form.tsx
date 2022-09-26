import React, { useState } from 'react';
// import axios from 'axios';
// CoordClient from .grpc.pb
// Query from .pb
import { CoordClient } from './proto/coord_grpc_web_pb';
import { Query } from './proto/coord_pb';

const client = new CoordClient('https://localhost:8080', null, null);

const Form = () => {
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    // console.log('srcId', srcId);
    // console.log('destId', destId);
    // TODO: fix CORS issue
    // try {
    //   const response = await axios.get('http://localhost:56837/shortestpath');
    //   console.log('response: ', response);
    // } catch (e) {
    //   console.log('error: ', e);
    // }
    const query = new Query(['testClientId', 'test', [1, 2, 3], 'test']);
    client.startQuery(query, null, (err, response) => {
      console.log('err: ', err);
      const res = response.toObject();
      console.log('res: ', res);
    });
    /*
    {
		ClientId:  "testclientId",
		QueryType: "test",
		Nodes:     []uint64{1, 2, 3},
		Graph:     "test",
	}
    */
  };

  return (
    <form onSubmit={handleSubmit}>
      <label>
        Source ID:
        <input
          type='text'
          // name='username'
          value={srcId}
          onChange={(e) => setSrcId(e.target.value)}
        />
      </label>
      <label>
        Destination ID:
        <input
          type='text'
          // name='age'
          value={destId}
          onChange={(e) => setDestId(e.target.value)}
        />
      </label>
      <input type='submit' />
    </form>
  );
};

export default Form;
