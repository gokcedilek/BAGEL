import React, { useState } from 'react';
// import axios from 'axios';
// CoordClient from .grpc.pb
// Query from .pb
import { CoordClient } from './proto/coord_grpc_web_pb';
import { Query } from './proto/coord_pb';
import protobuf from 'protobufjs';

// const client = new CoordClient('https://localhost:8080', null, null);
const client = new CoordClient('http://localhost:8080', null, null);

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
    const query = new Query(['testId', 'test', [1, 2, 4], 'test']);
    client.startQuery(query, null, (err, response) => {
      if (err) return console.log('err: ', err);
      console.log('response: ', response.toObject());

      // TODO: attempt to fix deserializing ANY on the client side
      // protobuf.load('./proto/coord.proto', (err, root) => {
      //   if (err) {
      //     console.log('err: ', err);
      //     return;
      //   }
      //   const Result = root?.lookupType('QueryResult.Result');
      //   // response.getResultAs
      //   console.log('Result: ', Result);
      //   const message = Result?.decode(response);
      //   console.log('message: ', message);
      //   // const object = Result.toObject(message, {
      //   //   longs: String,
      //   //   enums: String,
      //   //   bytes: String,
      //   // });
      //   // console.log('object: ', object);
      // });
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
