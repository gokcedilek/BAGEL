import React, { useEffect, useState } from 'react';
import { CoordClient } from './proto/coord_grpc_web_pb';
import {
  Query,
  QUERY_TYPE,
  QueryProgressRequest,
  QueryProgressResponse,
  FetchGraphRequest,
  FetchGraphResponse,
} from './proto/coord_pb';

const client = new CoordClient('http://localhost:8080', null, null);

const Form = () => {
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  // useEffect(() => {
  //   const sensorRequest = new SensorRequest();
  //   const stream = client.tempSensor(sensorRequest, {});
  //   stream.on('data', (response: SensorResponse) => {
  //     console.log('data object: ', response.toObject());
  //   });
  // }, []);

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
      if (err) return console.log('startQuery err: ', err);
      console.log('startQuery response: ', response.toObject());
    });

    const fetchGraphRequest = new FetchGraphRequest();
    client.fetchGraph(fetchGraphRequest, null, (err, response) => {
      if (err) return console.log('fetchGraph err: ', err);
      console.log('fetchGraph response: ', response.toObject());
    });

    const queryProgressRequest = new QueryProgressRequest();
    const stream = client.queryProgress(queryProgressRequest, {});
    stream.on('data', (response: QueryProgressResponse) => {
      console.log('queryProgress data: ', response.toObject());
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
