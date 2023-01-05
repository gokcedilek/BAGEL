import React, { useEffect, useState } from 'react';
import { CoordClient } from './proto/coord_grpc_web_pb';
import {
  Query,
  QUERY_TYPE,
  QueryProgressRequest,
  QueryProgressResponse,
  FetchGraphRequest,
  FetchGraphResponse,
  VertexMessage,
} from './proto/coord_pb';
// import * as d3 from 'd3';
import * as Viva from 'vivagraphjs';
import { useStateWithCallbackLazy } from 'use-state-with-callback';

const client = new CoordClient('http://localhost:8080', null, null);

// type Graph = {
//   [vertexId: number]: number;
// };
type Graph = {
  [workerId: number]: number[];
};

// type Message

const Test = () => {
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  const [superstepNumber, setSuperStepNumber] = useState(0);
  const [graph, setGraph] = useState<Graph>({});
  const [renderer, setRenderer] = useState<any>(null);
  // const [graph, setGraph] = useStateWithCallbackLazy<Graph>({});
  const [messages, setMessages] = useState<VertexMessage[]>([]);
  const [result, setResult] = useState<any>(null);

  useEffect(() => {
    drawInitialGraph();
  }, [graph]);

  const sendQuery = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const query = new Query([
      'clientId',
      QUERY_TYPE.SHORTEST_PATH,
      [srcId, destId],
      'xxx',
      'gokce-test-db',
    ]);
    client.startQuery(query, null, (err, response) => {
      // setGraph({});
      renderer.dispose();
      if (err) return console.log('startQuery err: ', err);
      console.log('startQuery response: ', response.toObject());
      setResult(response.toObject().result);
    });

    const fetchGraphRequest = new FetchGraphRequest();
    client.fetchGraph(fetchGraphRequest, null, (err, response) => {
      if (err) return console.log('fetchGraph err: ', err);
      console.log('fetchGraph response: ', response.toObject());

      const graphToDraw: Graph = {};
      response.toObject().workerverticesMap.forEach((workerVertices: any) => {
        const workerId = workerVertices[0];
        const vertices = workerVertices[1].verticesList;
        graphToDraw[workerId] = vertices;
      });

      setGraph(graphToDraw);
    });

    const queryProgressRequest = new QueryProgressRequest();
    const stream = client.queryProgress(queryProgressRequest, {});
    stream.on('data', (response: QueryProgressResponse) => {
      console.log('queryProgress data: ', response.toObject());
      // TODO populate ssn and messages state
    });
  };

  const randomColor = () => {
    const maxVal = 0xffffff;
    let randomNumber = Math.floor(Math.random() * maxVal).toString(16);
    const randomColor = randomNumber.padStart(6, '0');

    return `#${randomColor.toUpperCase()}`;
  };

  const drawInitialGraph = () => {
    const vivaGraph = Viva.Graph.graph();

    // vivaGraph.addNode('1');
    // vivaGraph.addNode('2');

    // vivaGraph.addLink('1', '2');

    for (let [workerId, vertices] of Object.entries(graph)) {
      const color = randomColor();
      for (let vertexId of vertices) {
        vivaGraph.addNode(vertexId, { color: color });
      }
    }

    const graphics = Viva.Graph.View.svgGraphics();
    graphics
      .node((node: { id: number; data: { color: string } }) => {
        const circle = Viva.Graph.svg('circle')
          .attr('id', node.id)
          .attr('cx', 7)
          .attr('cy', 7)
          .attr('r', 7)
          .attr('stroke', '#fff')
          .attr('stroke-width', '1.5px')
          .attr('fill', node.data.color);

        circle.append('title').text('test!');

        return circle;
      })
      .placeNode((nodeUI: any, pos: any) => {
        nodeUI.attr('cx', pos.x).attr('cy', pos.y);
      });

    const renderer = Viva.Graph.View.renderer(vivaGraph, {
      graphics: graphics,
      container: document.getElementById('graph-container'),
    });

    setRenderer(renderer);

    renderer.run();
  };

  return (
    <form onSubmit={sendQuery}>
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

export default Test;
