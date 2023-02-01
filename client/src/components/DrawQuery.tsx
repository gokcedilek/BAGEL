import React, { useEffect, useState } from 'react';

import { useQueryClient } from '../contexts/useQueryClient';
import { FetchGraphRequest } from '../proto/coord_pb';
import * as Viva from 'vivagraphjs';

type Graph = {
  [workerId: number]: number[];
};

const DrawQuery = () => {
  const { queryClient } = useQueryClient();
  const [graph, setGraph] = useState<Graph>({});
  const [renderer, setRenderer] = useState<any>(null);

  useEffect(() => {
    drawInitialGraph();
  }, [graph]);

  // const fetchGraphRequest = new FetchGraphRequest();
  // queryClient.fetchGraph(fetchGraphRequest, null, (err, response) => {
  //   if (err) return console.log('fetchGraph err: ', err);
  //   console.log('fetchGraph response: ', response.toObject());

  //   const graphToDraw: Graph = {};
  //   response.toObject().workerverticesMap.forEach((workerVertices: any) => {
  //     const workerId = workerVertices[0];
  //     const vertices = workerVertices[1].verticesList;
  //     graphToDraw[workerId] = vertices;
  //   });

  //   setGraph(graphToDraw);
  // });

  const randomColor = () => {
    const maxVal = 0xffffff;
    let randomNumber = Math.floor(Math.random() * maxVal).toString(16);
    const randomColor = randomNumber.padStart(6, '0');

    return `#${randomColor.toUpperCase()}`;
  };

  const drawInitialGraph = () => {
    const vivaGraph = Viva.Graph.graph();

    console.log('draw graph!');

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

  return <div id='graph-container'></div>;
};

export default DrawQuery;
