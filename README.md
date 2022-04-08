# BAG:EL
## Best Algorithms for Graphs: Easy Learning

A distributed graph processor based on the Pregel API. 

### Makefile Targets
 - `all` to build the `worker`, `coord` and `client`
 - `worker`
 - `coord`
 - `client`
 - `test` to run the unit test for the worker
 - `clean` to remove the build files and clean the cached test results

### Running the code
 - After building, the binary files will be found in the `./bin` folder
   - `./bin/client runs a client instance that can be used to queue up requests
     - `client shortestpath {vertex1} {vertex2}` runs a shortest path calculation from vertex1 to vertex2
     - `client pagerank {vertex}` finds the flow value of the given vertex in the stationary distribution given by the pagerank algorithm
   - `./bin/coord` runs a coordinator
   - `./bin/worker [workerId]` runs a worker node
