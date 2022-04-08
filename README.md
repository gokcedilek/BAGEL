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

### Run the app
 - `./bin/coord`
 - `./bin/worker [workerId]`
 - `./bin/client [query] [...args]`
