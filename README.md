# BAG:EL
## Best Algorithms for Graphs: Easy Learning

A distributed graph processor based on the Pregel API.
Possible operations are:
- finding the shortest path between two vertices
- finding the PageRank of a given vertex
  - in our implementation, the sum of the PageRanks across all vertices sum to |V|

### Makefile Targets
- `all` to build the `worker`, `coord`, `client`, `database`, and `cnf`
- `worker`
- `coord`
- `client`
- `database`
- `cnf`
- `test` to run the unit test for the worker
- `clean` to remove the build files and clean the cached test results

### Running the code
- After building, the binary files will be found in the `./bin` folder
- To configure the config files (`worker`, `coord`, `client`)
  - `./bin/cnf [sync|port|azure]`
    - `./bin/cnf sync` - will synchronize the `client` and `worker` config files to point to the correct `coord` port numbers
    - `./bin/cnf port` - will randomly assign ports (based on rand.Int()) to all the configuration files, and call `./bin/cnf sync` (automatically syncs worker and coord to the correct coord ports)
    - `./bin/cnf azure [coordServer] [clientServer]` - based on the list of Azure VM addresses, will assign the correct server addresses to all the configuration files.
      - `./bin/cnf azure [coordServer] [clientServer]`
        - `[coordServer]` - specify the name of the remote server for the coord to run on
        - `[clientServer]` - specify the name of the remote server for the client to run on
   - **(tl;dr)** To run on Azure servers
     1. `git checkout -b <branch_name>`
     2. `make cnf`
     3. `./bin/cnf port`
     4. `./bin/cnf azure [coordServer] [clientServer]`
     5. `git add . && git commit -m "azure" && git push origin <branch_name>`
     6. Take note of the assigned nodes (worker/coord/client) servers
        1. `[client_config.json assigned to server Gambier : 20.230.193.58
coord_config.json assigned to server Lulu : 20.83.241.160
worker0_config.json assigned to server Ivan : 52.175.222.198
worker1_config.json assigned to server Go : 20.98.67.22
worker2_config.json assigned to server Remote : 20.230.176.102
worker3_config.json assigned to server Anvil : 20.69.158.88
]`
     7. ssh into the Azure VMs
     8. Pull your branch `git fetch -v - a && git switch <branch_name>`
     9. `make clean all`
     10. Run `./bin/[worker|coord|client]` 
         1. based on the VM you are on and the output seen above 
         2. (ie. `client_config.json assigned to server Gambier` therefore, run `./bin/coord` on Gambier VM)
- Run the following in order to issue a query:
  - `./bin/coord` runs a coordinator
  - `./bin/worker [workerId]` runs a worker node
  - `./bin/client` runs a client instance that can be used to queue up requests
    - `client shortestpath {vertex1} {vertex2}` runs a shortest path calculation from vertex1 to vertex2
    - `client pagerank {vertex}` finds the PageRank of the vertex

### Run the code with Docker

- Create a user-defined bridge network: `docker network create bagel`
- Start coord:
  - Build: `docker build -f Dockerfile.coord -t coord .`
  - Remove previous container: `docker rm coord`
  - Run: `docker run --name coord --network bagel coord`
- Start worker(s):
  - Build: `docker build -f Dockerfile.worker -t worker .`
  - Remove previous container: `docker rm worker0`
  - Run: `docker run --name worker0 --network bagel worker 0`

### Run the code with Docker-compose

- `docker compose build` (not necessarily needed)
- `docker compose up`
