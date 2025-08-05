# Task Queue Manager

Built a scalable backend with NGINX, FastAPI, PostgreSQL, redis, dramatiq. Containerized with Docker.

![System Architecture](img/sys_arch.png)
## Metrics:
- Under light load (best case): >= 1500 RPS at < 7 msec latency
- The RPS is maintained even as the load increases from 10 to 160 concurrent-connections.

![Metrics : Latency vs RPS](img/metrics.jpg)
### Diagram:
- x, y axes: RPS, Latency (msec)
- The blue-bands represent the standard-deviation from the average-latency. The details are logged in *test_tqm/wrk.log*
- The integer written beside each dot is the number of concurrent-connections

## API Testing
- The load was generated with the tool [wrk](https://github.com/wg/wrk) on the slowest endpoint 'trigger'.
- The metrics were calculated on a dev setup: WSL, 8 logical-cores, processor: i5-113G7
  - For these measurements, the backend was run in WSL directly, rather than as docker-containers.

### To test the backend
1. launch it with: *docker compose up*
   - if a *mount error* occurs, then restart WSL. The error is stated in the file *test_tqm/wsl_mount.error*
2. register a user, with the file *test_tqm/register.py*
3. execute the shell script *test_tqm/bombard.sh*
