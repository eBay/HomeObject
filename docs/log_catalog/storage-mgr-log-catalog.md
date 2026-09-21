# Log Catalog

All log statements in the storage_mgr codebase, organized by log module (`logmod`) and log level.

`SVC_LOG*` macros (defined in `services.hpp`) expand to `LOG*MOD(module, "[req_id={}] " msg, req_id, ...)`.  
All `SVC_*` log messages therefore automatically include a `[req_id=N]` prefix at runtime.

Log modules registered via `SISL_LOGGING_DEF` / `SISL_LOGGING_INIT`:

| Module      | Source file                                                                       |
|-------------|-----------------------------------------------------------------------------------|
| `cm_client` | `src/clients/cm_client/cm_client.cpp`                                             |
| `pg_svc`    | `src/services/pg_svc/pg_svc_calls.cpp`                                            |
| `shard_svc` | `src/services/shard_svc/shard_svc_calls.cpp`                                      |
| `data_svc`  | `src/services/data_svc/data_svc_calls.cpp`                                        |
| _(global)_  | `src/lib/sm_lib.cpp`, `sm_config.cpp`, `main.cpp`, `http_svc.cpp`, `services.hpp` |

---

## Module: `cm_client`

### DEBUG

| Line                | Message template                                                        | Context                                            |
|---------------------|-------------------------------------------------------------------------|----------------------------------------------------|
| `cm_client.cpp:66`  | `Receive Cm Request successfully: {status_msg}, method {method}`        | gRPC call succeeded, no RPC error                  |
| `cm_client.cpp:74`  | `cm not leader, received new cm leader: {leader_addr}, method {method}` | CM replied NOT_RAFT_LEADER with a redirect address |
| `cm_client.cpp:99`  | `Sending heartbeat, req {proto_debug_string}`                           | Before every `HeartBeat` gRPC call                 |
| `cm_client.cpp:109` | `Sending LookupSmSvc for uuid: [{uuid}], req {proto_debug_string}`      | Before `LookupSmSvc` gRPC call                     |
| `cm_client.cpp:129` | `Sending PgStatus Report`                                               | Before `ReportPgStatus` gRPC call                  |

### INFO

| Line                | Message template           | Context                                             |
|---------------------|----------------------------|-----------------------------------------------------|
| `cm_client.cpp:146` | `Got Back SvcId: [{uuid}]` | `RegisterSmSvc` succeeded and returned a valid UUID |

### ERROR

| Line                | Message template                                               | Context                                                   |
|---------------------|----------------------------------------------------------------|-----------------------------------------------------------|
| `cm_client.cpp:77`  | `Receive error, method: {method}, error: {error_case_name}`    | gRPC OK but RPC-level error (not timeout, not NOT_LEADER) |
| `cm_client.cpp:81`  | `gRPC timeout, method: {method}`                               | gRPC `DEADLINE_EXCEEDED`                                  |
| `cm_client.cpp:84`  | `gRPC message failed: {error_message}, method: {method}`       | gRPC transport failure (generic)                          |
| `cm_client.cpp:116` | `gRPC message failed: {error_message} dor lookup node: {uuid}` | `LookupSmSvc` gRPC transport failure                      |
| `cm_client.cpp:152` | `gRPC message failed: {error_message}`                         | `RegisterSmSvc` gRPC transport failure                    |

### CRITICAL

| Line                | Message template          | Context                                              |
|---------------------|---------------------------|------------------------------------------------------|
| `cm_client.cpp:149` | `Invalid SvcId: [{uuid}]` | `RegisterSmSvc` returned a UUID that failed to parse |

---

## Module: `pg_svc`

All `SVC_LOG*` messages include `[req_id={req_id}]` prefix automatically.

### INFO

| Line                   | Message template                                                           | Context                                                 |
|------------------------|----------------------------------------------------------------------------|---------------------------------------------------------|
| `pg_svc_calls.cpp:299` | `[req_id={N}] CM has requested that we change our client to use: {leader}` | `notifyCmChange`: received non-empty new leader address |

### WARN

| Line                   | Message template                                                                                          | Context                                                   |
|------------------------|-----------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `pg_svc_calls.cpp:230` | `[req_id={N}] We do not have the name of the service at ID: {uuid} for [pg={pg_id}], [task_id={task_id}]` | `pgMemberChange`: incoming member has an empty name field |

### ERROR

| Line                   | Message template                                                                                          | Context                                                 |
|------------------------|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| `pg_svc_calls.cpp:132` | `[req_id={N}] PG ID too large [max: 65535]!: [{pg_id}]`                                                   | `createPg`: pg_id exceeds UINT16_MAX                    |
| `pg_svc_calls.cpp:140` | `[req_id={N}] PG size not set!: [{pg_id}]`                                                                | `createPg`: pg_size_bytes == 0                          |
| `pg_svc_calls.cpp:150` | `[req_id={N}] Invalid member count during createPg: [{n} members]`                                        | `createPg`: member list is empty                        |
| `pg_svc_calls.cpp:166` | `[req_id={N}] SvcId uuid invalid: [{uuid}: {exception}]`                                                  | `createPg`: UUID string fails boost parsing             |
| `pg_svc_calls.cpp:176` | `[req_id={N}] SvcIds should have unique names, found: [{unique}/{total}] names`                           | `createPg`: duplicate member names                      |
| `pg_svc_calls.cpp:209` | `[req_id={N}] PG ID too large [max: 65535]!: [{pg_id}]`                                                   | `pgMemberChange`: pg_id exceeds UINT16_MAX              |
| `pg_svc_calls.cpp:226` | `[req_id={N}] SvcId uuid invalid: [{out_uuid}=>{in_uuid}]: {exception} [pg={pg_id}], [task_id={task_id}]` | `pgMemberChange`: member UUID fails boost parsing       |
| `pg_svc_calls.cpp:238` | `[req_id={N}] Duplicate uuid provided: [{uuid}] [pg={pg_id}], [task_id={task_id}]`                        | `pgMemberChange`: member_in UUID equals member_out UUID |
| `pg_svc_calls.cpp:274` | `[req_id={N}] SvcId uuid invalid: [{out_uuid}=>{in_uuid}]: {exception} [pg={pg_id}], [task_id={task_id}]` | `getPgMemberChangeStatus`: UUID fails boost parsing     |
| `pg_svc_calls.cpp:307` | `[req_id={N}] Missing leader information`                                                                 | `notifyCmChange`: received empty leader string          |

---

## Module: `shard_svc`

All `SVC_LOG*` messages include `[req_id={req_id}]` prefix automatically.

### DEBUG

| Line                      | Message template                                                                        | Context                                      |
|---------------------------|-----------------------------------------------------------------------------------------|----------------------------------------------|
| `shard_svc.cpp:47`        | `Registering ShardSvc with gRPC Server.`                                                | Service registration at startup              |
| `shard_svc_calls.cpp:218` | `[req_id={N}] CreateShard request: pg_id={pg_id}, size={size}, latency_us={latency_us}` | `createShard` completed within 1 s threshold |
| `shard_svc_calls.cpp:238` | `[req_id={N}] SealShard request: shard_id={shard_id}, latency_us={latency_us}`          | `sealShard` completed within 1 s threshold   |

### WARN

| Line                      | Message template                                                                               | Context                              |
|---------------------------|------------------------------------------------------------------------------------------------|--------------------------------------|
| `shard_svc_calls.cpp:215` | `[req_id={N}] [SLOW] CreateShard request: pg_id={pg_id}, size={size}, latency_us={latency_us}` | `createShard` latency > 1,000,000 µs |
| `shard_svc_calls.cpp:235` | `[req_id={N}] [SLOW] SealShard request: shard_id={shard_id}, latency_us={latency_us}`          | `sealShard` latency > 1,000,000 µs   |

### ERROR

| Line                      | Message template                                           | Context                                 |
|---------------------------|------------------------------------------------------------|-----------------------------------------|
| `shard_svc_calls.cpp:116` | `[req_id={N}] PG ID too large [max: 65535]!: [{pg_id}]`    | `createShard`: pg_id exceeds UINT16_MAX |
| `shard_svc_calls.cpp:125` | `[req_id={N}] Shard Size too small [min: 4096]!: [{size}]` | `createShard`: size_bytes < 4 KiB       |
| `shard_svc_calls.cpp:186` | `[req_id={N}] PG ID too large [max: 65535]!: [{pg_id}]`    | `listShards`: pg_id exceeds UINT16_MAX  |

---

## Module: `data_svc`

All `SVC_LOG*` messages include `[req_id={req_id}]` prefix automatically.

### DEBUG

| Line                     | Message template                                                                                          | Context                                   |
|--------------------------|-----------------------------------------------------------------------------------------------------------|-------------------------------------------|
| `data_svc.cpp:39`        | `Registering data service`                                                                                | Service registration at startup           |
| `data_svc_calls.cpp:167` | `[req_id={N}] Read request: shard_id={shard_id}, blob_id={blob_id}, size={size}, latency_us={latency_us}` | `read` succeeded (only on success path)   |
| `data_svc_calls.cpp:180` | `[req_id={N}] Write request: shard_id={shard_id}, size={size}, latency_us={latency_us}`                   | `write` succeeded (only on success path)  |
| `data_svc_calls.cpp:192` | `[req_id={N}] Delete request: shard_id={shard_id}, blob_id={blob_id}, latency_us={latency_us}`            | `delete` succeeded (only on success path) |

### WARN

| Line                     | Message template                                                                                  | Context                                                       |
|--------------------------|---------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `data_svc_calls.cpp:127` | `[req_id={N}] MD5 mismatch [req: {req_checksum}] != [calc: {calc_checksum}]! [shard: {shard_id}]` | `write`: MD5 provided by client does not match computed value |

---

## Global (no module)

Used by `sm_lib.cpp`, `sm_config.cpp`, `main.cpp`, `http_svc.cpp`, `cluster_mgr.hpp`, `sm_security.hpp`, and the `pre_process` helper in `services.hpp`.

### DEBUG

| Line                  | Message template                                                                                                | Context                                                                           |
|-----------------------|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `sm_lib.cpp:251`      | `Sending heartbeat for [sm:{svc_uuid}]`                                                                         | Heartbeat loop tick, before calling `cm_client->heartbeat()`                      |
| `sm_lib.cpp:356`      | `Loading pg status for [sm:{svc_uuid}]`                                                                         | `load_all_pg_status()` entry                                                      |
| `sm_lib.cpp:504`      | `Local IP Address {ip}`                                                                                         | After resolving local IP via `getifaddrs`                                         |
| `cluster_mgr.hpp:67`  | `Adjusting CM Client to: [{addr}]`                                                                              | `update_client()`: switching active CM address to a newly discovered leader       |
| `cluster_mgr.hpp:98`  | `Attempt to access CM endpoint: [{addr}]`                                                                       | `probe_and_update_active_endpoint()`: probing each configured endpoint            |
| `cluster_mgr.hpp:104` | `Fail to access CM endpoint: [{addr}]`                                                                          | `probe_and_update_active_endpoint()`: endpoint returned `GRPC_FAIL` or `TIME_OUT` |
| `cluster_mgr.hpp:121` | `Attempt[{i}] to [{action}]`                                                                                    | `retry()`: each retry iteration before calling the remote function                |
| `services.hpp`        | `Received a request, in_req_id: {client_req_id}, out_req_id: {server_req_id}`                                   | Every incoming gRPC request in `pre_process`                                      |
| `services.hpp`        | `Received request with empty subcluster id, reqId: {client_req_id}, IGNORE!`                                    | `pre_process`: sc header absent but `sc_header_auth_enabled=false`                |
| `services.hpp`        | `Received invalid request due to mismatched subcluster id: {got}, expected: {expected}, reqId: {client_req_id}` | `pre_process`: sc header mismatch but `sc_header_auth_enabled=false`              |

### INFO

| Line               | Message template                                                                                   | Context                                          |
|--------------------|----------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `sm_lib.cpp:119`   | `Adding device: [{device}]`                                                                        | Iterating configured `use_devices` list          |
| `sm_lib.cpp:129`   | `Found configured mem_size: {total}, reserved mem_size: {reserved}, available memory: {available}` | `mem_size()` called                              |
| `sm_lib.cpp:136`   | `Found configured max data size: {size}`                                                           | `max_data_size()` called                         |
| `sm_lib.cpp:147`   | `HomeObject returned SvcId: {uuid}, this is a restart`                                             | Persistence found a prior SvcId on disk          |
| `sm_lib.cpp:206`   | `Starting up NuObject Ctrl services on address {addr}`                                             | `start_ctrl_services()` entry                    |
| `sm_lib.cpp:224`   | `Starting data services on {addr} with {threads} threads`                                          | `start_data_services()` entry                    |
| `sm_lib.cpp:236`   | `Starting http service`                                                                            | `init_http_services()` entry                     |
| `sm_lib.cpp:248`   | `Starting heartbeat loop`                                                                          | `run_heartbeat_loop()` entry                     |
| `sm_lib.cpp:267`   | `SSL is disabled`                                                                                  | `start_cert_watcher()` with `ssl_enabled=false`  |
| `sm_lib.cpp:270`   | `SSL is enabled, starting monitor cert`                                                            | `start_cert_watcher()` with `ssl_enabled=true`   |
| `sm_lib.cpp:278`   | `file change event for {filepath}, deleted? {deleted}`                                             | File-watcher callback triggered                  |
| `sm_lib.cpp:303`   | `file {filepath} deleted, `                                                                        | File deleted and `wait_for_cert` timed out       |
| `sm_lib.cpp:313`   | `There are {n} restart requests pending, will not restart services now. filepath: {filepath}`      | Debouncing multiple rapid cert changes           |
| `sm_lib.cpp:317`   | `Restarting services for file change event on {filepath}`                                          | Proceeding with cert-triggered restart           |
| `sm_lib.cpp:323`   | `Waiting for {n} pending requests to finish before restarting services`                            | Draining in-flight requests before restart       |
| `sm_lib.cpp:326`   | `All pending requests finished, restarting services`                                               | Drain complete, restart proceeding               |
| `sm_config.cpp:52` | `Setting max data size to {size}`                                                                  | CLI option `--max_data_size` overrides config    |
| `sm_config.cpp:76` | `Setting log levels, log_mods={log_mods_string}`                                                   | `log_mods` config is non-empty                   |
| `sm_config.cpp:98` | `No log_mods specified, using default log levels`                                                  | `log_mods` config is empty                       |
| `http_svc.cpp:43`  | `Received request to shutdown Storage Manager`                                                     | `POST /api/v1/shutdownSM` received               |
| `http_svc.cpp:57`  | `Shutting down Storage Manager with mode: {mode}, signal: {signal}`                                | Valid shutdown mode accepted                     |
| `http_svc.cpp:137` | `Restarting because of config change which needed a restart`                                       | `POST /api/v1/config` triggered a restart        |
| `main.cpp:35`      | `Going to sleep for {seconds} seconds`                                                             | `#ifdef _PRERELEASE` only; `--init_sleep` option |

### WARN

| Line                 | Message template                                             | Context                                                                                 |
|----------------------|--------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `sm_lib.cpp:150`     | `First time start-up, will request ID from CM`               | No SvcId found in persistence                                                           |
| `sm_lib.cpp:161`     | `fail to register to cm , will retry after 2s..`             | `register_svc` to CM failed; will retry (up to 10 times)                                |
| `sm_lib.cpp:285`     | `SmApplication instance is no longer valid`                  | Cert-watcher callback fired after `SmApplication` was destroyed                         |
| `sm_security.hpp:51` | `Fail to find a valid trf token client from ioenvironment`   | `get_trf_token_client()`: `auth_enabled=true` but ioenvironment has no token client     |
| `sm_security.hpp:61` | `Fail to find a valid trf token verifier from ioenvironment` | `get_trf_token_verifier()`: `auth_enabled=true` but ioenvironment has no token verifier |
| `http_svc.cpp:52`    | `Invalid shutdown mode received: {mode}`                     | `shutdown_mode` query param is not `graceful` or `force`                                |
| `main.cpp:79`        | `SIGNAL: {signal_name}`                                      | SIGINT or SIGTERM received                                                              |

### ERROR

| Line                 | Message template                                                                                                | Context                                                                                                 |
|----------------------|-----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `cluster_mgr.hpp:33` | `No CM endpoints found in the file: [{path}]`                                                                   | Constructor: CM endpoints config file is empty or missing entries; throws `runtime_error`               |
| `cluster_mgr.hpp:84` | `Fail to find a valid CM endpoint`                                                                              | `adaptive_forward()`: all forwarding attempts exhausted and `probe_and_update_active_endpoint()` failed |
| `cluster_mgr.hpp:93` | `No valid cm endpoint found`                                                                                    | `probe_and_update_active_endpoint()`: `_cm_endpoints` list is empty                                     |
| `sm_lib.cpp:245`     | `not a valid exit future!`                                                                                      | `run_heartbeat_loop` received an invalid `std::future`                                                  |
| `sm_lib.cpp:291`     | `Failed to register listener, sm_ssl_cert_watcher to watch file {cert_file}, Not monitoring cert files`         | SSL cert file watcher registration failed                                                               |
| `sm_lib.cpp:296`     | `Failed to register listener, sm_ssl_key_watcher to watch file {key_file}, Not monitoring cert files`           | SSL key file watcher registration failed                                                                |
| `sm_lib.cpp:366`     | `Failed to get pg stats for pg: {pg_id}`                                                                        | `get_stats()` returned false for a PG                                                                   |
| `sm_lib.cpp:452`     | `Failed to load version: {version}, name: {name}, error: {exception}`                                           | `Semver200_version` parsing threw on the build version string                                           |
| `sm_config.cpp:94`   | `Unable to setup the module [{}] in registered modules, error: {dlerror}`                                       | `dlsym` failed to find `module_level_<name>` symbol for a log mod                                       |
| `http_svc.cpp:30`    | `setup routes failed, {exception}`                                                                              | `http_server->setup_routes()` threw                                                                     |
| `main.cpp:81`        | `Failed to set stop code: {exception}`                                                                          | Second SIGTERM received; `promise::set_value` throws `future_error`                                     |
| `main.cpp:85`        | `Unhandled SIGNAL: {signal_name}`                                                                               | Signal other than SIGINT/SIGTERM received                                                               |
| `services.hpp`       | `Received request while service is stopping, in_req_id: {client_req_id}`                                        | `pre_process`: request arrived after `stop_svc=true`                                                    |
| `services.hpp`       | `Received invalid request with empty subcluster id, reqId: {client_req_id}`                                     | `pre_process`: sc header absent and `sc_header_auth_enabled=true`                                       |
| `services.hpp`       | `Received invalid request due to mismatched subcluster id: {got}, expected: {expected}, reqId: {client_req_id}` | `pre_process`: sc header mismatch and `sc_header_auth_enabled=true`                                     |
