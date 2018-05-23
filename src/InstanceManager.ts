"use strict";
import _ = require("lodash");
import arangojs from "arangojs";
import { UriOptions, UrlOptions } from "request";
import DockerRunner from "./DockerRunner";
import { FailoverError } from "./Errors";
import Instance from "./Instance";
import LocalRunner from "./LocalRunner";
import Runner from "./Runner";
import { compareTicks, endpointToUrl } from "./common";
import dd = require("dedent");
import rp = require("request-promise");

// Arango error code for "shutdown in progress"
const ERROR_SHUTTING_DOWN = 30;

const WAIT_TIMEOUT = 400; // seconds

const debugLog = (...logLine: any[]) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
    console.log(new Date().toISOString(), ...logLine);
  }
};

// 180s, after this time we kill -9 the instance
const shutdownKillAfterSeconds = 180;

// 200s, note that the cluster internally has a 120s timeout
const shutdownGiveUpAfterSeconds = 200;

// We check every 50ms if a server is down
const shutdownWaitInterval = 50;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export default class InstanceManager {
  singleServerCounter: number;
  dbServerCounter: number;
  coordinatorCounter: number;
  agentCounter: number;
  currentLog: string;
  storageEngine: "rocksdb" | "mmfiles";
  runner: Runner;
  instances: Instance[];

  constructor(
    pathOrImage: string = "../arangodb",
    runner: "local" | "docker" = "local",
    storageEngine: "rocksdb" | "mmfiles" = "mmfiles"
  ) {
    this.instances = [];

    if (runner === "local") {
      this.runner = new LocalRunner(pathOrImage);
    } else if (runner === "docker") {
      this.runner = new DockerRunner(pathOrImage);
    } else {
      throw new Error("Unkown runner type");
    }

    this.storageEngine = storageEngine;

    if (!this.runner) {
      throw new Error(
        'Must specify RESILIENCE_ARANGO_BASEPATH (source root dir including a "build" folder containing compiled binaries or RESILIENCE_DOCKER_IMAGE to test a docker container'
      );
    }
    this.currentLog = "";
    this.agentCounter = 0;
    this.coordinatorCounter = 0;
    this.dbServerCounter = 0;
    this.singleServerCounter = 0;
  }

  async rpAgency(
    o: UriOptions & rp.RequestPromiseOptions
  ): Promise<rp.RequestPromise> {
    let count = 0;
    let delay = 100;
    o.followAllRedirects = true;
    while (true) {
      try {
        return await rp(o);
      } catch (e) {
        if (e.statusCode !== 503 && count++ < 100) {
          throw e;
        }
      }
      await sleep(delay);
      delay = delay * 2;
      if (delay > 8000) {
        delay = 8000;
      }
    }
  }

  async rpAgencySingleWrite(
    o: UrlOptions & rp.RequestPromiseOptions
  ): Promise<rp.RequestPromise> {
    // This can be used if the body of the request contains a single writing
    // transaction which contains a clientID as third element. If we get a
    // 503 we try /_api/agency/inquire until we get a definitive answer as
    // to whether the call has worked or not.
    if (
      !Array.isArray(o.body) ||
      o.body.length !== 1 ||
      !Array.isArray(o.body[0]) ||
      o.body[0].length !== 3 ||
      typeof o.body[0][2] !== "string"
    ) {
      throw new Error("Illegal use of rpAgencySingleWrite!");
      //return this.rpWrite(o);
    }
    let count = 0;
    let delay = 100;
    o.followAllRedirects = true;
    let isInquiry = false;
    while (true) {
      if (!isInquiry) {
        try {
          return await rp(o);
        } catch (e) {
          if (e.statusCode !== 503 && count++ < 100) {
            throw e;
          }
          isInquiry = true; // switch to inquiry mode
        }
      } else {
        let oo = {
          method: o.method,
          url: (o.url as string).replace("write", "inquire"),
          json: o.json,
          body: [o.body[0][2]],
          followAllRedirects: true
        };
        let res;
        // If this throws, we fail:
        res = await rp(oo);
        if (
          Array.isArray(res.body) &&
          res.body.length == 1 &&
          Array.isArray(res.body[0])
        ) {
          if (res.body[0].length == 1 && typeof res.body[0][0] == "number") {
            res.body = res.body[0];
            return res; // this is a bit of a fake because the URL is now
            // /_api/agency/inquire, but never mind.
          } else if (res.body[0].length == 0) {
            isInquiry = false; // try again normally
          } else {
            throw new Error("Illegal answer from /_api/agency/inquire");
          }
        } else {
          throw new Error("Illegal answer from /_api/agency/inquire");
        }
      }
      await sleep(delay);
      delay = delay * 2;
      if (delay > 8000) {
        delay = 8000;
      }
    }
  }

  private startArango(
    name: string,
    endpoint: string,
    role: string,
    args: string[]
  ): Promise<Instance> {
    args.push("--server.authentication=false");
    //args.push('--log.level=v8=debug')

    if (process.env.LOG_COMMUNICATION && process.env.LOG_COMMUNICATION !== "") {
      args.push(`--log.level=communication=${process.env.LOG_COMMUNICATION}`);
    }

    if (process.env.LOG_REQUESTS && process.env.LOG_REQUESTS !== "") {
      args.push(`--log.level=requests=${process.env.LOG_REQUESTS}`);
    }

    if (process.env.LOG_AGENCY && process.env.LOG_AGENCY !== "") {
      args.push(`--log.level=agency=${process.env.LOG_AGENCY}`);
    }

    args.push(`--server.storage-engine=${this.storageEngine}`);

    if (process.env.ARANGO_EXTRA_ARGS) {
      args.push(...process.env.ARANGO_EXTRA_ARGS.split(" "));
    }

    const instance: Instance = {
      name,
      role,
      process: null,
      status: "NEW",
      exitcode: null,
      endpoint,
      args,
      logFn: (line: string) => {
        if (line.trim().length > 0) {
          let logLine = `${instance.name}(${instance.process!.pid}): \t${line}`;

          if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
            console.log(logLine);
          } else {
            logLine = logLine.replace(/\x1B/g, "");
            this.currentLog += logLine + "\n";
          }
        }
      }
    };
    return this.runner.firstStart(instance);
  }

  async startSingleServer(
    nameIn: string,
    num: number = 1
  ): Promise<Instance[]> {
    const newInst = [];
    for (let i = 0; i < num; i++) {
      const name = `${nameIn}-${++this.singleServerCounter}`;
      const ep = await this.runner.createEndpoint();
      const inst = await this.startArango(name, ep, "single", [
        `--cluster.agency-endpoint=${this.getAgencyEndpoint()}`,
        `--cluster.my-role=SINGLE`,
        `--cluster.my-address=${ep}`,
        `--replication.automatic-failover=true`
      ]);
      this.instances.push(inst);
      newInst.push(inst);
    }
    return newInst;
  }

  async startDbServer(name: string): Promise<Instance> {
    this.dbServerCounter++;
    const endpoint = await this.runner.createEndpoint();
    const args = [
      `--cluster.agency-endpoint=${this.getAgencyEndpoint()}`,
      `--cluster.my-role=PRIMARY`,
      `--cluster.my-address=${endpoint}`
    ];
    const instance = await this.startArango(name, endpoint, "primary", args);
    this.instances.push(instance);
    return instance;
  }

  private getAgencyEndpoint(): string {
    return this.instances.filter(instance => {
      return instance.role === "agent" && instance.status === "RUNNING";
    })[0].endpoint;
  }

  async startCoordinator(name: string): Promise<Instance> {
    this.coordinatorCounter++;
    const endpoint = await this.runner.createEndpoint();
    const args = [
      "--cluster.agency-endpoint=" + this.getAgencyEndpoint(),
      "--cluster.my-role=COORDINATOR",
      "--cluster.my-address=" + endpoint,
      "--log.level=requests=trace"
    ];
    const instance = await this.startArango(
      name,
      endpoint,
      "coordinator",
      args
    );
    this.instances.push(instance);
    return instance;
  }

  async replace(instance: Instance): Promise<Instance> {
    let name, role;
    switch (instance.role) {
      case "coordinator":
        this.coordinatorCounter++;
        name = "coordinator-" + this.coordinatorCounter;
        role = "COORDINATOR";
        break;
      case "primary":
        this.dbServerCounter++;
        name = "dbServer-" + this.dbServerCounter;
        role = "PRIMARY";
        break;
      default:
        throw new Error("Can only replace coordinators/dbServers");
    }
    if (this.instances.includes(instance)) {
      throw new Error("Instance must be destroyed before it can be replaced");
    }
    let args = [
      "--cluster.agency-endpoint=" + this.getAgencyEndpoint(),
      "--cluster.my-role=" + role,
      "--cluster.my-address=" + instance.endpoint
    ];
    instance = await this.startArango(
      name,
      instance.endpoint,
      instance.role,
      args
    );
    await this.waitForInstance(instance);
    this.instances.push(instance);
    return instance;
  }

  async startAgency(options: any = {}): Promise<Instance[]> {
    debugLog("starting agencies");
    let size = options.agencySize || 1;
    if (options.agencyWaitForSync === undefined) {
      options.agencyWaitForSync = false;
    }
    let compactionStep = "200";
    let compactionKeep = "100";
    if (process.env.AGENCY_COMPACTION_STEP) {
      compactionStep = process.env.AGENCY_COMPACTION_STEP;
    }
    if (process.env.AGENCY_COMPACTION_KEEP) {
      compactionKeep = process.env.AGENCY_COMPACTION_KEEP;
    }

    let firstEndpoint = null;
    let instances = [];
    for (let i = 0; i < size; i++) {
      const endpoint = await this.runner.createEndpoint();
      if (firstEndpoint === null) {
        firstEndpoint = endpoint;
      }
      const args = [
        "--agency.activate=true",
        "--agency.size=" + size,
        "--agency.pool-size=" + size,
        "--agency.wait-for-sync=true",
        "--agency.supervision=true",
        "--server.threads=16",
        "--agency.supervision-frequency=0.5",
        "--agency.supervision-grace-period=2.5",
        "--agency.compaction-step-size=" + compactionStep,
        "--agency.compaction-keep-size=" + compactionKeep,
        "--agency.my-address=" + endpoint,
        `--agency.endpoint=${firstEndpoint}`
      ];
      this.agentCounter++;
      debugLog("booting agency");
      const instance = this.startArango(
        "agency-" + this.agentCounter,
        endpoint,
        "agent",
        args
      );
      instances.push(instance);
    }

    return (this.instances = await Promise.all(instances));
  }

  async dumpAgency(): Promise<void> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());

    try {
      const body = await rp({
        method: "POST",
        json: true,
        uri: `${baseUrl}/_api/agency/read`,
        followAllRedirects: true,
        body: [["/"]]
      });
      console.error(JSON.stringify(body));
    } catch (e) {}
  }

  /// Lookup the async failover leader in agency
  async asyncReplicationLeaderId(): Promise<string | null> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());

    let body;
    try {
      body = await rp({
        method: "POST",
        json: true,
        uri: `${baseUrl}/_api/agency/read`,
        followAllRedirects: true,
        body: [["/arango/Plan"]]
      });
    } catch (e) {
      return null;
    }

    const leader = body[0].arango.Plan.AsyncReplication.Leader;
    if (!leader) {
      return null;
    }
    const servers = Object.keys(body[0].arango.Plan.Singles);
    if (-1 === servers.indexOf(leader)) {
      throw new Error(
        `AsyncReplication: Leader ${leader} not one of single servers`
      );
    }

    let singles = this.singleServers();
    if (servers.length !== singles.length) {
      throw new Error(
        `AsyncReplication: Requested ${singles.length}, but ${
          servers.length
        } ready`
      );
    }
    return leader;
  }

  /// wait for leader selection (leader key is in agency)
  async asyncReplicationLeaderSelected(ignore = null, timeoutSec = 25) {
    let i = timeoutSec * 10; // should be max 30s according to spec
    while (i-- > 0) {
      let val = await this.asyncReplicationLeaderId();
      if (val !== null && ignore !== val) {
        return val;
      }
      await sleep(100);
    }
    let uuid = await this.asyncReplicationLeaderId();
    console.error("Timout waiting for leader selection");
    if (uuid) {
      console.error(
        "Invalid leader in agency: %s (%s)",
        uuid,
        this.resolveUUID(uuid)
      );
    } else {
      console.error("No leader in agency");
    }
    throw new Error("Timout waiting for leader selection");
  }

  /// look into the agency and return the master instance
  /// assumes leader is in agency, does not try again
  async asyncReplicationLeaderInstance() {
    const uuid = await this.asyncReplicationLeaderId();
    if (uuid == null) {
      throw "leader is not in agency";
    }
    let instance = await this.resolveUUID(uuid);
    if (!instance) {
      console.error("Could not find leader instance locally");
      throw new Error("Could not find leader instance locally");
    }
    console.log("Leader in agency %s (%s)", uuid, instance.endpoint);
    // we need to wait for the server to get out of maintenance mode
    await this.asyncWaitInstanceOperational(instance.endpoint);
    return instance;
  }

  /// Wait for servers to get in sync
  private async getApplierState(url: string): Promise<rp.RequestPromise> {
    url = endpointToUrl(url);
    const body = await rp.get({
      json: true,
      uri: `${url}/_api/replication/applier-state?global=true`
    });
    return body;
  }

  /// Wait for servers to get in sync with leader
  async asyncReplicationTicksInSync(timoutSecs = 45.0): Promise<boolean> {
    let leader = await this.asyncReplicationLeaderInstance();
    let url = endpointToUrl(leader.endpoint);
    const body = await rp.get({ json: true, uri: `${url}/_api/wal/lastTick` });

    const leaderTick = body.tick;
    console.log("Leader Tick %s = %s", leader.endpoint, leaderTick);
    let followers = this.singleServers().filter(
      inst => inst.status === "RUNNING" && inst.endpoint != leader.endpoint
    );
    if (followers.length === 0) {
      return true; // no followers running anymore
      //throw new Error("No followers to wait for");
    }

    let tttt = Math.ceil(timoutSecs * 4);
    for (let i = 0; i < tttt; i++) {
      const result = await Promise.all(
        followers.map(async flw => this.getApplierState(flw.endpoint))
      );
      // is follower running, pulling from current leader and tick values are equal ?
      let unfinished = result.filter(
        result =>
          !result.state.running ||
          result.endpoint !== leader.endpoint ||
          compareTicks(result.state.lastProcessedContinuousTick, leaderTick) ==
            -1
      );
      if (unfinished.length == 0) {
        return true;
      }
      await sleep(250); // 0.25s
      if (i % 4 == 0 && i > tttt / 4) {
        console.log("Unfinished state: %s", JSON.stringify(unfinished));
      }
    }
    return false;
  }

  // Wait for server to respond to an AQL query
  private async asyncWaitInstanceOperational(
    endpoint: string,
    timoutSecs: number = 45.0
  ): Promise<true> {
    let db = arangojs({
      url: endpointToUrl(endpoint)
    });

    let i = Math.ceil(timoutSecs * 2);
    while (i-- > 0) {
      try {
        // should throw if server is unvalailable
        await db.query("FOR x IN [1,2] RETURN x");
        return true; // worked
      } catch (ignored) {}
      await sleep(500); // 0.5s
    }
    console.error("Leader did not figure out leadership in time");
    throw new Error("Timeout waiting for leader to respond");
  }

  async findPrimaryDbServer(collectionName: string): Promise<Instance> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await this.rpAgency({
      method: "POST",
      uri: baseUrl + "/_api/agency/read",
      json: true,
      body: [
        [
          "/arango/Plan/Collections/_system",
          "/arango/Current/ServersRegistered"
        ]
      ]
    });

    const collections = info.arango.Plan.Collections._system;
    const servers = info.arango.Current.ServersRegistered;
    for (const id of Object.keys(collections)) {
      const collection = collections[id];
      if (collection.name !== collectionName) {
        continue;
      }
      const shards = collection.shards;
      const shardsId = Object.keys(shards)[0];
      const uuid = shards[shardsId][0];
      const server = servers[uuid];
      if (!server) {
        throw new FailoverError(
          `The leader for "${collectionName}" is not registered. Maybe a failover situation`
        );
      }
      const endpoint = server.endpoint;
      const dbServers = this.dbServers().filter(
        instance => instance.endpoint === endpoint
      );
      if (dbServers.length === 0) {
        throw new Error(`Unknown endpoint "${endpoint}"`);
      }
      return dbServers[0];
    }
    throw new Error(`Unknown collection "${collectionName}"`);
  }

  async startCluster(
    numAgents: number,
    numCoordinators: number,
    numDbServers: number,
    options: any = {}
  ): Promise<string> {
    const agencyOptions = options.agents || {};
    _.extend(agencyOptions, { agencySize: numAgents });

    debugLog(
      `Starting cluster A:${numAgents} C:${numCoordinators} D:${numDbServers}`
    );
    debugLog("booting agents...");

    await this.startAgency(agencyOptions);
    await this.waitForInstances(this.agents());
    debugLog("all agents are booted");

    await sleep(2000);
    const dbServers = Array.from(Array(numDbServers).keys()).map(index => {
      return this.startDbServer("dbServer-" + (index + 1));
    });

    debugLog("booting DBServers...");
    await Promise.all(dbServers);
    await this.waitForInstances(this.dbServers());
    debugLog("all DBServers are booted");
    await sleep(2000);

    const coordinators = Array.from(Array(numCoordinators).keys()).map(
      index => {
        return this.startCoordinator("coordinator-" + (index + 1));
      }
    );
    debugLog("booting Coordinators...");
    await Promise.all(coordinators);
    debugLog("all Coordinators are booted");

    debugLog("waiting for /_api/version to succeed an all servers");
    await this.waitForAllInstances();
    debugLog("Cluster is up and running");

    return this.getEndpoint();
  }

  async waitForInstance(
    instance: Instance,
    started: number = Date.now()
  ): Promise<Instance> {
    while (true) {
      if (instance.status !== "RUNNING") {
        throw new Error(dd`
          Instance ${instance.name} is down!
          Real status ${instance.status} with exitcode ${instance.exitcode}.
          See logfile here ${instance.logFile}`);
      }

      if (Date.now() - started > WAIT_TIMEOUT * 1000) {
        throw new Error(
          `Instance ${
            instance.name
          } is still not ready after ${WAIT_TIMEOUT} secs`
        );
      }

      try {
        await rp.get({
          uri: endpointToUrl(instance.endpoint) + "/_api/version"
        });
        return instance;
      } catch (e) {}
      // Wait 100 ms and try again
      await sleep(100);
    }
  }

  async waitForSyncReplication(): Promise<boolean> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    // We wait at most 50s for everything to get in sync
    for (let i = 0; i < 100; ++i) {
      const [info] = await this.rpAgency({
        method: "POST",
        uri: baseUrl + "/_api/agency/read",
        json: true,
        body: [["/arango/Current/Collections/_system"]]
      });
      if (
        !info.hasOwnProperty("arango") ||
        !info.arango.hasOwnProperty("Current") ||
        !info.arango.Current.hasOwnProperty("Collections") ||
        !info.arango.Current.Collections.hasOwnProperty("_system")
      ) {
        debugLog("Got unexpected result from AgencyPlan:", info);
        await sleep(500);
        continue;
      }
      const current = info.arango.Current.Collections._system;

      let foundNotInSync = false;
      if (Object.keys(current).length < 13) {
        // The Cluster needs to create 13 system collections. Wait and try again
        await sleep(500);
        continue;
      }
      for (const [, collection] of Object.entries(current)) {
        if (foundNotInSync) {
          break;
        }
        for (const [, shard] of Object.entries(collection)) {
          if (foundNotInSync) {
            break;
          }
          if (!Array.isArray(shard.servers) || shard.servers.length < 2) {
            // We do not have one in sync follower for each shard
            // Sleep and try again
            foundNotInSync = true;
            continue;
          }
        }
      }
      if (!foundNotInSync) {
        return true;
      }
      await sleep(500);
    }
    return false;
  }

  async waitForAllInstances(): Promise<Instance[]> {
    const results = await this.waitForInstances(this.instances);
    await this.waitForSyncReplication();
    return results;
  }

  private async waitForInstances(instances: Instance[]): Promise<Instance[]> {
    const allWaiters = instances.map(instance =>
      this.waitForInstance(instance)
    );
    const results = [];
    for (let waiter of allWaiters) {
      results.push(await waiter);
    }
    return results;
  }

  private getEndpoint(instance?: Instance): string {
    return (instance || this.coordinators()[0]).endpoint;
  }

  getEndpointUrl(instance: Instance): string {
    return endpointToUrl(this.getEndpoint(instance));
  }

  check(): boolean {
    return this.instances.every(instance => {
      return instance.status === "RUNNING";
    });
  }

  async shutdownCluster(): Promise<void> {
    debugLog(`Shutting down the cluster`);
    const nonAgents = [
      ...this.coordinators(),
      ...this.dbServers(),
      ...this.singleServers()
    ];

    for (let waiter of nonAgents.map(n => this.shutdown(n))) {
      await waiter;
    }
    for (let waiter of this.agents().map(a => this.shutdown(a))) {
      await waiter;
    }
  }

  async cleanup(retainDir: boolean = false): Promise<string> {
    await this.shutdownCluster();
    this.instances = [];
    this.agentCounter = 0;
    this.coordinatorCounter = 0;
    this.dbServerCounter = 0;
    this.singleServerCounter = 0;
    await this.runner.cleanup(retainDir);
    let log = this.currentLog;
    this.currentLog = "";
    return log;
  }

  dbServers(): Instance[] {
    return this.instances.filter(instance => instance.role === "primary");
  }

  coordinators(): Instance[] {
    return this.instances.filter(instance => instance.role === "coordinator");
  }

  agents(): Instance[] {
    return this.instances.filter(instance => instance.role === "agent");
  }

  singleServers(): Instance[] {
    return this.instances.filter(inst => inst.role === "single");
  }

  /// use Current/ServersRegistered to find the corresponding
  /// instance metadata
  async resolveUUID(uuid: string): Promise<Instance | undefined> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await this.rpAgency({
      method: "POST",
      uri: baseUrl + "/_api/agency/read",
      json: true,
      body: [["/arango/Current/ServersRegistered"]]
    });
    const servers = info.arango.Current.ServersRegistered;
    const url = servers[uuid].endpoint;
    return this.instances.filter(inst => inst.endpoint == url).shift();
  }

  async assignNewEndpoint(instance: Instance): Promise<void> {
    const instanceIndex = this.instances.indexOf(instance);
    if (instanceIndex === -1) {
      throw new Error("Couldn't find instance " + instance.name);
    }

    let endpoint: string;
    do {
      endpoint = await this.runner.createEndpoint();
    } while (endpoint === instance.endpoint);

    ["server.endpoint", "agency.my-address", "cluster.my-address"].filter(
      arg => {
        const endpointArgIndex = instance.args.indexOf(
          "--" + arg + "=" + instance.endpoint
        );
        if (endpointArgIndex !== -1) {
          instance.args[endpointArgIndex] = "--" + arg + "=" + endpoint;
        }
      }
    );

    return this.runner.updateEndpoint(instance, endpoint);
  }

  // beware! signals are not supported on windows and it will simply do hard kills all the time
  // use shutdown to gracefully stop an instance!
  async kill(instance: Instance): Promise<void> {
    this.ensureInstance(instance);

    instance.process!.kill("SIGKILL");
    instance.status = "KILLED";
    while (instance.status !== "EXITED") {
      await sleep(50);
    }
  }

  // beware! signals are not supported on windows and it will simply do hard kills all the time
  // send a STOP signal to halt an instance
  sigstop(instance: Instance): void {
    this.ensureInstance(instance);

    instance.process!.kill("SIGSTOP");
    instance.status = "STOPPED";
  }

  sigcontinue(instance: Instance): void {
    this.ensureInstance(instance);

    if (instance.status !== "STOPPED") {
      throw new Error(
        `trying to send SIGCONT to a process with status ${instance.status}`
      );
    }

    instance.process!.kill("SIGCONT");
    instance.status = "RUNNING";
  }

  private ensureInstance(instance: Instance): void {
    if (!this.instances.includes(instance)) {
      throw new Error(`Could not find instance ${instance.name}`);
    }
    if (!instance.process) {
      throw new Error(`Could not find process of instance ${instance.name}`);
    }
  }

  // Internal function do not call from outside
  private async checkDown(instance: Instance): Promise<Instance> {
    debugLog(`Test if ${instance.name} is down`);
    // let attempts = 0;
    const start = Date.now();
    let sigkillSent = false;

    while (instance.status !== "EXITED") {
      const duration = (Date.now() - start) / 1000;

      if (duration >= shutdownKillAfterSeconds && !sigkillSent) {
        debugLog(`Sending SIGKILL to ${instance.name}`);
        if (!instance.process) {
          throw new Error(
            `Could not find process of instance ${instance.name}`
          );
        }
        instance.process.kill("SIGKILL");
        instance.status = "KILLED";
      }

      if (duration >= shutdownGiveUpAfterSeconds) {
        // Sorry we give up, could neither terminate instance with Shutdown nor with SIGKILL
        debugLog(`Failed to shutdown and kill ${instance.name}. Aborting.`);
        throw new Error(
          `${instance.name} did not stop gracefully after ${duration}s`
        );
      }

      // wait a while and try again.
      await sleep(shutdownWaitInterval);
    }

    debugLog(`${instance.name} is now gone.`);
    return instance;
  }

  async shutdown(instance: Instance): Promise<Instance> {
    if (instance.status === "EXITED") {
      return instance;
    }
    debugLog(`Shutdown ${instance.name}`);

    try {
      await rp.delete({
        url: this.getEndpointUrl(instance) + "/_admin/shutdown"
      });
    } catch (err) {
      let expected = false;
      if (err && (err.statusCode === 503 || err.error)) {
        // Some "errors" are expected.
        let errObj =
          typeof err.error === "string" ? JSON.parse(err.error) : err.error;
        if (errObj.code === "ECONNREFUSED") {
          console.warn(
            "hmmm...server " +
              instance.name +
              " did not respond (" +
              JSON.stringify(errObj) +
              "). Assuming it is dead. Status is: " +
              instance.status
          );
          expected = true;
        } else if (errObj.code === "ECONNRESET") {
          expected = true;
        } else if (err.statusCode === 503) {
          console.warn(
            "server " +
              instance.name +
              " answered 503. Assuming it is shutting down. Status is: " +
              instance.status
          );
          expected = true;
        } else if (errObj.errorNum === ERROR_SHUTTING_DOWN) {
          expected = true;
        }
        if (!expected) {
          console.error(
            "An unexpected error occured during shutdown. Error code: ",
            errObj.errorNum,
            ", error type: ",
            typeof err.error
          );
        }
      }
      if (!expected) {
        console.error("Unhandled error", err);
        throw err;
      }
    }
    return this.checkDown(instance);
  }

  async destroy(instance: Instance): Promise<void> {
    if (this.instances.includes(instance)) {
      await this.shutdown(instance);
    }
    await this.runner.destroy(instance);
    const idx = this.instances.indexOf(instance);
    if (idx !== -1) {
      this.instances.splice(idx, 1);
    }
  }

  async restart(instance: Instance): Promise<Instance> {
    if (!this.instances.includes(instance)) {
      throw new Error("Couldn't find instance " + instance.name);
    }
    debugLog(`Restarting ${instance.name}`);
    await this.runner.restart(instance);
    return this.waitForInstance(instance);
  }

  // this will append the logs to the test in case of a failure so
  // you get a nice combined log of what happened on the server and client
  moveServerLogs(test: any): void {
    if (test.state === "failed") {
      test.err.message = this.currentLog + "\n\n" + test.err.message;
    }
    this.currentLog = "";
  }

  private async getFoxxmaster(): Promise<Instance | undefined> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await this.rpAgency({
      method: "POST",
      uri: baseUrl + "/_api/agency/read",
      json: true,
      body: [
        ["/arango/Current/Foxxmaster", "/arango/Current/ServersRegistered"]
      ]
    });
    const uuid = info.arango.Current.Foxxmaster;
    const endpoint = info.arango.Current.ServersRegistered[uuid].endpoint;
    return this.instances.find(instance => instance.endpoint === endpoint);
  }

  async restartCluster(): Promise<void> {
    const fm = (await this.getFoxxmaster())!;
    await this.shutdownCluster();
    await Promise.all(this.agents().map(agent => this.restart(agent)));
    await sleep(2000);
    await Promise.all(this.dbServers().map(dbs => this.restart(dbs)));
    this.restart(fm);
    await sleep(2000);
    await Promise.all(
      this.coordinators()
        .filter(coord => coord !== fm)
        .map(coord => this.restart(coord))
    );
    await this.waitForAllInstances();
  }
}
