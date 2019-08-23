"use strict";
import _ = require("lodash");
import dd = require("dedent");
import rp = require("request-promise-native");
import fs = require("fs");
import { UriOptions } from "request";
import DockerRunner from "./DockerRunner";
import { FailoverError } from "./Errors";
import Instance, { Role, Status } from "./Instance";
import LocalRunner from "./LocalRunner";
import Runner from "./Runner";
import { compareTicks, endpointToUrl } from "./common";
import VersionResponse, { Version } from "./VersionResponse";
import * as path from "path";
import { EEXIST } from "constants";

// Arango error code for "shutdown in progress"
const ERROR_SHUTTING_DOWN = 30;

const WAIT_TIMEOUT = 400; // seconds

const debugLog = (...logLine: any[]) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
    const [fmt, ...args] = logLine;
    console.log(new Date().toISOString() + " " + fmt, ...args);
  }
};

const isNonEmptyString = (x: any) : x is string => _.isString(x) && x !== '';

// 180s, after this time we kill -9 the instance
const shutdownKillAfterSeconds = 180;

// We check every 50ms if a server is down
const shutdownWaitInterval = 50;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// TODO this has more properties. Maybe use a class instead.
interface HealthResponse {
  Endpoint: string;
}

function isHealth(x: any): x is HealthResponse {
  return x.hasOwnProperty("Endpoint") && typeof x.Endpoint === "string";
}

function toHealth(x: any): HealthResponse {
  if(isHealth(x)) {
    return x;
  }
  else {
    throw new TypeError(`Not a Health object: ${x}`);
  }
}

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
    let instancesDirectory : string | undefined;
    if (isNonEmptyString(process.env.ARANGO_INSTANCES_DIR)) {
      instancesDirectory = process.env.ARANGO_INSTANCES_DIR;
      InstanceManager.createDir(instancesDirectory);
    }

    if (runner === "local") {
      this.runner = new LocalRunner(pathOrImage, instancesDirectory);
    } else if (runner === "docker") {
      // TODO The DockerRunner does have no concept of instance directories
      this.runner = new DockerRunner(pathOrImage);
    } else {
      throw new Error("Unkown runner type");
    }

    this.storageEngine = storageEngine;
    this.currentLog = "";
    this.agentCounter = 0;
    this.coordinatorCounter = 0;
    this.dbServerCounter = 0;
    this.singleServerCounter = 0;
  }

  static async rpAgency(
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

  private startArango(
    name: string,
    endpoint: string,
    role: Role,
    args: string[]
  ): Promise<Instance> {
    args.push("--server.authentication=false");
    args.push("--log.use-microtime=true");
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

    if (process.env.LOG_REPLICATION && process.env.LOG_REPLICATION !== "") {
      args.push(`--log.level=replication=${process.env.LOG_REPLICATION}`);
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

  async replace(instance: Instance, withNewEndpoint = false): Promise<Instance> {
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
    if (withNewEndpoint) {
      await this.assignNewEndpoint(instance);
    }
    instance = await this.startArango(
      name,
      instance.endpoint,
      instance.role,
      args
    );
    await InstanceManager.waitForInstance(instance);
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
    } catch (e) { }
  }

  static access(obj: any, field: string | number) {
    if (obj[field]) {
      return obj[field];
    } else {
      return undefined;
    }
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

    // const leader = body[0].arango.Plan.AsyncReplication.Leader;
    const leader = [0, "arango", "Plan", "AsyncReplication", "Leader"]
      .reduce(InstanceManager.access, body);
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
  async asyncReplicationLeaderSelected(
    ignore = null,
    timeoutSec = 25
  ): Promise<string> {
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
  async asyncReplicationLeaderInstance(): Promise<Instance> {
    const uuid = await this.asyncReplicationLeaderId();
    if (uuid == null) {
      throw "leader is not in agency";
    }
    let instance = await this.resolveUUID(uuid);
    if (!instance) {
      console.error("Could not find leader instance locally");
      throw new Error("Could not find leader instance locally");
    }
    debugLog("Leader in agency %s (%s)", uuid, instance.endpoint);
    // we need to wait for the server to get out of maintenance mode
    await InstanceManager.asyncWaitInstanceOperational(instance.endpoint);
    return instance;
  }

  /// Wait for servers to get in sync
  private static async getApplierState(url: string): Promise<rp.RequestPromise> {
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

    // TODO replace this loop by something more readable
    let tttt = Math.ceil(timoutSecs * 4);
    for (let i = 0; i < tttt; i++) {
      const result = await Promise.all(
        followers.map(async flw => InstanceManager.getApplierState(flw.endpoint))
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
  private static async asyncWaitInstanceOperational(
    endpoint: string,
    timoutSecs: number = 45.0
  ): Promise<true> {
    let url = endpointToUrl(endpoint);
    let i = Math.ceil(timoutSecs * 2);
    while (i-- > 0) {
      try {
        let res = await rp.get({
          json: true,
          uri: `${url}/_admin/server/availability`
        });
        if (res.code === 200) {
          return true; // worked
        }
      } catch (ignored) { }
      await sleep(500); // 0.5s
    }
    console.error("Leader did not figure out leadership in time");
    throw new Error("Timeout waiting for leader to respond");
  }

  async findPrimaryDbServer(collectionName: string): Promise<Instance> {
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());
    const [info] = await InstanceManager.rpAgency({
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

  // Note that this waits for all instances to be up and working, but not for
  // synchronous replication of system collections to be up. So you might have
  // to wait before killing a dbServer.
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

    const dbServers = Array.from(Array(numDbServers).keys()).map(index => {
      return this.startDbServer("dbServer-" + (index + 1));
    });

    debugLog("booting DBServers...");
    await Promise.all(dbServers);
    await this.waitForInstances(this.dbServers());
    debugLog("all DBServers are booted");

    const coordinators = Array.from(Array(numCoordinators).keys()).map(
      index => {
        return this.startCoordinator("coordinator-" + (index + 1));
      }
    );
    debugLog("booting Coordinators...");
    await Promise.all(coordinators);
    debugLog("all Coordinators are booted");

    debugLog("waiting for /_api/version to succeed on all servers");
    await this.waitForAllInstances();
    debugLog("adding server IDs to all instances");
    await this.addIdsToAllInstances();
    debugLog("Cluster is up and running");

    return this.getEndpoint();
  }

  static async waitForInstance(instance: Instance): Promise<Instance> {
    let lastError;

    for (
      const start = Date.now();
      Date.now() - start < WAIT_TIMEOUT * 1000;
      await sleep(100)
    ) {
      if (instance.status !== "RUNNING") {
        throw new Error(dd`
          Instance ${instance.name} is down!
          Real status ${instance.status} with exitcode ${instance.exitcode}.
          See logfile here ${instance.logFile}`);
      }

      try {
        const response = await rp.get({
          uri: endpointToUrl(instance.endpoint) + "/_api/version",
          json: true,
        });

        try {
          instance.version = new VersionResponse(response);
        } catch (e) {
          debugLog(`Failed parsing version response ${response}: ${e}`);
          // noinspection ExceptionCaughtLocallyJS
          throw e;
        }
        return instance;
      } catch (e) {
        lastError = e;
      }
    }

    let message = `Instance ${instance.name} is still not ready after ${WAIT_TIMEOUT}s.`;
    if (lastError !== undefined) {
      message += ` The last error was: ${lastError}`;
    }
    debugLog(message);
    throw new Error(message);
  }

  async waitForSyncReplication(): Promise<void> {
    debugLog(`waitForSyncReplication()`);
    const baseUrl = endpointToUrl(this.getAgencyEndpoint());

    const systemCollections = new Set<string>([
      "_appbundles",
      "_apps",
      "_aqlfunctions",
      "_fishbowl",
      "_frontend",
      "_graphs",
      "_analyzers",
      "_jobs",
      "_modules",
      "_queues",
      "_routing",
      "_statistics",
      "_statistics15",
      "_statisticsRaw",
      "_users"
    ]);
    const version = this.getArangoVersion();

    // _analyzers are available since 3.5
    if (version.major <= 3 && version.minor < 4) {
      systemCollections.delete('_analyzers');
    }
    // except in 3.4 (and only in 3.4) they were called
    // _iresearch_analyzers
    if (version.major == 3 && version.minor == 4) {
      systemCollections.delete('_analyzers');
      systemCollections.add('_iresearch_analyzers');
    }

    const allInSync = function (
      current: { [key:string]: { [key:string]: { servers: Array<string> } } },
      expectedDistribution: Map<string, Map<string, Array<string>>>,
      colIdToName: Map<string, string>,
      lastError: { error?: string }
    ) {
      for (const [collectionId, shards] of expectedDistribution) {
      const collection = colIdToName.get(collectionId);
        for (const [shard, servers] of shards) {
          if (!current[collectionId]) {
            debugLog(`Collection not yet in Current: ${collection}`);
            lastError.error = `Collection not yet in Current: ${collection}`;
            return false;
          }
          if (!current[collectionId][shard]) {
            debugLog(`Shard not yet in Current: ${collection}/${shard}`);
            lastError.error = `Shard not yet in Current: ${collection}/${shard}`;
            return false;
          }
          if (!_.isEqual(current[collectionId][shard].servers, servers)) {
            debugLog(`Not in sync: ${collection}/${shard}. `
              + `Expected: ${servers}, `
              + `actual: ${current[collectionId][shard].servers}`
            );
            lastError.error = `Not in sync: ${collection}/${shard}. `
              + `Expected: ${servers}, `
              + `actual: ${current[collectionId][shard].servers}`;
            return false;
          }
        }
      }

      debugLog(`Now everything's in sync`);

      return true;
    };

    const allSystemCollectionsInPlan = function (plan: Object, lastError: {error?: string}) {
      const planCollections = new Set(
        Object.values(plan).map(colInfo => colInfo.name)
      );

      const missingCollections =
        _.filter(systemCollections,
          collection => !planCollections.has(collection)
        );

      if (missingCollections.length > 0) {
        debugLog(`collections ${missingCollections} still missing from Plan`);
        lastError.error = `collections ${missingCollections} still missing from Plan`;
        return false;
      }

      // inverse check if there are any system collections we haven't included.
      // If that's so, there's probably a new one which has to be added for some
      // versions of arangodb.
      for (const collection of planCollections) {
        if (!collection.startsWith('_')) {
          // maybe a collection was added by a test. skip those.
          continue;
        }

        if (!systemCollections.has(collection)) {
          throw new Error(
            `Unexpected system collection '${collection}'. `
            + `If you encounter this error, you probably have to update the `
            + `instance manager by adding the new system collection(s). Make `
            + `sure you only do it for the affected versions.`
          );
        }
      }

      debugLog(`Plan has all system collections`);

      return true;
    };

    const colIdToNameFromPlan = (plan: Object) => new Map(
      Object.entries(plan)
        .map<[string, string]>(([id, colInfo]) => [id, colInfo.name])
    );

    const planToDistribution = function (plan: Object) {
      return new Map(
        Object.entries(plan)
          .map<[string, Map<string, Array<string>>]>(([colId, colInfo]) => {
            return [colId, new Map(Object.entries(colInfo.shards))];
          })
      );
    };

    const lastError : {error?: string} = {
      error: undefined,
    };

    // We wait at most 50s for everything to get in sync
    for (
      const start = Date.now();
      Date.now() - start < 50e3;
      await sleep(100)
    ) {
      debugLog(`Checking if everything is in sync...`);
      const agencyQuery =
        [["/arango/Plan/Collections/_system",
          "/arango/Current/Collections/_system",
        ]];
      const [info] = await InstanceManager.rpAgency({
        method: "POST",
        uri: baseUrl + "/_api/agency/read",
        json: true,
        body: agencyQuery,
      });

      if (
        info.hasOwnProperty("arango") &&
        info.arango.hasOwnProperty("Current") &&
        info.arango.Current.hasOwnProperty("Collections") &&
        info.arango.Current.Collections.hasOwnProperty("_system") &&
        info.arango.hasOwnProperty("Plan") &&
        info.arango.Plan.hasOwnProperty("Collections") &&
        info.arango.Plan.Collections.hasOwnProperty("_system")
      ) {
        const current = info.arango.Current.Collections._system;
        const plan = info.arango.Plan.Collections._system;

        if (allSystemCollectionsInPlan(plan, lastError)) {
          // The outer Map's keys are collection IDs,
          // the inner Map's keys are shards,
          // the inner Map's values are lists of DBServer IDs.
          const expectedDistribution: Map<string, Map<string, Array<string>>>
            = planToDistribution(plan);

          const colIdToName = colIdToNameFromPlan(plan);

          if (allInSync(current, expectedDistribution, colIdToName, lastError)) {
            // Everything's fine
            return;
          }
        }
      } else {

        lastError.error = "Got unexpected result from AgencyPlan: "
          + JSON.stringify(info)
          + " as result of the query " + JSON.stringify(agencyQuery);
        debugLog("Got unexpected result from AgencyPlan:", info);
      }
    }

    let agencyDump, exception;
    try {
      agencyDump = await InstanceManager.rpAgency({
        method: "POST",
        uri: baseUrl + "/_api/agency/read",
        json: true,
        body: [["/"]],
      });
    } catch(e) { exception = e;}


    let message = `Plan and Current didn't come in sync after 50s.`;
    if (lastError.error !== undefined) {
        message += ` The last error was: ${lastError.error}`;
    }
    if (agencyDump !== undefined) {
      message += `; Complete agency dump: ` + JSON.stringify(agencyDump);
    }
    if (exception !== undefined) {
      message += `; Error getting a full agency dump: ` + JSON.stringify(exception);
    }
    throw new Error(message);
  }

  async waitForAgency(): Promise<void> {
    const getLeader = async (instance: Instance) : Promise<string | undefined> => {
      try {
        const baseUrl = endpointToUrl(instance.endpoint);
        const res = await InstanceManager.rpAgency({
          uri: baseUrl + "/_api/agency/config",
          json: true,
        });
        const {leaderId} = res;
        if (typeof leaderId === "string") {
          return leaderId;
        }
      } catch(e) {
        debugLog(`Error reading agent config: `, e);
      }

      return undefined;
    };

    let lastLeaders : undefined | { [key: string]: string | undefined };

    for (
      const start = Date.now();
      Date.now() - start < 50e3;
      await sleep(100)
    ) {
      const agents = this.agents();
      const maybeLeaders = await Promise.all(agents.map(getLeader));
      const leaders = maybeLeaders
        .filter(_.isString)
        .filter(l => l !== '');
      const allAgentsHaveALeader = maybeLeaders.length === leaders.length;
      const allLeadersAreTheSame = leaders.length === 0
          || leaders.every(leader => leader === leaders[0]);
      if (allAgentsHaveALeader && allLeadersAreTheSame) {
        return;
      }

      // This is only for the error message below.
      lastLeaders = _.zipObject(
        agents.map(agent => agent.name),
        maybeLeaders,
      );
    }

    throw new Error(
      `Agents did not all agree on the same leader after 50s. `
      + `The last leaders per agent were: ` + JSON.stringify(lastLeaders)
    );
  }

  async waitForAllInstances(): Promise<Instance[]> {
    const results = await this.waitForInstances(this.instances);
    if (this.hasDbServers()) {
      // This is the cluster case: When we have DB Servers, we also have an
      // agency.
      // In this case, we wait for synchronous replication.
      await this.waitForSyncReplication();
    } else {
      // We have just an agency and/or single servers (possibly with active
      // failover).
      await this.waitForAgency();
    }
    return results;
  }

  private async waitForInstances(instances: Instance[]): Promise<Instance[]> {
    const allWaiters = instances.map(instance =>
      InstanceManager.waitForInstance(instance)
    );
    const results = [];
    for (let waiter of allWaiters) {
      results.push(await waiter);
    }
    return results;
  }

  private async addIdsToAllInstances(): Promise<void> {
    for (
      const start = Date.now();
      Date.now() - start < 50e3;
      await sleep(100)
    ) {
      const coordinator = this.getCoordinator();
      const res = await rp({
        url: this.getEndpointUrl(coordinator) + "/_admin/cluster/health",
        json: true,
      });
      const healthEntries = Object.entries(res.Health);

      const endpointToId = new Map<string, string>(
        healthEntries.map<[string, string]>(
          ([id, infos]) => [toHealth(infos).Endpoint, id]
        )
      );

      this.instances
      // skip instances which already got ids
        .filter(instance => !instance.hasOwnProperty('id'))
        .forEach(instance => {
          const endpoint = this.getEndpoint(instance);
          // set id if we could find it
          if (endpointToId.has(endpoint)) {
            instance.id = endpointToId.get(endpoint);
          } else {
            debugLog(`Endpoint ${endpoint} not found in health struct.`);
          }
        });

      if (this.instances.every(inst => inst.hasOwnProperty('id'))) {
        return;
      }
    }

    throw new Error('Failed adding IDs to all instances');
  }

  private getCoordinator() : Instance {
    const runningCoords = this
      .coordinators()
      .filter(server => server.status == "RUNNING");
    if (runningCoords.length === 0) {
      throw new Error('No coordinator is running');
    }

    return runningCoords[0];
  }

  private getEndpoint(instance?: Instance): string {
    return (instance || this.coordinators()[0]).endpoint;
  }

  private getRunningInstances() : Instance[] {
    return this.instances.filter(server => server.status == "RUNNING");
  }

  // We assume all processes are started with the same binary. So just take any.
  private getArangoVersion(): Version {
    const instances = this.getRunningInstances();

    if (instances.length === 0) {
      throw new Error('No running instances.');
    }

    const inst = instances[0];

    if (inst.version === undefined) {
      throw new Error('Expected a running instance to have its version set. '
      + 'Did someone forget to call waitForInstance() on it?');
    }

    return inst.version.version;
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
    const errors: Error[] = [];

    const collectAsyncErrors = async (promise: Promise<Instance>) => {
      try {
        await promise;
      }
      catch (e) {
        errors.push(e);
      }
    };

    const shutdownAll = async (instances: Instance[]) => {
      return await Promise.all(
        instances
          .map(inst => this.shutdown(inst))
          .map(collectAsyncErrors)
      );
    };

    // Shut down in order. Wait for all servers of a kind to finish before
    // starting to shut down the next. This holds especially for the agents!
    debugLog('Shutting down all coordinators');
    await shutdownAll(this.coordinators());
    debugLog('Shutting down all dbServers, coordinators should all be down');
    await shutdownAll(this.dbServers());
    debugLog('Shutting down all singleServers');
    await shutdownAll(this.singleServers());
    debugLog('Shutting down all agents, everything else should be down');
    await shutdownAll(this.agents());

    if (errors.length > 0) {
      throw new Error(
        `Caught ${errors.length} error(s) during shutdownCluster:\n`
        + errors.map(e => e.toString()).join("\n")
      );
    }
  }

  // Setting collectCores = true with a DockerRunner will result in an
  // exception.
  // Unless retainDir is true as well, setting collectCores to true will have
  // no effect. retainDir will automatically set to true on a shutdown failure.
  async cleanup(retainDir: boolean = false,
                collectCores: boolean = false,
                notes?: string): Promise<string> {
    try {
        await this.shutdownCluster();
    } catch (e) {
      debugLog(`cleanup: Error during shutdownCluster:`);
      debugLog(e);
      retainDir = true;
    }

    if (retainDir && notes !== undefined) {
      this.writeNotes(notes);
    }

    if (retainDir && collectCores) {
      this.collectCores();
    }

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

  // Move core files to instance directories, using PIDs to match instances
  // to core files.
  // Expects core files to begin with `core-%p-`, where %p is the pid, and to be
  // in the current working directory.
  // TODO Reading /proc/sys/kernel/core_pattern to detect the core pattern would
  // be nice!
  private collectCores(): void {
    for (const instance of this.instances) {
      if (!instance.dir) {
        // This is expected to happen if the DockerRunner is used with
        // collectCores, as it does not (yet) have a concept of an instance
        // directory.
        console.error(`Instance ${instance.name} has no directory set! `);
        continue;
      }
      if (instance.process) {
        const pid = instance.process.pid;
        const re = new RegExp(`^core-${pid}-`);
        const coreFn = fs.readdirSync(".").find(fn => re.test(fn));
        if (coreFn) {
          debugLog(`Moving core file ${coreFn} to ${instance.dir}`);
          fs.renameSync(coreFn, path.join(instance.dir, coreFn));
        }
      }
    }
  }

  private writeNotes(notes: string): void {
    try {
      const dir = this.runner.getRootDir();
      const fn = path.join(dir, 'NOTES');

      fs.writeFileSync(fn, notes);
    } catch(e) {
      console.error(`Failed writing NOTES: ${e}`);
    }
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
    const [info] = await InstanceManager.rpAgency({
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

    // Instance is already dead.
    if (instance.status === "EXITED") {
      return;
    }

    // I'm assuming here that no context switch can happen between the previous
    // check (status === "EXITED") and the following assignment
    // (status = "KILLED"). If this assumption is wrong, this would be a race
    // when the process.on("exit"...) trigger, setting status = "EXITED", would
    // be executed in between, resulting in an endless loop.

    instance.status = "KILLED";
    instance.process!.kill("SIGKILL");

    // Need to convince TypeScript that status does not stay "KILLED" here.
    while ((<Status>instance.status) !== "EXITED") {
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
  private static async checkDown(instance: Instance): Promise<Instance> {
    debugLog(`Test if ${instance.name} is down`);
    // let attempts = 0;
    const start = Date.now();
    let sigkillSent = false;

    while (instance.status !== "EXITED") {
      const duration = (Date.now() - start) / 1000;

      if (duration >= shutdownKillAfterSeconds && !sigkillSent) {
        if (!instance.process) {
          throw new Error(
            `Could not find process of instance ${instance.name}`
          );
        }
        // Send SIGABRT to produce a core dump.
        console.warn(
          `Sending SIGABRT to ${instance.name} due to timeout `
          + `during shutdown. PID is ${instance.process.pid}.`);
        instance.process.kill("SIGABRT");
        instance.status = "KILLED";
        throw new Error(`Reached shutdown timeout, logfile is ${instance.logFile}`);
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
        let errObj;
        if (typeof err.error === "string") {
          // The 503 errors during shutdown have err.error === "".
          if (err.error !== "") {
            try {
              errObj = JSON.parse(err.error);
            }
            catch (e) {
              console.error(e, ` while parsing: ${err.error}`);
              throw e;
            }
          }
          else {
            errObj = {};
          }
        } else {
          errObj = err.error;
        }
        if (errObj.code === "ECONNREFUSED") {
          debugLog(
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
          debugLog(
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
    return await InstanceManager.checkDown(instance);
  }

  async destroy(instance: Instance): Promise<void> {
    if (this.instances.includes(instance)) {
      try {
        await this.shutdown(instance);
      } catch(e) {
        debugLog(`destroy(${instance.name}): shutdown failed with ${e}.`);
        // Abort destroy when shutdown fails
        throw e;
      }
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
    return InstanceManager.waitForInstance(instance);
  }

  // this will append the logs to the test in case of a failure so
  // you get a nice combined log of what happened on the server and client
  moveServerLogs(test: any): void {
    if (test.state === "failed") {
      test.err.message = this.currentLog + "\n\n" + test.err.message;
    }
    this.currentLog = "";
  }

  async restartCluster(): Promise<void> {
    await this.shutdownCluster();
    await Promise.all(this.agents().map(agent => this.restart(agent)));
    await Promise.all(this.dbServers().map(dbs => this.restart(dbs)));
    await Promise.all(
      this.coordinators()
        .map(coord => this.restart(coord))
    );
    await this.waitForAllInstances();
  }

  private hasDbServers(): boolean {
    return this.dbServers().length > 0;
  }

  private static createDir(dirPath: string) {
    try {
      fs.mkdirSync(dirPath);
      debugLog(`Created directory '${dirPath}'`);
    } catch (e) {
      if (e.errno === -EEXIST) {
      }
      else {
        throw e;
      }
    }
  }
}
