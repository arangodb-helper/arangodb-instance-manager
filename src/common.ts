import { spawn } from "child_process";
import Instance from "./Instance";
import portfinder = require("portfinder");
import ip = require("ip");

const debugLog = (...logLine: any[]) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
    const [fmt, ...args] = logLine;
    console.log(new Date().toISOString() + " " + fmt, ...args);
  }
};

export function startInstance(instance: Instance): Promise<Instance> {
  instance.port = portFromEndpoint(instance.endpoint);
  return new Promise((resolve, reject) => {
    try {
      const process = spawn(instance.binary!, instance.args);

      process.stdout.on("data", data =>
        data
          .toString()
          .split("\n")
          .forEach(instance.logFn)
      );

      process.stderr.on("data", data =>
        data
          .toString()
          .split("\n")
          .forEach(instance.logFn)
      );

      process.on("exit", code => {
        debugLog(`${instance.name} exited (${code})`);
        instance.status = "EXITED";
        instance.exitcode = code;
      });
      instance.exitcode = null;
      instance.status = "RUNNING";
      instance.process = process;
      resolve(instance);
    } catch (e) {
      reject(e);
    }
  });
}

let startMinPort = 4000;
if (process.env.MIN_PORT) {
  startMinPort = parseInt(process.env.MIN_PORT, 10);
}

let portOffset = 50;
if (process.env.PORT_OFFSET) {
  portOffset = parseInt(process.env.PORT_OFFSET, 10);
}

let maxPort = 65535;
if (process.env.MAX_PORT) {
  maxPort = parseInt(process.env.MAX_PORT, 10);
}
let minPort = startMinPort;
let findFreePort = function(ip: any) {
  let startPort = minPort;
  minPort += portOffset;
  if (minPort >= maxPort) {
    minPort = startMinPort;
  }
  return portfinder.getPortPromise({ port: startPort, host: ip });
};

export function portFromEndpoint(endpoint: string): string {
  return endpoint.match(/:(\d+)\/?/)![1];
}

export async function createEndpoint(myIp = ip.address()): Promise<string> {
  const port = await findFreePort(myIp);
  return `tcp://${myIp}:${port}`;
}

export const endpointToUrl = function(endpoint: string): string {
  if (endpoint.substr(0, 6) === "ssl://") {
    return "https://" + endpoint.substr(6);
  }

  const pos = endpoint.indexOf("://");

  if (pos === -1) {
    return "http://" + endpoint;
  }

  return "http" + endpoint.substr(pos);
};

export const compareTicks = function(l: string, r: string): number {
  var i;
  if (l === null) {
    l = "0";
  }
  if (r === null) {
    r = "0";
  }
  if (l.length !== r.length) {
    return l.length - r.length < 0 ? -1 : 1;
  }

  // length is equal
  for (i = 0; i < l.length; ++i) {
    if (l[i] !== r[i]) {
      return l[i] < r[i] ? -1 : 1;
    }
  }

  return 0;
};
