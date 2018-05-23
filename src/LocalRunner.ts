"use strict";
import path = require("path");
import { promisify } from "util";
import Instance from "./Instance";
import Runner from "./Runner";
import { createEndpoint, startInstance } from "./common";
import tmp = require("tmp");
import mkdirp = require("mkdirp-promise");
import fs = require("fs");
import rmRf = require("rmfr");
const rename = promisify(fs.rename);

export default class LocalRunner implements Runner {
  basePath: string;
  rootDir: string;
  constructor(basePath: string) {
    this.rootDir = tmp.dirSync({ prefix: "arango-resilience" }).name;
    this.basePath = basePath;
  }

  createEndpoint() {
    return createEndpoint();
  }

  firstStart(instance: Instance): Promise<Instance> {
    const dir = path.join(this.rootDir, instance.name);
    const dataDir = path.join(dir, "data");
    const appsDir = path.join(dir, "apps");
    const logFile = path.join(dir, "arangod.log");
    instance.logFile = logFile;

    instance.args.unshift("--configuration=none");
    instance.args.push(
      ...[
        `--javascript.startup-directory=${path.join(this.basePath, "js")}`,
        `--javascript.app-path=${appsDir}`,
        `--server.endpoint=${instance.endpoint}`,
        `--log.file=${logFile}`,
        dataDir
      ]
    );

    const arangod = path.join(this.basePath, "build", "bin", "arangod");
    if (process.env.RESILIENCE_ARANGO_WRAPPER) {
      let wrapper = process.env.RESILIENCE_ARANGO_WRAPPER.split(" ");
      instance.binary = wrapper.shift()!;
      instance.args = wrapper.concat(arangod, instance.args);
    } else {
      instance.binary = arangod;
    }

    return Promise.all([mkdirp(dataDir), mkdirp(appsDir)]).then(() => {
      return startInstance(instance);
    });
  }

  async updateEndpoint(instance: Instance, endpoint: string): Promise<void> {
    instance.endpoint = endpoint;
  }

  restart(instance: Instance): Promise<Instance> {
    return startInstance(instance);
  }

  destroy(instance: Instance): Promise<void> {
    return rmRf(path.join(this.rootDir, instance.name));
  }

  cleanup(retainDir: boolean): Promise<void> {
    if (retainDir) {
      const newDir = tmp.dirSync({ prefix: "arango-resilience-failure" }).name;
      console.warn(`Test failed, moving files to ${newDir}`);
      return rename(this.rootDir, newDir);
    } else {
      return rmRf(this.rootDir);
    }
  }
}
