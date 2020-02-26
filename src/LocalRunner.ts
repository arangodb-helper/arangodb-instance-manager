"use strict";
import path = require("path");
import { SimpleOptions } from "tmp";
import { createEndpoint, startInstance } from "./common";
import Instance from "./Instance";
import Runner from "./Runner";
import tmp = require("tmp");
import mkdirp = require("mkdirp-promise");
import rmRf = require("rmfr");

const debugLog = (...logLine: any[]) => {
  if (process.env.LOG_IMMEDIATE && process.env.LOG_IMMEDIATE == "1") {
    const [fmt, ...args] = logLine;
    console.log(new Date().toISOString() + " " + fmt, ...args);
  }
};

export default class LocalRunner implements Runner {
  basePath: string;
  rootDir?: string;
  newDirOptions: SimpleOptions;

  constructor(basePath: string, instancesDirectory?: string) {
    this.newDirOptions = { prefix: "arango-resilience" };
    if (instancesDirectory !== undefined) {
      this.newDirOptions.dir = instancesDirectory;
    }
    // rootDir will be created when the first instance is started
    this.rootDir = undefined;
    this.basePath = basePath;
  }

  createEndpoint() {
    return createEndpoint();
  }

  firstStart(instance: Instance): Promise<Instance> {
    this.assertRootDir();
    const dir = path.join(this.getRootDir(), instance.name);
    instance.dir = dir;
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
    return rmRf(path.join(this.getRootDir(), instance.name));
  }

  cleanup(retainDir: boolean): Promise<void> {
    const oldDirectory = this.rootDir;
    this.rootDir = undefined;

    if (retainDir) {
      console.warn(`Test failed, won't remove directory ${oldDirectory}`);
      return Promise.resolve();
    } else if (oldDirectory !== undefined) {
      // TODO remove the log message
      debugLog(`Removing directory ${oldDirectory}`);
      return rmRf(oldDirectory);
    } else {
      console.warn(
        `There is no rootDir, ` +
          `did you call cleanup before starting an instance?`
      );
      return Promise.resolve();
    }
  }

  getRootDir(): string {
    this.assertRootDir();
    // assertRootDir guarantees rootDir to be a string
    return this.rootDir as string;
  }

  private createNewRootDir(): void {
    if (this.rootDir !== undefined) {
      console.error(
        `Creating a new root directory, but the old wasn't cleaned ` +
          `up! Old directory was: ${this.rootDir}`
      );
    }

    this.rootDir = tmp.dirSync(this.newDirOptions).name;

    debugLog(`Created new root directory ${this.rootDir}`);
  }

  // Create root directory if necessary.
  private assertRootDir(): void {
    if (this.rootDir !== undefined) {
      return;
    }

    this.createNewRootDir();
  }
}
