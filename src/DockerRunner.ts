"use strict";
import { exec, spawn } from "child_process";
import Instance from "./Instance";
import Runner from "./Runner";
import { createEndpoint, portFromEndpoint, startInstance } from "./common";
import crypto = require("crypto");
import which = require("which");

const asyncExec = (binary: string, args: string[]): Promise<number> => {
  return new Promise((resolve, reject) => {
    let process = spawn(binary, args);
    process.on("close", code => {
      if (code != 0) {
        reject(code);
      } else {
        resolve(code);
      }
    });
  });
};

const asyncWhich = (cmd: string): Promise<string> =>
  new Promise((resolve, reject) => {
    which(cmd, (err, path) => {
      if (err) {
        reject(err);
      } else {
        resolve(path);
      }
    });
  });

export default class DockerRunner implements Runner {
  docker?: string;
  prefix: string;
  containerNames: string[];
  image: string;

  constructor(image: string) {
    var currentDate = new Date().valueOf().toString();
    var random = Math.random().toString();
    this.prefix =
      "arango-" +
      crypto
        .createHash("md5")
        .update(currentDate + random)
        .digest("hex");
    this.image = image;
    this.containerNames = [];
  }

  createEndpoint(): Promise<string> {
    return createEndpoint();
  }

  async locateDocker(): Promise<string> {
    if (!this.docker) {
      this.docker = await asyncWhich("docker");
    }
    return this.docker;
  }

  containerName(instance: Instance): string {
    return this.prefix + "-" + instance.name;
  }

  async firstStart(instance: Instance): Promise<Instance> {
    const dockerBin = await this.locateDocker();
    instance.binary = dockerBin;
    instance.args.push("--server.endpoint=tcp://0.0.0.0:8529");
    // instance.arangodArgs = instance.args.slice();
    let dockerArgs = [
      "run",
      "-e",
      "ARANGO_NO_AUTH=1",
      "-p",
      portFromEndpoint(instance.endpoint) + ":8529",
      "--name=" + this.containerName(instance),
      this.image,
      "arangod"
    ];

    instance.args = dockerArgs.concat(instance.args);
    this.containerNames.push(this.containerName(instance));
    return startInstance(instance);
  }

  restart(instance: Instance): Promise<Instance> {
    instance.args = ["start", "-a", this.containerName(instance)];
    return startInstance(instance);
  }

  async destroy(instance: Instance): Promise<void> {
    const dockerBin = await this.locateDocker();
    await asyncExec(dockerBin, ["rm", "-fv", this.containerName(instance)]);
  }

  cleanup(): Promise<void> {
    return Promise.all(
      this.containerNames.map(containerName => {
        return this.locateDocker().then(dockerBin => {
          return new Promise((resolve, reject) => {
            exec(dockerBin + " rm -fv " + containerName, err => {
              if (err) {
                reject(err);
              } else {
                resolve();
              }
            });
          });
        });
      })
    ).then(() => {
      this.containerNames = [];
    });
  }

  async updateEndpoint(instance: Instance, endpoint: string): Promise<void> {
    instance.endpoint = endpoint;
    const dockerBinary = await this.locateDocker();
    await asyncExec(dockerBinary, [
      "commit",
      this.containerName(instance),
      this.containerName(instance) + "-tmp-container"
    ]);
    await asyncExec(dockerBinary, ["rm", "-f", this.containerName(instance)]);
    const dockerArgs = [
      "create",
      "-e",
      "ARANGO_NO_AUTH=1",
      "-p",
      portFromEndpoint(instance.endpoint) + ":8529",
      "--name=" + this.containerName(instance),
      this.containerName(instance) + "-tmp-container",
      "arangod"
    ];
    await asyncExec(
      dockerBinary,
      dockerArgs.concat(instance.args /*arangodArgs*/)
    );
    await asyncExec(dockerBinary, [
      "rmi",
      "-f",
      this.containerName(instance) + "-tmp-container"
    ]);
  }
}
