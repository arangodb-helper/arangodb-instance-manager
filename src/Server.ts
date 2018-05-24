import _ = require("lodash");
import express = require("express");
import {
  NextFunction,
  Request,
  RequestHandler,
  Response
} from "express-serve-static-core";
import Instance from "./Instance.js";
import InstanceManager from "./InstanceManager.js";

const asyncMiddleware = (fn: RequestHandler): any => (
  req: Request,
  res: Response,
  next: NextFunction
): any => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

export default class Server {
  port: number;
  app: express.Express;
  im: InstanceManager;

  constructor(
    port: number = 9000,
    pathOrImage: string = "../arangodb",
    runner: "local" | "docker" = "local",
    storageEngine: "rocksdb" | "mmfiles" = "mmfiles"
  ) {
    this.app = express();
    this.im = new InstanceManager(pathOrImage, runner, storageEngine);
    this.port = port;

    const router = express.Router();
    this.app.use(router);

    router.post(
      "/cluster",
      asyncMiddleware(async (req: Request, res: Response): Promise<any> => {
        const body = req.body ? req.body : {};
        const endpoint = await this.im.startCluster(
          body.numAgents | 1,
          body.numCoordinators | 3,
          body.numDbServeres | 2,
          body.options | ({} as any)
        );
        res.send({ endpoint });
      })
    );

    router.delete("/", (_req: Request, res: Response): any => {
      this.im.cleanup();
      this.im = new InstanceManager(pathOrImage, runner, storageEngine);
      res.send({});
    });

    router.get("/cluster/coordinators", (_req: Request, res: Response): any => {
      res.send(this.im.coordinators().map(i => _.omit(i, ["process"])));
    });

    router.get("/instance/:name", (req: Request, res: Response): any => {
      const name = req.params.name;
      const instance = this.instance(name);
      res.send(_.omit(instance, ["process"]));
    });

    router.delete(
      "/instance/:name",
      asyncMiddleware(async (req: Request, res: Response): Promise<any> => {
        const name = req.params.name;
        await this.im.shutdown(this.instance(name));
        res.send({});
      })
    );

    router.patch(
      "/instance/:name",
      asyncMiddleware(async (req: Request, res: Response): Promise<any> => {
        const name = req.params.name;
        await this.im.restart(this.instance(name));
        res.send({});
      })
    );
  }

  private instance(name: String): Instance {
    return this.im.instances.filter(i => i.name === name)[0];
  }

  start(): void {
    this.app.listen(this.port, () =>
      console.log(`ArangoDB Instance Manager listen on port ${this.port}`)
    );
  }
}
