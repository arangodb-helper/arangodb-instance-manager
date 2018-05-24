import express = require("express");
import {
  NextFunction,
  Request,
  RequestHandler,
  Response
} from "express-serve-static-core";
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
      res.send(this.im.coordinators().map(i => i.endpoint));
    });
  }

  start(): void {
    this.app.listen(this.port, () =>
      console.log(`ArangoDB Instance Manager listen on port ${this.port}`)
    );
  }
}
