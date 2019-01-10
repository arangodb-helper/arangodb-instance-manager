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
      asyncMiddleware(
        async (req: Request, res: Response): Promise<any> => {
          const body = req.body ? req.body : {};
          const endpoint = await this.im.startCluster(
            body.numAgents || 1,
            body.numCoordinators || 3,
            body.numDbServeres || 2,
            body.options || ({} as any)
          );
          res.send({ endpoint });
        }
      )
    );

    router.delete(
      "/",
      (_req: Request, res: Response): any => {
        this.im.cleanup();
        this.im = new InstanceManager(pathOrImage, runner, storageEngine);
        res.send({});
      }
    );

    router.get(
      "/instance/coordinators",
      (_req: Request, res: Response): any => {
        res.send(this.forClient(this.im.coordinators()));
      }
    );

    router.get(
      "/instance/single",
      (_req: Request, res: Response): any => {
        res.send(this.forClient(this.im.singleServers()));
      }
    );

    router.head(
      "/instance",
      asyncMiddleware(
        async (_req: Request, res: Response): Promise<any> => {
          await this.im.waitForAllInstances();
          res.send({});
        }
      )
    );

    router
      .route("/instance/:name")
      .get(
        (req: Request, res: Response): any => {
          const name = req.params.name;
          const instance = this.instance(name);
          res.send(this.forClient([instance])[0]);
        }
      )
      .head(
        asyncMiddleware(
          async (req: Request, res: Response): Promise<any> => {
            const name = req.params.name;
            await InstanceManager.waitForInstance(this.instance(name));
            res.send({});
          }
        )
      );

    router.delete(
      "/instance/:name",
      asyncMiddleware(
        async (req: Request, res: Response): Promise<any> => {
          const name = req.params.name;
          const kill = req.query.kill || false;
          if (kill) {
            await this.im.kill(this.instance(name));
          } else {
            await this.im.shutdown(this.instance(name));
          }
          res.send({});
        }
      )
    );

    router.post(
      "/instance/:name",
      asyncMiddleware(
        async (req: Request, res: Response): Promise<any> => {
          const name = req.params.name;
          await this.im.restart(this.instance(name));
          res.send({});
        }
      )
    );

    router.post(
      "/agency",
      asyncMiddleware(
        async (_req: Request, res: Response): Promise<any> => {
          const instances = await this.im.startAgency();
          res.send(this.forClient(instances));
        }
      )
    );

    router.post(
      "/single",
      asyncMiddleware(
        async (req: Request, res: Response): Promise<any> => {
          const num = req.query.num || undefined;
          const instances = await this.im.startSingleServer("single", num);
          res.send(this.forClient(instances));
        }
      )
    );

    router
      .route("/replication/leader")
      .get(
        asyncMiddleware(
          async (_req: Request, res: Response): Promise<any> => {
            const instance = await this.im.asyncReplicationLeaderInstance();
            res.send(this.forClient([instance])[0]);
          }
        )
      )
      .head(
        asyncMiddleware(
          async (req: Request, res: Response): Promise<any> => {
            const ignore = req.query.ignore || undefined;
            await this.im.asyncReplicationLeaderSelected(ignore);
            res.send({});
          }
        )
      );

    router.get(
      "/replication/leader/id",
      asyncMiddleware(
        async (_req: Request, res: Response): Promise<any> => {
          const uuid = await this.im.asyncReplicationLeaderId();
          res.send({ uuid });
        }
      )
    );
  }

  private instance(name: String): Instance {
    return this.im.instances.filter(i => i.name === name)[0];
  }

  private forClient(instances: Instance[]): any[] {
    return instances.map(i => _.omit(i, ["process"]));
  }

  start(): void {
    this.app.listen(this.port, () =>
      console.log(`ArangoDB Instance Manager listen on port ${this.port}`)
    );
  }
}
