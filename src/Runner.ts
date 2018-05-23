import Instance from "./Instance";

export default interface Runner {
  createEndpoint(): Promise<string>;
  firstStart(instance: Instance): Promise<Instance>;
  updateEndpoint(instance: Instance, endpoint: string): Promise<void>;
  restart(instance: Instance): Promise<Instance>;
  destroy(instance: Instance): Promise<void>;
  cleanup(retainDir: boolean): Promise<void>;
}
