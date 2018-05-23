declare module "mkdirp-promise" {
  declare function mkdirp(dir: string, opts?: any): Promise<void>;
  export = mkdirp;
}

declare module "rmfr" {
  declare function rmfr(...args: string[]): Promise<void>;
  export = rmfr;
}
