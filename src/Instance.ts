import { ChildProcess } from "child_process";
import VersionResponse from "./VersionResponse";

export type Role = "agent" | "primary" | "coordinator" | "single";
export type Status = "NEW" | "RUNNING" | "STOPPED" | "EXITED" | "KILLED";

export default interface Instance {
  name: string;
  logFile?: string;
  endpoint: string;
  binary?: string;
  args: string[];
  port?: string;
  status: Status;
  exitcode: number | null;
  process: ChildProcess | null;
  logFn: (line: string) => void;
  role: Role;
  id?: string;
  version?: VersionResponse;
  dir?: string;
}
