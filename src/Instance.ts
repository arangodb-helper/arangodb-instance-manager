import { ChildProcess } from "child_process";
import VersionResponse from "./VersionResponse";

export default interface Instance {
  name: string;
  logFile?: string;
  endpoint: string;
  binary?: string;
  args: string[];
  port?: string;
  status: string;
  exitcode: number | null;
  process: ChildProcess | null;
  logFn: (line: string) => void;
  role: string;
  id?: string;
  version?: VersionResponse;
}
