import { ChildProcess } from "child_process";

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
}
