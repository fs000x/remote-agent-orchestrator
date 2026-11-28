/**
 * ssh2 promise helpers: password-auth client, one-shot exec, sftp upload.
 * No public keys are ever installed (user constraint: password auth only).
 */
import { Client, type ClientChannel, type SFTPWrapper } from "ssh2";
import { ErrorCode, RaoError } from "../protocol.js";
import { passwordOf, type DeviceRecord } from "../devices.js";

export interface ExecResult {
  code: number;
  stdout: string;
  stderr: string;
}

export function connectClient(device: DeviceRecord): Promise<Client> {
  const password = passwordOf(device);
  if (!password) {
    return Promise.reject(new RaoError(
      ErrorCode.AuthRequired,
      `no password available for device ${device.id} (config auth or keyring)`,
    ));
  }
  return new Promise((resolve, reject) => {
    const client = new Client();
    const timer = setTimeout(() => {
      client.end();
      reject(new RaoError(
        ErrorCode.HostOffline,
        `ssh ${device.user}@${device.host}: connect timeout`,
        true,
      ));
    }, 12000);
    client.on("ready", () => {
      clearTimeout(timer);
      resolve(client);
    });
    client.on("error", (err) => {
      clearTimeout(timer);
      reject(new RaoError(
        ErrorCode.ConnectionFailed,
        `ssh ${device.user}@${device.host}: ${err.message}`,
        true,
      ));
    });
    client.connect({
      host: device.host,
      port: device.port ?? 22,
      username: device.user,
      password,
      readyTimeout: 10000,
      // password auth ONLY (user constraint: never touch agent/keys)
      tryKeyboard: false,
    });
  });
}

export async function sshExec(
  device: DeviceRecord,
  cmd: string,
  timeoutMs = 15000,
): Promise<ExecResult> {
  const client = await connectClient(device);
  try {
    return await execOn(client, cmd, timeoutMs);
  } finally {
    client.end();
  }
}

export function execOn(
  client: Client,
  cmd: string,
  timeoutMs = 15000,
): Promise<ExecResult> {
  return new Promise((resolve, reject) => {
    client.exec(cmd, (err, stream) => {
      if (err) {
        reject(new RaoError(ErrorCode.TransportLost, `exec: ${err.message}`, true));
        return;
      }
      let stdout = "";
      let stderr = "";
      const timer = setTimeout(() => {
        stream.close();
        reject(new RaoError(ErrorCode.RpcTimeout, `exec timeout: ${cmd.slice(0, 80)}`, true));
      }, timeoutMs);
      stream.on("data", (c: Buffer) => { stdout += c.toString("utf8"); });
      stream.stderr.on("data", (c: Buffer) => { stderr += c.toString("utf8"); });
      stream.on("close", (code: number) => {
        clearTimeout(timer);
        resolve({ code: code ?? -1, stdout, stderr });
      });
    });
  });
}

export function sftpPutFiles(
  client: Client,
  files: Array<{ localPath: string; remotePath: string }>,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    client.sftp((err, sftp: SFTPWrapper) => {
      if (err) {
        reject(new RaoError(ErrorCode.TransportLost, `sftp: ${err.message}`, true));
        return;
      }
      const uploaded: string[] = [];
      const next = (i: number): void => {
        if (i >= files.length) {
          sftp.end();
          resolve(uploaded);
          return;
        }
        const f = files[i];
        if (!f) { next(i + 1); return; }
        const { localPath, remotePath } = f;
        sftp.fastPut(localPath, remotePath, (e) => {
          if (e) {
            sftp.end();
            reject(new RaoError(ErrorCode.TransportLost, `sftp put ${remotePath}: ${e.message}`, true));
            return;
          }
          uploaded.push(remotePath);
          next(i + 1);
        });
      };
      next(0);
    });
  });
}

export async function sshMkdirp(device: DeviceRecord, dir: string): Promise<void> {
  await sshExec(device, `mkdir -p "${dir}"`, 10000);
}

export type { ClientChannel };
