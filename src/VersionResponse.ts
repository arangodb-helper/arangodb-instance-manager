export type Version = {
  major: number;
  minor: number;
  patch: number | string;
};

export default class VersionResponse {
    server: "arango";
    license: "community" | "enterprise";
    version: Version;

    // construct from a json response like this:
    // {"server":"arango","version":"3.4.0","license":"enterprise"}
    constructor(response: any) {
        for (const prop of ["server", "license", "version"]) {
            if (!response.hasOwnProperty(prop)) {
                throw new TypeError(`Missing property .${prop}`);
            }
        }
        if (Object.keys(response).length !== 3) {
            throw new TypeError(`Unexpected properties in response, response is ${JSON.stringify(response)}`);
        }

        if (response.server !== "arango") {
            throw new TypeError(`Unexpected property .server=${response.server}; expected "arango".`);
        }
        if (response.license !== "community" && response.license !== "enterprise") {
            throw new TypeError(`Unexpected property .license=${response.license}; expected "community" or "enterprise.`);
        }
        const versionRe = /^(\d+)\.(\d+)\.(.*)$/;
        let versionReResult;
        if (typeof response.version !== "string" || !(versionReResult = versionRe.exec(response.version))) {
            // Be a bit lenient here, allow e.g. 3.4.devel. But we do need major and minor version.
            throw new TypeError(`Unexpected property .version=${response.version}; expected a version string.`);
        }

        this.server = response.server;
        this.license = response.license;
        let [majorStr, minorStr, patchStr] = versionReResult[0];
        let major, minor, patch;
        major = parseInt(majorStr);
        minor = parseInt(minorStr);
        if (/^\d+$/.test(patchStr)) {
            patch = parseInt(patchStr);
        } else {
            patch = patchStr;
        }
        this.version = {
            major, minor, patch
        };
    }
}
