require('dotenv').config(); // eslint-disable-line @typescript-eslint/no-var-requires
import { promises as fsP } from 'fs';
import http from 'http';
import https from 'https';
import app from './app';
import log from './logger';
import * as config from './config';

/** Main function */
async function main() {
  /** @type {http.Server | https.Server} */
  let server;
  if (config.TLS) {
    if (!config.TLS_CERT || !config.TLS_KEY) throw new Error('no tls key or cert specified');
    server = https.createServer({
      cert: await fsP.readFile(config.TLS_CERT),
      key: await fsP.readFile(config.TLS_KEY)
    }, app);
  } else server = http.createServer(app);
  server.listen(config.PORT).on('listening', () => log.info('Listening'));
}
main();
