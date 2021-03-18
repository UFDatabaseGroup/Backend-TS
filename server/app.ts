import express from 'express';
import morgan from 'morgan';
import next from 'next';
import log from './logger';
import api from './api';
import { BASE_URL, API_BASE_URL } from './config';

let nextApp = next({
  dev: process.env.NODE_ENV !== 'production'
});
let handleNext = nextApp.getRequestHandler();
let app = express();

app.set('trust proxy', 'loopback');

app.use(morgan('combined', {
  stream: {
    write(line) {
      log.info(line.replace(/\n$/g, ''));
    }
  }
}));

// web application
nextApp.prepare().then(() => {
  app.use(BASE_URL.pathname, (req, res) => {
    handleNext(req, res);
  });
});

app.use(API_BASE_URL.pathname, api);

export default app;
