import express from 'express';
import log from './logger';
import { wrapAsync as wrap } from './common';

let router = express.Router(); // eslint-disable-line new-cap

router.get('/', wrap(async (req, res) => {
  log.info('a request!');
  res.send('hello, world!');
}));

import counterRoute from './api/counter';
router.use('/counter', counterRoute);

export default router;
