import express from 'express';
import bodyParser from 'body-parser';
import Ajv from 'ajv';

let router = express.Router(); // eslint-disable-line new-cap
let jsonParse = bodyParser.json();
let ajv = new Ajv();
export let count = 0;

router.get('/', (_req, res) => res.send({ count }));

let counterPostSchema = ajv.compile({
  type: 'object',
  properties: {
    count: { type: 'number' }
  }
});
router.post('/', jsonParse, (req, res) => {
  if (!counterPostSchema(req.body)) {
    res.status(400).send({
      status: 'error',
      error: 'VALIDATION_FAILED',
      details: counterPostSchema.errors
    });
    return;
  }
  count = req.body.count;
  res.send({ status: 'ok' });
});

export default router;
