import express from 'express';

/**
 * Wraps an async function handler
 * @param fn
 * @return
 */
export function wrapAsync(fn: express.RequestHandler): express.RequestHandler {
  return (req, res, next) => fn(req, res, next).catch(next);
}
