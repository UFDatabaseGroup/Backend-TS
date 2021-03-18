import winston, { Logform, format, transports } from 'winston';
import { formatWithOptions } from 'util';
import { LOG_LEVEL, LOG_COLORS } from './config';

const FORMAT_OPTIONS = {
  depth: null,
  maxArrayLength: null,
  colors: LOG_COLORS
};

export let logger = winston.createLogger({
  level: LOG_LEVEL,
  transports: [
    new transports.Console()
  ],
  format: format.combine(
    LOG_COLORS
      ? format.colorize()
      : format.uncolorize(),
    format.printf((info: Logform.TransformableInfo) => {
      // @ts-ignore typescript pls
      let splat = info[Symbol.for('splat')];
      return [
        `${info.level}:`,
        splat
          ? formatWithOptions(FORMAT_OPTIONS, info.message, ...splat)
          : formatWithOptions(FORMAT_OPTIONS, info.message)
      ].join(' ');
    })
  )
});

export default logger;
