import { URL } from 'url';

/**
 * Get an environment variable
 * @param name Environment variable name
 * @param required Whether the option is required
 * @return Environment variable value or null if it is not defined
 */
function get(name: string, required: true): string;
function get(name: string, required?: boolean): string | null;
function get(name: string, required?: boolean): string | null {
  let has = Object.prototype.hasOwnProperty.call(process.env, name);
  if (!has) {
    if (required) throw new Error(`required value ${name} not provided`);
    else return null;
  }
  let value = process.env[name]!;
  return value;
}

function string(name: string, defaultValue?: string): string;
function string(name: string, defaultValue: null): string | null;
function string(name: string, defaultValue?: string | null): string | null {
  return get(name, typeof defaultValue === 'undefined');
}

/** Get an environment variable with a number value */
function number(name: string, defaultValue?: number): number;
function number(name: string, defaultValue: null): number | null;
function number(name: string, defaultValue?: number | null): number | null {
  let value = get(name, typeof defaultValue === 'undefined');
  if (value === null) return defaultValue!;
  let numberValue = +value;
  if (Number.isNaN(numberValue)) {
    throw new Error(`invalid number [${value}] provided for variable [${name}]`);
  }
  return numberValue;
}

/** Get an environment variable with a boolean value */
function boolean(name: string, defaultValue?: boolean): boolean;
function boolean(name: string, defaultValue: null): boolean | null;
function boolean(name: string, defaultValue?: boolean | null): boolean | null {
  let value = get(name, typeof defaultValue === 'undefined');
  if (value === null) return defaultValue!;
  switch (value.toLowerCase()) {
    case '0':
    case 'false':
    case 'no':
      return false;
    case '1':
    case 'true':
    case 'yes':
      return true;
    default:
      throw new Error(`unrecognized boolean value [${value}] provided for variable [${name}]`);
  }
}

function url(name: string, defaultValue?: URL): URL;
function url(name: string, defaultValue: null): URL | null;
function url(name: string, defaultValue?: URL | null): URL | null {
  let value = get(name, typeof defaultValue === 'undefined');
  if (value === null) return defaultValue!;
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch (err) {
    if (err.code === 'ERR_INVALID_URL') {
      throw new Error(`invalid URL [${value}] provided for variable [${name}]`);
    } else {
      throw new Error(`error parsing URL variable [${name}]: ${err}`);
    }
  }
  return parsed;
}

export const BASE_URL = url('BASE_URL');
export const API_BASE_URL = url('API_BASE_URL');
export const PORT = number('PORT');
export const TLS = boolean('TLS', false);
export const TLS_CERT = TLS ? string('TLS_CERT') : null;
export const TLS_KEY = TLS ? string('TLS_KEY') : null;
export const LOG_LEVEL = string('LOG_LEVEL', 'info');
export const LOG_COLORS = boolean('LOG_COLORS', true);
