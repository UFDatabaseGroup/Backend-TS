import React from 'react';
import NextLink from 'next/link';
import MuiLink from '@material-ui/core/Link';

/** Extremely simple next-wrapped material-ui link */
const Link: React.FC<{ href: string | URL }> = props => (
  <NextLink href={props.href} passHref>
    <MuiLink>{props.children}</MuiLink>
  </NextLink>
);

export default Link;
