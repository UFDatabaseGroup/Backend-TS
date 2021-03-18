import React from 'react';
import NextLink from 'next/link';
import { Link as MuiLink } from '@material-ui/core';
import Link from './Link';

// i am lazy, please do not judge
const ListLink: React.FunctionComponent<{ target: string }> = ({ target }) => {
  return <li>
    <Link href={'/' + target}>{target}</Link>
  </li>;
};

export default ListLink;
