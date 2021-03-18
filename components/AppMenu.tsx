import React from 'react';
import { AppBar, Toolbar, IconButton, Typography } from '@material-ui/core';
import Menu from '@material-ui/icons/Menu';
import theme from '../client/theme';
import Link from 'next/link';

const AppMenu: React.FC<{ page: string }> = ({ page }) => (
  <AppBar position="static">
    <Toolbar>
      <Link href="/" passHref>
        <IconButton edge="start" color="inherit" style={{ marginRight: theme.spacing(2) }}>
          <Menu />
        </IconButton>
      </Link>
      <Typography variant="h6">
        {page}
      </Typography>
    </Toolbar>
  </AppBar>
);

export default AppMenu;
