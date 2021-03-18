import React from 'react';
import Head from 'next/head';
import Button from '@material-ui/core/Button';
import AppMenu from '../components/AppMenu';
import { Paper, Box, TextField, Typography, Grid } from '@material-ui/core';
import styles from './login.module.scss';

// dummy material design login box
export default function Login() {
  return <React.Fragment>
    <Head>
      <title>example test</title>
    </Head>
    <AppMenu page="Login" />
    <Paper className={styles.loginBox} elevation={3}>
      <Grid container spacing={2} direction="column">
        <Grid item>
          <Typography variant="h5" align="center">Login</Typography>
        </Grid>
        <Grid item>
          <TextField id="standard-basic" label="Email" fullWidth={true} />
        </Grid>
        <Grid item>
          <TextField id="standard-basic" label="Password" fullWidth={true} />
        </Grid>
        <Grid item container justify="space-between">
          <Grid item>
            <Button color="primary">
              Register
            </Button>
          </Grid>
          <Grid item>
            <Button variant="contained" color="primary">
              Login
            </Button>
          </Grid>
        </Grid>
      </Grid>
    </Paper>
  </React.Fragment>;
}
