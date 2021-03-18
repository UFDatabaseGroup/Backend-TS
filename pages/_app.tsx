import React from 'react';
import App, { AppInitialProps } from 'next/app';
import Head from 'next/head';
import { CssBaseline, StylesProvider, ThemeProvider } from '@material-ui/core';
import theme from '../client/theme';

// roboto font
import 'fontsource-roboto/300.css';
import 'fontsource-roboto/400.css';
import 'fontsource-roboto/500.css';
import 'fontsource-roboto/700.css';
// to use cdn instead, use
// <link rel="stylesheet"
//   href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap" />

// import global styles
import './index.scss';

// define stuff that should exist on every page
export default class OverriddenApp extends App<AppInitialProps> {
  componentDidMount() {
    // Remove the server-side injected CSS.
    const jssStyles = document.getElementById('jss-server-side');
    if (jssStyles) {
      jssStyles.parentElement!.removeChild(jssStyles);
    }
  }

  render() {
    const { Component, pageProps } = this.props;
    return <>
      <Head>
        <meta name="viewport"
          content="minimum-scale=1, initial-scale=1, width=device-width" />
      </Head>
      <StylesProvider injectFirst>
        <ThemeProvider theme={theme}>
          <CssBaseline />
          <Component {...pageProps} />
        </ThemeProvider>
      </StylesProvider>
    </>;
  }
}
