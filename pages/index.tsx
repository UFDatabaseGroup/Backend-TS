import React from 'react';
import Head from 'next/head';
import ListLink from '../components/ListLink';

export default function App() {
  return <div>
    <Head>
      <title>Index</title>
    </Head>
    <h2>Index of random stuff</h2>
    <ul>
      <ListLink target="blah" />
      <ListLink target="login" />
    </ul>
  </div>;
}
