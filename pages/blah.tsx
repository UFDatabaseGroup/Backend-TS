import React from 'react';
import Head from 'next/head';
import styles from './blah.module.scss';
import Link from '../components/Link';

// testing css modules, delete when done
export default function Blah() {
  return <div>
    <Head>
      <title>css modules test page</title>
    </Head>
    <p>
      <span className={styles.things}>Hi!</span>
      other things here...
    </p>
    <p><Link href="/">go home</Link></p>
  </div>;
}
