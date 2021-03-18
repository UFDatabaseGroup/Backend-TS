module.exports = {
  extends: [
    'plugin:react/recommended',
    '@hellomouse/eslint-config-typescript'
  ],
  env: {
    browser: true,
    es6: true,
    node: true
  },
  plugins: [
    'react'
  ],
  settings: {
    react: {
      version: 'detect'
    }
  },
  rules: {
    // typescript
    'react/prop-types': 'off',
    // react components are usually PascalCase but explaining that to eslint is hard
    '@typescript-eslint/naming-convention': 'off',
    // new _jsx transform no longer needs this
    'react/react-in-jsx-scope': 'off',
    '@typescript-eslint/ban-types': ['error', {
      types: {
        // react uses {} as empty object even though it is completely allowed
        // to assign 4 to a variable of type {}
        '{}': false
      },
      extendDefaults: true
    }]
  }
};
