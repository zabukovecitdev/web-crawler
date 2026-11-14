import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import sonarjs from 'eslint-plugin-sonarjs'
import tseslint from 'typescript-eslint'

// The same limits as the C# build (see README.md, "Limits"). Every rule is an error.
// A React component is one function that holds its markup, so .tsx files get a
// longer function limit than the 30 lines of .ts files and of C# methods.
const limits = {
  complexity: ['error', { max: 8 }],
  'sonarjs/cognitive-complexity': ['error', 10],
  'max-lines-per-function': ['error', { max: 30, skipBlankLines: true, skipComments: true }],
  'max-params': ['error', { max: 6 }],
  'max-depth': ['error', { max: 2 }],
  'max-lines': ['error', { max: 300, skipBlankLines: true, skipComments: true }],
  curly: ['error', 'all'],
}

export default tseslint.config(
  { ignores: ['dist'] },
  {
    // The same as the C# ban on #pragma and [SuppressMessage]: no eslint-disable comments.
    linterOptions: { noInlineConfig: true, reportUnusedDisableDirectives: 'error' },
  },
  {
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    plugins: {
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
      sonarjs,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': ['error', { allowConstantExport: true }],
      '@typescript-eslint/ban-ts-comment': 'error',
      ...limits,
    },
  },
  {
    files: ['**/*.tsx'],
    rules: {
      'max-lines-per-function': ['error', { max: 60, skipBlankLines: true, skipComments: true }],
    },
  },
)
