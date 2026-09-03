import babelParser from "@babel/eslint-parser";
import eslint from "@eslint/js";
import { defineConfig } from "eslint/config";
import eslintConfigPrettier from "eslint-config-prettier";
import globals from "globals";

export default defineConfig([
    {
        ignores: ["dist/**", "node_modules/**"],
    },
    eslint.configs.recommended,
    {
        files: ["scripts/**/*.ts", "src/**/*.ts", "test/**/*.ts"],
        languageOptions: {
            parser: babelParser,
            parserOptions: {
                requireConfigFile: false,
                babelOptions: {
                    presets: ["@babel/preset-typescript"],
                },
            },
            globals: globals.browser,
        },
        rules: {
            // TypeScript 7's compiler owns symbol resolution until typescript-eslint supports TS7.
            "no-undef": "off",
            "no-unused-vars": "off",
        },
    },
    {
        files: ["scripts/**/*.ts", "test/**/*.ts"],
        languageOptions: {
            globals: globals.node,
        },
    },
    eslintConfigPrettier,
]);
