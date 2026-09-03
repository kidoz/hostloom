import type { Config } from "prettier";

// Indentation is not set here: the repository's root .editorconfig owns indent_style and
// indent_size, and Prettier reads it for every file it formats in this package.
const config: Config = {
    printWidth: 100,
};

export default config;
