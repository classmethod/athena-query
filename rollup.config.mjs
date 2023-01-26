import commonjs from "@rollup/plugin-commonjs";
import typescript from "@rollup/plugin-typescript";

export default {
  input: "index.ts",
  output: [
    {
      file: "dist/index.esm.mjs",
    },
    {
      file: "dist/index.cjs.js",
      format: "cjs",
      exports: "default",
    },
  ],
  plugins: [typescript({ exclude: ["test/**"] }), commonjs()],
};
