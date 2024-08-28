## data preparation

Run the following in `cellatlas/human`.

```sh
cat data/*/observation.json | duckdb -c "COPY (SELECT * EXCLUDE links FROM read_json_auto('/dev/stdin')) TO 'obs.parquet'"
```

## development

This project doesn't need any build tools; it's just a standalone HTML file.

You can use Vite to serve the files locally (and get hot reloading).

```sh
npx vite
```

## code quality

You can format the code directly with deno.

```sh
deno fmt --unstable-html index.html
```
