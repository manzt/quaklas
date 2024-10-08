/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
// deno-lint-ignore-file prefer-const
// @ts-check
// @deno-types="npm:@uwdata/mosaic-core@0.10.0"
import * as mosaic from "https://esm.sh/@uwdata/mosaic-core@0.10.0?bundle";
// @deno-types="jsr:@manzt/quak@0.0.0"
import * as quak from "https://esm.sh/jsr/@manzt/quak@0.0.0?bundle";

/**
 * @param {unknown} condition
 * @param {string} message
 * @return {asserts condition}
 */
function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

/**
 * Quak does not expose an API to format the table cells.
 *
 * This is a little hack to observe changes (mutations) in the HTML <table>
 * (i.e., when quak adds/removes rows) and then replace the text content of a
 * <td> (cell) with an <a> (anchor) element that links to a URL of choice.
 *
 * @param {quak.DataTable} dt - the quak DataTable
 * @param {Record<string, (v: string) => HTMLElement | string>} format - a mapping of column name to a function that reformats the cell contents
 */
function applyCellReformatting(dt, format) {
  // Probably should avoid grabbing the shadowRoot directly...
  let tableElement = dt.node().shadowRoot?.querySelector("table");
  assert(tableElement, "Could not find the <table> element from the DataTable");

  // we need to wait until quak renders the table header to inspect the columns
  let outer = new MutationObserver((mutations) => {
    for (let mutation of mutations) {
      // we just want changes to thead
      if ("tagName" in mutation.target && mutation.target.tagName !== "THEAD") {
        continue;
      }
      // get the columns
      let columns = Array.from(
        tableElement.querySelectorAll("thead th div span:nth-child(1)"),
        (th) => th.textContent,
      );
      assert(columns.length > 0, "Could not find any columns in the DataTable");

      /** @type {Array<[number, (v: string) => HTMLElement | string]>} */
      let formatters = Object
        .entries(format)
        .filter(([col]) => columns.includes(col)) // ignore columns that are not in the table
        .map(([col, func]) => [columns.indexOf(col), func]);

      // Create an observer instance that calls our callback anytime a mutation (DOM change) occurs
      // https://developer.mozilla.org/en-US/docs/Web/API/MutationObserver
      let inner = new MutationObserver((mutations) => {
        for (let mutation of mutations) {
          // @ts-expect-error - type is not defined in the TypeScript type definitions
          if (!mutation.type == "childList") {
            continue; // skip if not a childList mutation
          }
          for (let node of mutation.addedNodes) {
            assert(
              node instanceof HTMLTableRowElement,
              "Expected a table row element",
            );
            let tds = node.querySelectorAll("td");
            for (let [columnIndex, reformat] of formatters) {
              // zero-based index of the column to format
              let td = tds[columnIndex + 1]; // skip index column
              /** @type {HTMLTableCellElement} */
              if (!td?.textContent) continue;
              let rendered = reformat(td.textContent);
              td.replaceChildren();
              if (typeof rendered === "string") {
                td.textContent = rendered;
              } else {
                td.appendChild(rendered);
              }
            }
          }
        }
      });
      inner.observe(tableElement.querySelector("tbody"), { childList: true });
      outer.disconnect();
    }
  });
  outer.observe(tableElement.querySelector("thead"), { childList: true });
}

/**
 * Register the parquet files as tables in the DuckDB database
 * @param {{ registerFileBuffer(name: string, bytes: Uint8Array): Promise<void> }} db - the DuckDB database
 * @param {Array<"obs" | "cells">} tables - the list of table names
 */
async function registerTables(db, tables) {
  let promises = tables.map(async (tableName) => {
    let url = new URL(
      /* @vite-ignore */ `assets/${tableName}.parquet`,
      import.meta.url,
    );
    let response = await fetch(url);
    let bytes = new Uint8Array(await response.arrayBuffer());
    await db.registerFileBuffer(`${tableName}.parquet`, bytes);
  });
  await Promise.all(promises);
}

/**
 * Main function to create a DataTable from a parquet file
 *
 * @param {HTMLElement} el - the parent element to append the DataTable
 * @param {object} options - options
 * @param {"obs" | "cells"} options.mode - the embed mode
 */
export async function embed(el, options) {
  let query = queryFromParams(new URLSearchParams(globalThis.location.search));
  // Setup mosaic (query engine) with WASM DuckDB
  let connector = mosaic.wasmConnector();
  mosaic.coordinator().databaseConnector(connector);

  await registerTables(
    await connector.getDuckDB(),
    options.mode === "obs" ? ["obs"] : ["obs", "cells"], // don't load the cells table if not needed
  );

  await mosaic.coordinator().exec([
    `CREATE TABLE obs AS SELECT * FROM read_parquet('obs.parquet')`,
    `CREATE VIEW obs_subset AS ${query ?? `SELECT * FROM obs`}`,
    ...(options.mode === "obs" ? [] : [
      `CREATE TABLE cells AS SELECT * FROM read_parquet('cells.parquet')`,
      `CREATE VIEW cells_subset AS SELECT cells.* FROM obs_subset JOIN cells ON obs_subset.observation_id = cells.observation_id`,
    ]),
  ]);

  // create the quak datatable of the table
  let dt = await quak.datatable(`${options.mode}_subset`, {
    coordinator: mosaic.coordinator(),
    height: 500,
  });

  // apply custom formatters to the cells
  applyCellReformatting(dt, {
    observation_id(v) {
      let a = Object.assign(document.createElement("a"), {
        innerText: v,
        href: `https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${v}`,
        target: "_blank",
      });
      a.style.fontVariantNumeric = "tabular-nums";
      return a;
    },
  });

  el.appendChild(dt.node());

  if (options.mode === "obs") {
    let ul = document.createElement("ul");
    ul.style.marginTop = "1rem";
    el.appendChild(ul);

    // Probably a better way to do this than propagating the entire SQL string to the URL
    // We could probably have the predicates much more nicely formatted
    // (e.g. ?organ=brain&species=homospaiens&... )
    // but for now this "works" to demonstrate linking to another view
    let links = [document.createElement("a"), document.createElement("a")];
    {
      let a = links[0];
      a.href = globalThis.location.href;
      a.innerText = "view subset →";
      let li = document.createElement("li");
      li.appendChild(a);
      ul.appendChild(li);
    }

    {
      let a = links[1];
      let url = new URL(globalThis.location.href);
      url.pathname = `${url.pathname.replace(/\/$/, "")}/cells`;
      a.href = url.href;
      a.innerText = "view cells →";
      let li = document.createElement("li");
      li.appendChild(a);
      ul.appendChild(li);
    }

    dt.sql.subscribe((/** @type {string | undefined} */ sql) => {
      if (!sql) return;
      let params = new URLSearchParams(globalThis.location.search);
      serializeAndWriteFilterSearchParams(params, sql);
      for (let a of links) {
        // replace the search params on the links
        let url = new URL(a.href);
        url.search = params.toString();
        a.href = url.href;
      }
    });
  }
}

/** @typedef {{ field: string, kind: "range", values: [min: number, max: number] }} RangeFilter */
/** @typedef {{ field: string, kind: "categorical", values: Array<string> }} CategoricalFilter */
/** @typedef {RangeFilter | CategoricalFilter} Filter */

/** @param {string} sql */
function extractFilters(sql) {
  // TODO: We should probably get something back from quak that is more structured
  // rather than this hacky parsing
  let whereMatch = sql.match(
    /WHERE\s+(.*?)(?:\s+(?:ORDER BY|LIMIT|GROUP BY)\s|$)/i,
  );
  if (!whereMatch) return [];
  let conditions = whereMatch[1].split(") AND (").map((f) => f.trim());

  /** @type {Array<Filter>} */
  let filters = [];
  for (let cond of conditions) {
    // '("<field>" IS NOT DISTINCT FROM '<value>')'
    let match = cond.match(/"([^"]+)" IS NOT DISTINCT FROM '([^']+)'/);
    if (match) {
      filters.push({
        field: match[1],
        kind: "categorical",
        values: [match[2]],
      });
      continue;
    }

    // '("<field>" BETWEEN <min> AND <max>)'
    match = cond.match(/"([^"]+)" BETWEEN (\d+.\d+) AND (\d+.\d+)/);
    if (match) {
      filters.push({
        field: match[1],
        kind: "range",
        values: [Number(match[2]), Number(match[3])],
      });
      continue;
    }

    throw new Error(`Unsupported condition: ${cond}`);
  }

  return filters;
}

/**
 * @param {URLSearchParams} params
 * @param {string} sql
 */
function serializeAndWriteFilterSearchParams(params, sql) {
  let filters = extractFilters(sql);
  for (let filter of filters) {
    if (filter.kind === "range") {
      params.append(
        filter.field,
        filter.values.join(" "),
      );
      continue;
    }
    params.append(
      filter.field,
      filter.values.map((v) => v.includes(" ") ? `"${v}"` : v).join(" "),
    );
  }
}

/**
 * @param {URLSearchParams} params
 * @returns {string | null}
 */
function queryFromParams(params) {
  let filters = Array
    .from(params.entries())
    .map(([field, value]) => deserializeFilterSearchParam(field, value));

  if (filters.length === 0) {
    return null;
  }

  let clauses = filters.map((filter) => {
    if (filter.kind === "range") {
      return `"${filter.field}" BETWEEN ${filter.values[0]} AND ${
        filter.values[1]
      }`;
    }
    return `"${filter.field}" IN (${
      filter.values.map((v) => `'${v}'`).join(", ")
    })`;
  });

  return `SELECT * FROM obs WHERE ${clauses.join(" AND ")}`;
}

/**
 * @param {string} field
 * @param {string} value
 * @returns {Filter}
 */
function deserializeFilterSearchParam(field, value) {
  // check first if we have '<number> <number>'
  let range = value.match(/(\d+.\d+) (\d+.\d+)/);
  if (range) {
    return {
      field,
      kind: "range",
      values: [Number(range[1]), Number(range[2])],
    };
  }
  return {
    field,
    kind: "categorical",
    values: parseEntities(value),
  };
}

/**
 * @param {string} input
 * @returns {Array<string>}
 */
function parseEntities(input) {
  // This regex matches either:
  // 1. A sequence of non-space characters
  // 2. A quoted string (allowing escaped quotes inside)
  const regex = /([^\s"]+)|"([^"\\]*(?:\\.[^"\\]*)*)"/g;
  const entities = [];
  let match;
  while ((match = regex.exec(input)) !== null) {
    // If it's an unquoted entity
    if (match[1]) {
      entities.push(match[1]);
    } // If it's a quoted entity
    else if (match[2]) {
      entities.push(match[2].replace(/\\"/g, '"')); // Replace escaped quotes
    }
  }
  return entities;
}
