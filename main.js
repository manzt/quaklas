/// <reference lib="dom" />
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
 * @param {object} options - options
 * @param {(v: string) => HTMLElement} options.format - a function that takes the cell value and returns an HTMLElement
 * @param {number} options.columnIndex - the 0-based index of the column to format
 */
function observeTableChangesAndFormatColumnCell(dt, {
  format,
  columnIndex,
}) {
  // Probably should avoid grabbing the shadowRoot directly...
  let tableElement = dt.node().shadowRoot?.querySelector("table");
  assert(tableElement, "Could not find the <table> element from the DataTable");

  // Create an observer instance that calls our callback anytime a mutation (DOM change) occurs
  // https://developer.mozilla.org/en-US/docs/Web/API/MutationObserver
  let observer = new MutationObserver((mutations) => {
    for (let mutation of mutations) {
      // @ts-expect-error - type is not defined in the TypeScript type definitions
      if (!mutation.type == "childList") {
        continue; // skip if not a childList mutation
      }
      // @ts-expect-error - type is not defined in the TypeScript type definitions
      for (let node of mutation.addedNodes) {
        // zero-based index of the column to format
        /** @type {HTMLTableCellElement} */
        let td = node?.querySelector?.(`td:nth-child(${columnIndex + 1})`);
        if (!td?.textContent) continue;
        let rendered = format(td.textContent);
        td.textContent = ""; // clear the text content
        td.appendChild(rendered);
      }
    }
  });
  observer.observe(tableElement, { childList: true, subtree: true });
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

  /** @type {Array<"obs" | "cells">} */
  let tables = options.mode === "obs" ? ["obs"] : ["obs", "cells"];

  await registerTables(await connector.getDuckDB(), tables);

  /** @type {Array<string>} */
  // create a temporary table from the parquet file in the :memory: database
  let execs = tables.map(
    (tbl) =>
      `CREATE OR REPLACE TABLE ${tbl} AS SELECT * FROM read_parquet('${tbl}.parquet')`,
  );
  execs.push(
    `CREATE OR REPLACE VIEW obs_view AS ${query ?? `SELECT * FROM obs`}`,
  );
  if (options.mode === "cells") {
    execs.push(
      `CREATE OR REPLACE VIEW cells_subset AS SELECT cells.* FROM obs_view JOIN cells ON obs_view.observation_id = cells.observation_id`,
    );
  }
  await mosaic.coordinator().exec(execs);

  // create the quak datatable of the table
  let dt = await quak.datatable(
    options.mode === "cells" ? "cells_subset" : query ? "obs_view" : "obs",
    {
      coordinator: mosaic.coordinator(),
      height: 500,
    },
  );

  // Little bit of a hacky way to find the inner <table> element.
  observeTableChangesAndFormatColumnCell(dt, {
    columnIndex: options.mode === "obs" ? 1 : 11,
    format(v) {
      let a = document.createElement("a");
      a.innerText = v;
      a.href = `https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${v}`;
      a.target = "_blank";
      a.style.fontVariantNumeric = "tabular-nums"; // looks better with monospacing
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
    {
      let a = document.createElement("a");
      a.href = globalThis.location.href;
      a.innerText = "view subset →";
      let li = document.createElement("li");
      li.appendChild(a);
      ul.appendChild(li);
    }

    {
      let a = document.createElement("a");
      let url = new URL(globalThis.location.href);
      url.pathname = `${url.pathname.replace(/\/$/, "")}/cells`;
      a.href = url.href;
      a.innerText = "view cells →";
      let li = document.createElement("li");
      li.appendChild(a);
      ul.appendChild(li);
    }

    dt.sql.subscribe((sql) => {
      if (!sql) return;
      let url = new URL(globalThis.location.href);
      writeSearchParams(url.searchParams, sql);
      for (let li of ul.children) {
        let a = li.querySelector("a");
        let aUrl = new URL(a.href);
        aUrl.search = url.search;
        a.href = aUrl.href;
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
function writeSearchParams(params, sql) {
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
    .map(([field, value]) => readSearchParam(field, value));

  if (filters.length === 0) {
    return null;
  }

  let clauses = filters.map((filter) => {
    if (filter.kind === "range") {
      return `${filter.field} BETWEEN ${filter.values[0]} AND ${
        filter.values[1]
      }`;
    }
    return `${filter.field} IN (${
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
function readSearchParam(field, value) {
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
