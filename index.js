import "dotenv/config";
import express from "express";
import fs from "node:fs";
import snowflake from "snowflake-sdk";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

// ==== Snowflake connection ====
function createConnection() {
  const privateKey = process.env.SNOWFLAKE_PRIVATE_KEY_PEM.replace(/\\n/g, "\n");

  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    privateKey,
    privateKeyPass: process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
}

function runQuery(sql) {
  const connection = createConnection();

  return new Promise((resolve, reject) => {
    connection.connect(err => {
      if (err) return reject(err);

      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          connection.destroy(() => {});
          if (err) reject(err);
          else resolve(rows || []);
        }
      });
    });
  });
}

// ==== MCP Server ====
const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0"
});

const forbidden = /\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE)\b/i;

server.tool(
  "run_query",
  "Run a SELECT SQL query on Snowflake",
  {
    sql: z.string()
  },
  async ({ sql }) => {
    if (forbidden.test(sql)) {
      throw new Error("Only SELECT queries are allowed");
    }

    const rows = await runQuery(sql);

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(rows)
        }
      ]
    };
  }
);

// ==== HTTP server ====
async function main() {
  const app = express();
  app.use(express.json());

  app.get("/health", (_, res) => res.json({ ok: true }));

  const transport = new StreamableHTTPServerTransport({
    app,
    endpoint: "/mcp"
  });

  server.connect(transport);

  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
}

main();
