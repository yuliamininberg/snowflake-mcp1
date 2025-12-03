import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

// === 1. Create Snowflake connection (PASSWORD version) ===
function createConnection() {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  });
}

async function runQuery(sql) {
  const connection = createConnection();

  return new Promise((resolve, reject) => {
    connection.connect((err) => {
      if (err) return reject(err);

      connection.execute({
        sqlText: sql,
        complete: (err, _stmt, rows) => {
          connection.destroy(() => {});
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });
  });
}

// === 2. Create MCP server ===
const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0",
});

// Only allow SELECT queries
const forbidden = /\b(UPDATE|DELETE|INSERT|MERGE|DROP|ALTER|TRUNCATE)\b/i;

server.tool(
  "run_query",
  "Run a SELECT SQL query on Snowflake",
  {
    sql: z.string(),
  },
  async ({ sql }) => {
    if (forbidden.test(sql)) {
      throw new Error("Only SELECT statements are allowed.");
    }

    const rows = await runQuery(sql);

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(rows),
        },
      ],
    };
  }
);

// === 3. Start HTTP server for MCP ===
async function main() {
  const app = express();
  app.use(express.json());

  // Health check
  app.get("/health", (_req, res) => res.json({ ok: true }));

  // Attach MCP transport
  const transport = new StreamableHTTPServerTransport({
    app,
    endpoint: "/mcp",
  });

  // MOST IMPORTANT: CONNECT THE MCP SERVER
  await server.connect(transport);

  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`MCP server running on port ${port}`);
  });
}

main().catch(err => {
  console.error("MCP server failed to start:", err);
});
