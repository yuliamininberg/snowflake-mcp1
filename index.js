import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

// ------------------------------------------------------------
// 1. Snowflake Connection
// ------------------------------------------------------------

function createConnection() {
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  });

  connection.connect((err) => {
    if (err) {
      console.error("❌ Snowflake connection failed:", err);
    } else {
      console.log("✔ Connected to Snowflake");
    }
  });

  return connection;
}

const connection = createConnection();

// ------------------------------------------------------------
// 2. MCP Server
// ------------------------------------------------------------

const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0",
});

// Register run_query tool
server.registerTool(
  "run_query",
  {
    sql: z.string(),
  },
  async ({ sql }) => {
    console.log("🔍 Running SQL:", sql);

    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error("❌ Query failed:", err);
            console.error("❌ Snowflake query error:", err);
            reject({ error: err.message });

          } else {
            console.log("✔ Query succeeded");
            resolve({ rows });
          }
        },
      });
    });
  }
);

console.log("✔ MCP tool registered: run_query");

// ------------------------------------------------------------
// 3. EXPRESS SERVER — manual JSON-RPC handler
// ------------------------------------------------------------

const app = express();
app.use(express.json());

// POST /mcp endpoint (Claude + curl calls)
app.post("/mcp", async (req, res) => {
  const body = req.body;

  console.log("📩 Incoming RPC:", body);

  if (!body || !body.method) {
    return res.status(400).json({ error: "Invalid request" });
  }

  if (body.method === "callTool") {
    const { name, arguments: args } = body.params;

    try {
      const result = await server.callTool(name, args);

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      res.write(`event: message\n`);
      res.write(`data: ${JSON.stringify({
        jsonrpc: "2.0",
        id: body.id,
        result,
      })}\n\n`);

      return res.end();
    } catch (err) {
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.write(`event: message\n`);
      res.write(`data: ${JSON.stringify({
        jsonrpc: "2.0",
        id: body.id,
        error: { code: -32000, message: err.error || "Tool failed" },
      })}\n\n`);
      return res.end();
    }
  }

  // Unknown method
  res.writeHead(200, { "Content-Type": "text/event-stream" });
  res.write(`event: message\n`);
  res.write(`data: ${JSON.stringify({
    jsonrpc: "2.0",
    id: body.id,
    error: { code: -32601, message: "Method not found" },
  })}\n\n`);
  res.end();
});

// simple GET
app.get("/mcp", (req, res) => {
  res.json({ status: "MCP running" });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 MCP server running on port ${PORT}`);
});
