import "dotenv/config";
import express from "express";
import snowflake from "snowflake-sdk";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

// ------------------------------------------------------------
// 1. Create Snowflake Connection
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
// 2. Create MCP Server & Register Tools
// ------------------------------------------------------------

const server = new McpServer({
  name: "snowflake-mcp",
  version: "1.0.0",
});

console.log("DEBUG: MCP server object keys:", Object.keys(server));
console.log(
  "Available server methods:",
  Object.getOwnPropertyNames(Object.getPrototypeOf(server))
);

// REGISTER TOOL (correct API for your SDK)
server.tool(
  "run_query",
  z.object({
    sql: z.string(),
  }),
  async ({ sql }) => {
    console.log("🔍 Running SQL:", sql);

    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error("❌ Snowflake query error:", err);
            return reject(err);
          }

          console.log("✔ Query succeeded:", rows);
          resolve({ rows });
        },
      });
    });
  }
);

console.log("✔ MCP tool registered: run_query");
console.log("DEBUG registered tools:", server._registeredTools);

// ------------------------------------------------------------
// 3. EXPRESS JSON-RPC HANDLER
// ------------------------------------------------------------

const app = express();
app.use(express.json());

app.post("/mcp", async (req, res) => {
  console.log("📩 Incoming RPC:", req.body);
  const body = req.body;

  if (body.method === "callTool") {
    const toolName = body.params.name;
    const toolArgs = body.params.arguments;

    console.log("👉 Calling tool:", toolName, toolArgs);

    try {
      // FIX: use SDK's built-in execution function
      const result = await server.executeToolHandler({
        name: toolName,
        arguments: toolArgs,
      });

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      res.write("event: message\n");
      res.write(
        `data: ${JSON.stringify({
          jsonrpc: "2.0",
          id: body.id,
          result,
        })}\n\n`
      );
      return res.end();
    } catch (err) {
      console.error("❌ Tool failed:", err);

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
      });

      res.write("event: message\n");
      res.write(
        `data: ${JSON.stringify({
          jsonrpc: "2.0",
          id: body.id,
          error: {
            code: -32000,
            message: err.message || "Tool failed",
          },
        })}\n\n`
      );
      return res.end();
    }
  }

  // fallback
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
  });

  res.write(`event: message\n`);
  res.write(
    `data: ${JSON.stringify({
      jsonrpc: "2.0",
      id: body.id,
      error: { code: -32601, message: "Method not found" },
    })}\n\n`
  );

  res.end();
});

// ------------------------------------------------------------
// SIMPLE STATUS ENDPOINT
// ------------------------------------------------------------
app.get("/mcp", (req, res) => {
  res.json({ status: "MCP server running" });
});

// ------------------------------------------------------------
// START SERVER
// ------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 MCP server running on port ${PORT}`);
});
