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
console.log("DEBUG full server object:", server);


// Register the run_query tool
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
            console.error("❌ Snowflake query error:", err);
            return reject({ error: err.message });
          }

          console.log("✔ Query succeeded:", rows);
          resolve({ rows });
        },
      });
    });
  }
);

console.log("✔ MCP tool registered: run_query");

// ------------------------------------------------------------
// 3. EXPRESS HTTP SERVER — JSON-RPC Handler for /mcp
// ------------------------------------------------------------

const app = express();
app.use(express.json());

// Handle POST /mcp
app.post("/mcp", async (req, res) => {
  const body = req.body;

  console.log("📩 Incoming RPC:", body);

  if (!body || !body.method) {
    return res.status(400).json({ error: "Invalid request" });
  }

  // Handle MCP callTool
if (body.method === "callTool") {
  const toolName = body.params.name;
  const toolArgs = body.params.arguments;

  console.log("👉 Calling tool:", toolName, "ARGS:", toolArgs);

  try {
    // FIX: Your tools live in _registeredTools
    const tool = server._registeredTools.get(toolName);

    if (!tool) {
      throw new Error(`Tool '${toolName}' not found`);
    }

    // FIX: invoke tool handler
    const result = await tool.impl(toolArgs);

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });

    res.write(`event: message\n`);
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

    res.write(`event: message\n`);
    res.write(
      `data: ${JSON.stringify({
        jsonrpc: "2.0",
        id: body.id,
        error: { code: -32000, message: err.message || "Tool failed" },
      })}\n\n`
    );

    return res.end();
  }
}


  // Unsupported method
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

// Simple GET for testing
app.get("/mcp", (req, res) => {
  res.json({ status: "MCP server running" });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 MCP server running on port ${PORT}`);
});
