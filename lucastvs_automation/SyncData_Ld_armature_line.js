// // Push data from SQL to MongoDB (Service Broker Triggering)
// const sql = require("mssql/msnodesqlv8");
// const mongoose = require("mongoose");
// const winston = require("winston");

// // === Config ===
// const sqlConfig = {
//   connectionString:
//     "Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-MEFC1P0G;Database=demodb1;Trusted_Connection=Yes;TrustServerCertificate=Yes;",
//   driver: "msnodesqlv8",
// };

// // === Variable definitions ===
// const VARIABLES = [
//   { name: "Coreod_mm", final: "Corecheck_Result", sample_time_col: "OD_Turn_Date" },
//   { name: "Corerunout_mm", final: "Corecheck_Result", sample_time_col: "OD_Turn_Date" },
//   { name: "Comod_mm", final: "Comcheck_Result", sample_time_col: "OD_Turn_Date" },
//   { name: "Comrunout_mm", final: "Comcheck_Result", sample_time_col: "OD_Turn_Date" },
// ];

// // === Logger ===
// const logger = winston.createLogger({
//   level: "info",
//   format: winston.format.combine(
//     winston.format.timestamp(),
//     winston.format.printf(
//       ({ timestamp, level, message }) => `[${timestamp}] ${level.toUpperCase()}: ${message}`
//     )
//   ),
//   transports: [new winston.transports.Console()],
// });

// // === MongoDB Schema ===
// const sentRecordSchema = new mongoose.Schema(
//   {
//     variable: { type: String, required: true },
//     data: { type: Number, required: true },
//     sample_time: { type: Date, required: true },
//     Pallet_No: { type: String, required: true },
//     final_indicator: { type: String, required: true },
//     equipment: { type: String, required: true },
//     part: { type: String, required: true },
//     sentToApi: { type: Boolean, default: false },
//   },
//   { timestamps: true }
// );

// // Unique index to prevent duplicates
// sentRecordSchema.index({ Pallet_No: 1, variable: 1, sample_time: 1 }, { unique: true });
// const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// // === Mongo Connection ===
// async function connectMongo() {
//   if (mongoose.connection.readyState === 0) {
//     await mongoose.connect("mongodb://10.69.218.16:27017/armature_line");
//     logger.info("✅ Connected to MongoDB.");
//   }
// }

// // === Listen for Service Broker Messages and Push to Mongo ===
// async function listenServiceBroker() {
//   let pool;
//   try {
//     await connectMongo();
//     pool = await sql.connect(sqlConfig);
//     logger.info("🎯 Connected to SQL Server (listening for queue messages)");

//     while (true) {
//       // ✅ Wait for a message in the queue
//       const receiveResult = await pool.request().query(`
//         WAITFOR (
//           RECEIVE TOP(1)
//             conversation_handle,
//             CAST(message_body AS NVARCHAR(MAX)) AS message_body
//           FROM RecordChangeQueue
//         ), TIMEOUT 5000;
//       `);

//       if (!receiveResult.recordset.length) {
//         logger.info("⌛ No new messages in queue (timeout).");
//         continue; // go back to waiting
//       }

//       logger.info("📥 Message received from Service Broker");

//       // After receiving, fetch latest rows from your source table
//       const columns = [
//         "Part_Name",
//         "Pallet_No",
//         ...VARIABLES.flatMap(v => [
//           `${v.name}`,
//           `${v.final} AS ${v.final}_${v.name}`,
//           `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`,
//         ]),
//       ].join(", ");

//       const result = await pool
//         .request()
//         .query(`SELECT TOP 10 ${columns} FROM linedb ORDER BY OD_Turn_Date DESC`);

//       const rows = result.recordset;
//       if (!rows.length) {
//         logger.info("No new records found in linedb.");
//         continue;
//       }

//       for (const row of rows) {
//         for (const v of VARIABLES) {
//           const dataValue = row[v.name];
//           const finalValue = row[`${v.final}_${v.name}`];
//           const sampleTime = row[`${v.sample_time_col}_${v.name}`]
//             ? new Date(row[`${v.sample_time_col}_${v.name}`])
//             : new Date();

//           if (!dataValue || !finalValue) continue;

//           // ✅ Prevent duplicates
//           const exists = await SentRecord.exists({
//             Pallet_No: row.Pallet_No,
//             variable: v.name,
//             sample_time: sampleTime,
//           });

//           if (exists) {
//             logger.info(
//               `Skipping duplicate: Pallet_No=${row.Pallet_No}, variable=${v.name}, sample_time=${sampleTime}`
//             );
//             continue;
//           }

//           await SentRecord.create({
//             variable: v.name,
//             data: dataValue,
//             sample_time: sampleTime,
//             Pallet_No: row.Pallet_No,
//             final_indicator: String(finalValue).toUpperCase(),
//             equipment: "Ld_armature Line",
//             part: row.Part_Name,
//             sentToApi: false,
//           });

//           logger.info(
//             `✅ Inserted into MongoDB: Pallet_No=${row.Pallet_No}, variable=${v.name}, sample_time=${sampleTime}`
//           );
//         }
//       }
//     }
//   } catch (err) {
//     logger.error(`❌ Sync Error: ${err.stack || err.message}`);
//   } finally {
//     if (pool && pool.connected) {
//       await pool.close();
//       logger.info("🔌 SQL connection closed.");
//     }
//   }
// }

// // === Start listening ===
// listenServiceBroker();





// // Push data from SQL to MongoDB
const sql = require("mssql/msnodesqlv8");
const mongoose = require("mongoose");
const winston = require("winston");

// === Config ===
const sqlConfig = {
  connectionString:
    "Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-MEFC1P0G;Database=demodb1;Trusted_Connection=Yes;TrustServerCertificate=Yes;",
  driver: "msnodesqlv8",
};

// === Variable definitions ===
const VARIABLES = [
  { name: "Coreod_mm", final: "Corecheck_Result", sample_time_col: "OD_Turn_Date" },
  { name: "Corerunout_mm", final: "Corecheck_Result", sample_time_col: "OD_Turn_Date" },
  { name: "Comod_mm", final: "Comcheck_Result", sample_time_col: "OD_Turn_Date" },
  { name: "Comrunout_mm", final: "Comcheck_Result", sample_time_col: "OD_Turn_Date" },
];

// === Logger ===
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) => `[${timestamp}] ${level.toUpperCase()}: ${message}`
    )
  ),
  transports: [new winston.transports.Console()],
});

// === MongoDB Schema ===
const sentRecordSchema = new mongoose.Schema(
  {
    variable: { type: String, required: true },
    data: { type: Number, required: true },
    sample_time: { type: Date, required: true },
    Pallet_No: { type: String, required: true },
    final_indicator: { type: String, required: true },
    equipment: { type: String, required: true },
    part: { type: String, required: true },
    sentToApi: { type: Boolean, default: false },
  },
  { timestamps: true }
);

// Unique index to prevent duplicates
sentRecordSchema.index(
  { Pallet_No: 1, variable: 1, sample_time: 1 },
  { unique: true }
);

const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// === Mongo Connection ===
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect("mongodb://localhost:27017/imr_sync");
    logger.info("✅ Connected to MongoDB.");
  }
}

// === Poll SQL Service Broker Queue and Push to Mongo ===
async function pollServiceBrokerQueue() {
  let pool;
  try {
    await connectMongo();
    pool = await sql.connect(sqlConfig);
    logger.info("🎯 Connected to SQL Server");

    // Build SQL query with unique aliases
    const columns = [
      "Part_Name",
      "Pallet_No",
      ...VARIABLES.flatMap(v => [
        `${v.name}`,
        `${v.final} AS ${v.final}_${v.name}`,
        `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`
      ])
    ].join(", ");

    // ✅ Filter from today's midnight onwards & limit TOP 1000
    const query = `
      SELECT TOP 500 ${columns}
      FROM linedb
      WHERE CAST(OD_Turn_Date AS DATE) = CAST(GETDATE() AS DATE)
      ORDER BY OD_Turn_Date DESC;
    `;

    const result = await pool.request().query(query);
    const rows = result.recordset;

    if (!rows.length) {
      logger.info("No new records found for today.");
      return;
    }

    for (const row of rows) {
      for (const v of VARIABLES) {
        const dataValue = row[v.name];
        const finalValue = row[`${v.final}_${v.name}`];
        const sampleTime = row[`${v.sample_time_col}_${v.name}`]
          ? new Date(row[`${v.sample_time_col}_${v.name}`])
          : new Date();
        console.log(finalValue,'finalval')
        if (!dataValue || !finalValue) continue;

        // ✅ Check MongoDB first to avoid duplicates
        const exists = await SentRecord.exists({
          Pallet_No: row.Pallet_No,
          variable: v.name,
          sample_time: sampleTime,
        });

        if (exists) {
          logger.info(
            `Skipping already inserted: Pallet_No=${row.Pallet_No}, variable=${v.name}, sample_time=${sampleTime}`
          );
          continue;
        }

        // Insert new record
        await SentRecord.create({
          variable: v.name,
          data: dataValue,
          sample_time: sampleTime,
          Pallet_No: row.Pallet_No,
          final_indicator: String(finalValue).toUpperCase(),
          equipment: "Ld_armature Line",
          part: row.Part_Name,
          sentToApi: false,
        });

        logger.info(
          `✅ Inserted into MongoDB: Pallet_No=${row.Pallet_No}, variable=${v.name}, sample_time=${sampleTime}`
        );
      }
    }
  } catch (err) {
    logger.error(`❌ Sync Error: ${err.stack || err.message}`);
  } finally {
    if (pool && pool.connected) {
      await pool.close();
      logger.info("🔌 SQL connection closed.");
    }
  }
}

// === Run every 1 second ===
setInterval(pollServiceBrokerQueue, 10000);

