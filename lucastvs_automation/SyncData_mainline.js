// // Push data from MySQL to MongoDB
// const mysql = require("mysql2/promise");
// const mongoose = require("mongoose");
// const winston = require("winston");

// // === Config ===
// const mysqlConfig = {
//   host: "localhost",       // your MySQL host
//   user: "root",            // your MySQL user
//   password: "prasanjit",    // your MySQL password
//   database: "demo2",        // your MySQL database
// };

// // === Variable definitions ===
// const VARIABLES = [
//   { name: "ptr_rtcurrent", final: "ptr_rtresult", sample_time_col: "pr_date" },
//   { name: "ptr_rtspeed", final: "ptr_rtresult", sample_time_col: "pr_date" },
//   { name: "ptr_ltcurrent", final: "ptr_ltresult", sample_time_col: "pr_date" },
//   { name: "ptr_ltspeed", final: "ptr_ltresult", sample_time_col: "pr_date" },
// ];

// // === Logger ===
// const logger = winston.createLogger({
//   level: "info",
//   format: winston.format.combine(
//     winston.format.timestamp(),
//     winston.format.printf(
//       ({ timestamp, level, message }) =>
//         `[${timestamp}] ${level.toUpperCase()}: ${message}`
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
//     pallet_no: { type: String, required: true },
//     final_indicator: { type: String, required: true },
//     equipment: { type: String, required: true },
//     part: { type: String, required: true },
//     sentToApi: { type: Boolean, default: false },
//   },
//   { timestamps: true }
// );

// sentRecordSchema.index(
//   { pallet_no: 1, variable: 1, sample_time: 1 },
//   { unique: true }
// );



// const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// // === Mongo Connection ===
// async function connectMongo() {
//   if (mongoose.connection.readyState === 0) {
//     await mongoose.connect("mongodb://10.69.218.16:27017/main_line");
//     logger.info("✅ Connected to MongoDB.");
//   }
// }

// // === Poll MySQL and Push to Mongo ===
// async function pollMySQL() {
//   let connection;
//   try {
//     await connectMongo();
//     // 🧹 Step 1: Clean up Mongo before inserting
//     // const deleteResult = await SentRecord.deleteMany({ pallet_no: null });
//     // if (deleteResult.deletedCount > 0) {
//     //   logger.warn(`🧹 Deleted ${deleteResult.deletedCount} records with null pallet_no`);
//     // }
//     connection = await mysql.createConnection(mysqlConfig);
//     logger.info("🎯 Connected to MySQL");

//     // Build SQL query with aliases
//     const columns = [
//       "partno",
//       "pallet_no",
//       ...VARIABLES.flatMap((v) => [
//         `${v.name}`,
//         `${v.final} AS ${v.final}_${v.name}`,
//         `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`,
//       ]),
//     ].join(", ");

//     const query = `
//       SELECT ${columns}
//       FROM ptrdb
//       WHERE DATE(pr_date) = CURDATE()
//       ORDER BY pr_date DESC
//       LIMIT 500;
//     `;

//     const [rows] = await connection.execute(query);

//     if (!rows.length) {
//       logger.info("⌛ No new records found.");
//       return;
//     }
    

//     for (const row of rows) {
//       for (const v of VARIABLES) {
//         const dataValue = row[v.name];
//         const finalValue = row[`${v.final}_${v.name}`];
//         const sampleTime = row[`${v.sample_time_col}_${v.name}`]
//           ? new Date(row[`${v.sample_time_col}_${v.name}`])
//           : new Date();
//         console.log(finalValue,`${v.final}_${v.name}`,'llkkk')

//         if (!dataValue || !finalValue) continue;

//         if (!row.pallet_no) {
//         logger.warn(
//             `Skipping row with null pallet_no for variable=${v.name}, sample_time=${sampleTime}`
//         );
//         continue;
//         }

//         // Check MongoDB first to avoid duplicates
//         const exists = await SentRecord.exists({
//           pallet_no: row.pallet_no,
//           variable: v.name,
//           sample_time: sampleTime,
//         });

//         if (exists) {
//           logger.info(
//             `Skipping already inserted: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`
//           );
//           continue;
//         }

//         // Insert new record
//         // await SentRecord.create({
//         //   variable: v.name,
//         //   data: dataValue,
//         //   sample_time: sampleTime,
//         //   pallet_no: row.pallet_no,
//         //   final_indicator: String(finalValue).toUpperCase(),
//         //   equipment: "Main assy Line 3",
//         //   part: row.partno,
//         //   sentToApi: false,
//         // });
//         const palletNo = row.pallet_no || "UNKNOWN";
//         await SentRecord.updateOne(
//         {
//             pallet_no: palletNo,
//             variable: v.name,
//             sample_time: sampleTime,
//         },
//         {
//             $setOnInsert: {
//             data: dataValue,
//             final_indicator: String(finalValue).toUpperCase(),
//             equipment: "Main assy Line 3",
//             part: row.partno,
//             sentToApi: false,
//             },
//         },
//         { upsert: true }  // ✅ Insert if not exists, ignore if exists
//         );


//         logger.info(
//           `✅ Inserted into MongoDB: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`
//         );
//       }
//     }
//   } catch (err) {
//     logger.error(`❌ Sync Error: ${err.stack || err.message}`);
//   } finally {
//     if (connection) {
//       await connection.end();
//       logger.info("🔌 MySQL connection closed.");
//     }
//   }
// }

// // === Run every 30 minutes (1800000 ms) ===
// setInterval(pollMySQL, 2000);




// // Push data from MySQL to MongoDB
// const mysql = require("mysql2/promise");
// const mongoose = require("mongoose");
// const winston = require("winston");

// // === Config ===
// const mysqlConfig = {
//   host: "localhost",     
//   user: "root",          
//   password: "prasanjit", 
//   database: "demo2",     
// };

// // === Variable definitions ===
// const VARIABLES_ALL = [
//   // ptrdb variables
//   { name: "ptr_rtcurrent", final: "ptr_rtresult", sample_time_col: "pr_date", table: "ptrdb" },
//   { name: "ptr_rtspeed", final: "ptr_rtresult", sample_time_col: "pr_date", table: "ptrdb" },
//   { name: "ptr_ltcurrent", final: "ptr_ltresult", sample_time_col: "pr_date", table: "ptrdb" },
//   { name: "ptr_ltspeed", final: "ptr_ltresult", sample_time_col: "pr_date", table: "ptrdb" },
//   // line3db variable
//   { name: "lr_pcurrent", final: "lr_presult", sample_time_col: "lr_date", table: "line3db" },
// ];

// // === Logger ===
// const logger = winston.createLogger({
//   level: "info",
//   format: winston.format.combine(
//     winston.format.timestamp(),
//     winston.format.printf(
//       ({ timestamp, level, message }) =>
//         `[${timestamp}] ${level.toUpperCase()}: ${message}`
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
//     pallet_no: { type: String, required: true },
//     final_indicator: { type: String, required: true },
//     equipment: { type: String, required: true },
//     part: { type: String, required: true },
//     sentToApi: { type: Boolean, default: false },
//   },
//   { timestamps: true }
// );

// sentRecordSchema.index(
//   { pallet_no: 1, variable: 1, sample_time: 1 },
//   { unique: true }
// );

// const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// // === Mongo Connection ===
// async function connectMongo() {
//   if (mongoose.connection.readyState === 0) {
//     await mongoose.connect("mongodb://10.69.218.16:27017/main_line");
//     logger.info("✅ Connected to MongoDB.");
//   }
// }

// // === Get last synced time per table ===
// async function getLastSyncedTime(table) {
//   const varsInTable = VARIABLES_ALL.filter(v => v.table === table).map(v => v.name);
//   const lastRecord = await SentRecord
//     .find({ variable: { $in: varsInTable } })
//     .sort({ sample_time: -1 })
//     .limit(1)
//     .lean();
//     console.log(lastRecord,'lastRecord')
//   return lastRecord.length ? lastRecord[0].sample_time : null;
// }

// // === Poll MySQL and push to MongoDB ===
// async function pollAllVariables() {
//   let connection;

//   try {
//     await connectMongo();
//     connection = await mysql.createConnection(mysqlConfig);
//     logger.info("🎯 Connected to MySQL");

//     // Group variables by table
//     const tableGroups = VARIABLES_ALL.reduce((acc, v) => {
//       if (!acc[v.table]) acc[v.table] = [];
//       acc[v.table].push(v);
//       return acc;
//     }, {});

//     for (const [table, vars] of Object.entries(tableGroups)) {
//       const columns = ["partno", "pallet_no",
//         ...vars.flatMap(v => [
//           `${v.name}`,
//           `${v.final} AS ${v.final}_${v.name}`,
//           `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`
//         ])
//       ].join(", ");

//       const dateCol = vars[0].sample_time_col;
//       const lastTime = await getLastSyncedTime(table);
//       console.log(lastTime,'lastTime')
//       const whereClause = lastTime
//         ? `${dateCol} > '${lastTime.toISOString().slice(0, 19).replace('T', ' ')}'`
//         : `DATE(${dateCol}) = CURDATE()`;

//       const query = `
//         SELECT ${columns}
//         FROM ${table}
//         WHERE ${whereClause}
//         ORDER BY ${dateCol} ASC
//         LIMIT 500;
//       `;

//       const [rows] = await connection.execute(query);

//       if (!rows.length) {
//         logger.info(`⌛ No new records found in ${table}.`);
//         continue;
//       }

//       for (const row of rows) {
//         for (const v of vars) {
//           const dataValue = row[v.name];
//           const finalValue = row[`${v.final}_${v.name}`];
//           const sampleTime = row[`${v.sample_time_col}_${v.name}`]
//           // console.log(sampleTime,'timee')
//             ? new Date(row[`${v.sample_time_col}_${v.name}`])
//             : new Date();

//           if (!dataValue || !finalValue) continue;
//           if (!row.pallet_no) {
//             logger.warn(`Skipping row with null pallet_no for variable=${v.name}, sample_time=${sampleTime}`);
//             continue;
//           }

//           const exists = await SentRecord.exists({
//             pallet_no: row.pallet_no,
//             variable: v.name,
//             sample_time: sampleTime,
//           });

//           if (exists) {
//             logger.info(
//               `⚠ Skipping already inserted: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`
//             );
//             continue;
//           }

//            // Insert new record
//           await SentRecord.create({
//             variable: v.name,
//             data: dataValue,
//             sample_time: sampleTime,
//             pallet_no: row.pallet_no,
//             final_indicator: String(finalValue).toUpperCase(),
//             equipment: "Main assy Line 3",
//             part: row.partno,
//             sentToApi: false,
//           });

//           // await SentRecord.updateOne(
//           //   {
//           //     pallet_no: row.pallet_no,
//           //     variable: v.name,
//           //     sample_time: sampleTime
//           //   },
//           //   {
//           //     $setOnInsert: {
//           //       data: dataValue,
//           //       final_indicator: String(finalValue).toUpperCase(),
//           //       equipment: "Main assy Line 3",
//           //       part: row.partno,
//           //       sentToApi: false,
//           //     }
//           //   },
//           //   { upsert: true }
//           // );

//           logger.info(`✅ Inserted: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`);
//         }
//       }
//     }

//   } catch (err) {
//     logger.error(`❌ Sync Error: ${err.stack || err.message}`);
//   } finally {
//     if (connection) {
//       await connection.end();
//       logger.info("🔌 MySQL connection closed.");
//     }
//   }
// }

// // === Run periodically every 30 minutes ===
// setInterval(pollAllVariables, 6000);



// Push data from MySQL to MongoDB
const mysql = require("mysql2/promise");
const mongoose = require("mongoose");
const winston = require("winston");

// === Config ===
const mysqlConfig = {
  host: "localhost",
  user: "root",
  password: "prasanjit",
  database: "demo2",
};

const mongoURI = "mongodb://10.69.218.16:27017/main_line";

// === Variable definitions ===
const VARIABLES_ALL = [
  // ptrdb variables
  { name: "ptr_rtcurrent", final: "ptr_rtresult", sample_time_col: "pr_date", table: "ptrdb" },
  { name: "ptr_rtspeed", final: "ptr_rtresult", sample_time_col: "pr_date", table: "ptrdb" },
  { name: "ptr_ltcurrent", final: "ptr_ltresult", sample_time_col: "pr_date", table: "ptrdb" },
  { name: "ptr_ltspeed", final: "ptr_ltresult", sample_time_col: "pr_date", table: "ptrdb" },
  // line3db variable
  { name: "lr_pcurrent", final: "lr_presult", sample_time_col: "lr_date", table: "line3db" },
];

// === Logger ===
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) =>
        `[${timestamp}] ${level.toUpperCase()}: ${message}`
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
    pallet_no: { type: String, required: true },
    final_indicator: { type: String, required: true },
    equipment: { type: String, required: true },
    part: { type: String, required: true },
    sentToApi: { type: Boolean, default: false },
  },
  { timestamps: true }
);

// Unique index → prevents duplicate inserts
sentRecordSchema.index(
  { pallet_no: 1, variable: 1, sample_time: 1 },
  { unique: true }
);

const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// === Mongo Connection ===
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(mongoURI);
    logger.info("✅ Connected to MongoDB.");
  }
}

// === Utility: format JS Date for MySQL ===
function formatDateForMySQL(date) {
  const pad = (n) => (n < 10 ? "0" + n : n);
  return (
    date.getFullYear() +
    "-" + pad(date.getMonth() + 1) +
    "-" + pad(date.getDate()) +
    " " + pad(date.getHours()) +
    ":" + pad(date.getMinutes()) +
    ":" + pad(date.getSeconds())
  );
}

// === Get last synced time per table ===
async function getLastSyncedTime(table) {
  const varsInTable = VARIABLES_ALL.filter(v => v.table === table).map(v => v.name);
  const lastRecord = await SentRecord
    .find({ variable: { $in: varsInTable } })
    .sort({ sample_time: -1 })
    .limit(1)
    .lean();

  return lastRecord.length ? lastRecord[0].sample_time : null;
}

// === Poll MySQL and push to MongoDB ===
async function pollAllVariables() {
  let connection;
  try {
    await connectMongo();
    connection = await mysql.createConnection(mysqlConfig);
    logger.info("🎯 Connected to MySQL");

    // Group variables by table
    const tableGroups = VARIABLES_ALL.reduce((acc, v) => {
      if (!acc[v.table]) acc[v.table] = [];
      acc[v.table].push(v);
      return acc;
    }, {});

    for (const [table, vars] of Object.entries(tableGroups)) {
      const columns = ["partno", "pallet_no",
        ...vars.flatMap(v => [
          `${v.name}`,
          `${v.final} AS ${v.final}_${v.name}`,
          `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`
        ])
      ].join(", ");

      const dateCol = vars[0].sample_time_col;
      const lastTime = await getLastSyncedTime(table);
      console.log(lastTime,'time')
      const whereClause = lastTime
        ? `${dateCol} = '${formatDateForMySQL(new Date(lastTime))}'`
        : `${dateCol} >= '2025-09-03 00:00:00'`; // first sync start date

      const query = `
        SELECT ${columns}
        FROM ${table}
        WHERE ${whereClause}
        ORDER BY ${dateCol} ASC
        LIMIT 5000;
      `;

      const [rows] = await connection.execute(query);

      if (!rows.length) {
        logger.info(`⌛ No new records found in ${table}.`);
        continue;
      }

      // Prepare bulk docs
      const docs = [];
      for (const row of rows) {
        for (const v of vars) {
          const dataValue = row[v.name];
          const finalValue = row[`${v.final}_${v.name}`];
          const sampleTime = row[`${v.sample_time_col}_${v.name}`]
            ? new Date(row[`${v.sample_time_col}_${v.name}`])
            : new Date();

          if (dataValue == null || !finalValue || !row.pallet_no) continue;

          docs.push({
            variable: v.name,
            data: dataValue,
            sample_time: sampleTime,
            pallet_no: row.pallet_no,
            final_indicator: String(finalValue).toUpperCase(),
            equipment: "Main assy Line 3",
            part: row.partno,
            sentToApi: false,
          });
        }
      }

      if (docs.length > 0) {
        try {
          await SentRecord.insertMany(docs, { ordered: false });
          logger.info(`✅ Inserted ${docs.length} docs from ${table}`);
        } catch (err) {
          if (err.code === 11000) {
            logger.warn(`⚠ Duplicate key errors ignored for ${table}`);
          } else {
            throw err;
          }
        }
      }
    }

  } catch (err) {
    logger.error(`❌ Sync Error: ${err.stack || err.message}`);
  } finally {
    if (connection) {
      await connection.end();
      logger.info("🔌 MySQL connection closed.");
    }
  }
}

// === Run periodically every 30 seconds ===
setInterval(pollAllVariables, 3000);
