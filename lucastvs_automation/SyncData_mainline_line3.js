// Push data from MySQL to MongoDB
const mysql = require("mysql2/promise");
const mongoose = require("mongoose");
const winston = require("winston");

// === Config ===
const mysqlConfig = {
  host: "localhost",       // your MySQL host
  user: "root",            // your MySQL user
  password: "prasanjit",    // your MySQL password
  database: "demo2",        // your MySQL database
};

// === Variable definitions ===
const VARIABLES = [
  { name: "lr_pcurrent", final: "lr_presult", sample_time_col: "lr_date" },
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

sentRecordSchema.index(
  { pallet_no: 1, variable: 1, sample_time: 1 },
  { unique: true }
);



const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// === Mongo Connection ===
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect("mongodb://10.69.218.16:27017/main_line");
    logger.info("✅ Connected to MongoDB.");
  }
}

// === Poll MySQL and Push to Mongo ===
async function pollMySQL() {
  let connection;
  try {
    await connectMongo();
    // 🧹 Step 1: Clean up Mongo before inserting
    // const deleteResult = await SentRecord.deleteMany({ pallet_no: null });
    // if (deleteResult.deletedCount > 0) {
    //   logger.warn(`🧹 Deleted ${deleteResult.deletedCount} records with null pallet_no`);
    // }
    connection = await mysql.createConnection(mysqlConfig);
    logger.info("🎯 Connected to MySQL");

    // Build SQL query with aliases
    const columns = [
      "partno",
      "pallet_no",
      ...VARIABLES.flatMap((v) => [
        `${v.name}`,
        `${v.final} AS ${v.final}_${v.name}`,
        `${v.sample_time_col} AS ${v.sample_time_col}_${v.name}`,
      ]),
    ].join(", ");

    const query = `
      SELECT ${columns}
      FROM ptrdb
      WHERE DATE(pr_date) = CURDATE()
      ORDER BY pr_date DESC
      LIMIT 500;
    `;

    const [rows] = await connection.execute(query);

    if (!rows.length) {
      logger.info("⌛ No new records found.");
      return;
    }
    

    for (const row of rows) {
      for (const v of VARIABLES) {
        const dataValue = row[v.name];
        const finalValue = row[`${v.final}_${v.name}`];
        const sampleTime = row[`${v.sample_time_col}_${v.name}`]
          ? new Date(row[`${v.sample_time_col}_${v.name}`])
          : new Date();
        console.log(finalValue,`${v.final}_${v.name}`,'llkkk')

        if (!dataValue || !finalValue) continue;

        if (!row.pallet_no) {
        logger.warn(
            `Skipping row with null pallet_no for variable=${v.name}, sample_time=${sampleTime}`
        );
        continue;
        }

        // Check MongoDB first to avoid duplicates
        const exists = await SentRecord.exists({
          pallet_no: row.pallet_no,
          variable: v.name,
          sample_time: sampleTime,
        });

        if (exists) {
          logger.info(
            `Skipping already inserted: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`
          );
          continue;
        }

        // Insert new record
        // await SentRecord.create({
        //   variable: v.name,
        //   data: dataValue,
        //   sample_time: sampleTime,
        //   pallet_no: row.pallet_no,
        //   final_indicator: String(finalValue).toUpperCase(),
        //   equipment: "Main assy Line 3",
        //   part: row.partno,
        //   sentToApi: false,
        // });
        const palletNo = row.pallet_no || "UNKNOWN";
        await SentRecord.updateOne(
        {
            pallet_no: palletNo,
            variable: v.name,
            sample_time: sampleTime,
        },
        {
            $setOnInsert: {
            data: dataValue,
            final_indicator: String(finalValue).toUpperCase(),
            equipment: "Main assy Line 3",
            part: row.partno,
            sentToApi: false,
            },
        },
        { upsert: true }  // ✅ Insert if not exists, ignore if exists
        );


        logger.info(
          `✅ Inserted into MongoDB: pallet_no=${row.pallet_no}, variable=${v.name}, sample_time=${sampleTime}`
        );
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

// === Run every 30 minutes (1800000 ms) ===
setInterval(pollMySQL, 2000);




