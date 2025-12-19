const mongoose = require("mongoose");
const axios = require("axios");
const moment = require("moment-timezone");
const winston = require("winston");

// === API Config ===
// const API_URL = "http://localhost:8000/ax/demo/api/imrpost/";
// const API_TOKEN = "70c96e34de6b331712b83207e2f17268402bd76f";

// === API Config ===
const API_URL = "https://intelliqs.zometric.com/ax/lucastvs/api/imrpost/";
const API_TOKEN = "7be2c7165c76f60e68b3fd6f6dfc828c3815356b";

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
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "push_to_api.log" }),
  ],
});

// === MongoDB Schema ===
const sentRecordSchema = new mongoose.Schema(
  {
    variable: { type: String, required: true },
    data: { type: Number, required: true },
    sample_time: { type: Date, required: true },
    // pallet_no: { type: String, required: true },  
    sol_sino: { type: String, required: true },  // ✅ changed pallet_no to sol_sino
    final_indicator: { type: String, required: true },
    equipment: { type: String, required: true },
    part: { type: String, required: true },
    sentToApi: { type: Boolean, default: false },
  },
  { timestamps: true }
);

// ✅ Prevent duplicate records for the same measurement
// sentRecordSchema.index(
//   { part: 1, pallet_no: 1, variable: 1, sample_time: 1 }, // ✅ changed
//   { unique: true }
// );

sentRecordSchema.index(
  { part: 1, sol_sino: 1, variable: 1, sample_time: 1 }, // ✅ changed
  { unique: true }
);

const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// === MongoDB Connection ===
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect("mongodb://192.9.206.121:27017/main_line");
    logger.info("✅ Connected to MongoDB.");
  }
}

// === Push Records to API ===
async function pushToCloud() {
  try {
    await connectMongo();

    // fetch up to 10000 pending records
    const pendingRecords = await SentRecord.find({ sentToApi: false }).limit(5000);

    if (!pendingRecords.length) {
      logger.info("⌛ No new records to push.");
      return;
    }

    // ✅ Deduplicate by sol_sino + variable
    const uniqueRecordsMap = new Map();
    pendingRecords.forEach((rec) => {
      const key = `${rec.sol_sino}_${rec.variable}`;
      if (!uniqueRecordsMap.has(key)) {
        uniqueRecordsMap.set(key, rec);
      }
    });

    const uniqueRecords = Array.from(uniqueRecordsMap.values());

    // prepare payload
    const payload = uniqueRecords.map((record) => ({
      equipment: record.equipment,
      part: record.part,
      vc: record.variable, // ✅ API expects "vc"
      data_point: record.data,
      time_stamp: moment(record.sample_time).format("YYYY-MM-DD HH:mm:ss"),
      serial_no: record.sol_sino, // ✅ API expects serial_no
      remarks: "",
    }));

    try {
      const res = await axios.post(API_URL, payload, {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Token ${API_TOKEN}`,
        },
        timeout: 5000,
      });

      if (res.status === 200 || res.status === 201) {
        const ids = uniqueRecords.map((r) => r._id);
        await SentRecord.updateMany(
          { _id: { $in: ids } },
          { $set: { sentToApi: true, updatedAt: new Date() } }
        );
        logger.info(
          `✅ Successfully pushed ${uniqueRecords.length} unique (sol_sino + vc) records to API.`
        );
      } else {
        logger.warn(
          `⚠️ API responded with status ${res.status}: ${JSON.stringify(res.data)}`
        );
      }
    } catch (err) {
        logger.error("❌ API push failed:");
        logger.error(`Message: ${err.message}`);
        logger.error(`Code: ${err.code}`);
        logger.error(`Stack: ${err.stack}`);
        if (err.response) {
          logger.error(`Response: ${JSON.stringify(err.response.data)}`);
        }
        if (err.request) {
          logger.error(`Request: ${err.request._header}`);
        }
    }
  } catch (err) {
    logger.error(`❌ Push Error: ${err.stack || err.message}`);
  }
}

// === Cleanup Old Records ===
async function cleanupOldRecords() {
  try {
    await connectMongo();

    const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hrs ago

    const oldRecords = await SentRecord.find({ createdAt: { $lt: cutoff } })
      .limit(10000)
      .select("_id");

    if (oldRecords.length === 0) {
      logger.info("⌛ No old records to delete.");
      return;
    }

    const ids = oldRecords.map((r) => r._id);

    await SentRecord.deleteMany({ _id: { $in: ids } });

    logger.info(`🗑️ Deleted ${ids.length} old records (>24 hrs).`);
  } catch (err) {
    logger.error(`❌ Cleanup Error: ${err.stack || err.message}`);
  }
}


// === Run every 7 seconds ===
setInterval(pushToCloud, 7000);

// // === Run cleanup every 24 hrs (86400000 ms) ===
// setInterval(cleanupOldRecords, 7000);
