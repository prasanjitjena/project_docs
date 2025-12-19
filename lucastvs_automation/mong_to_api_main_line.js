// const mongoose = require("mongoose");
// const axios = require("axios");
// const moment = require("moment-timezone");
// const winston = require("winston");
// const https = require("https");


// // === API Config ===
// const API_URL = "http://localhost:8000/ax/demo/api/imrpost/";
// const API_TOKEN = "70c96e34de6b331712b83207e2f17268402bd76f";

// // === API Config ===
// // const API_URL = "https://intelliqs.zometric.com/ax/lucastvs/api/imrpost/";
// // const API_TOKEN = "7be2c7165c76f60e68b3fd6f6dfc828c3815356b";

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
//   transports: [
//     new winston.transports.Console(),
//     new winston.transports.File({ filename: "push_to_api.log" }),
//   ],
// });

// // === MongoDB Schema ===
// const sentRecordSchema = new mongoose.Schema(
//   {
//     variable: { type: String, required: true },
//     data: { type: Number, required: true },
//     sample_time: { type: Date, required: true },
//     // pallet_no: { type: String, required: true },  
//     pallet_no: { type: String, required: true },  // ✅ changed pallet_no to pallet_no
//     final_indicator: { type: String, required: true },
//     equipment: { type: String, required: true },
//     part: { type: String, required: true },
//     sentToApi: { type: Boolean, default: false },
//   },
//   { timestamps: true }
// );

// // ✅ Prevent duplicate records for the same measurement
// // sentRecordSchema.index(
// //   { part: 1, pallet_no: 1, variable: 1, sample_time: 1 }, // ✅ changed
// //   { unique: true }
// // );

// sentRecordSchema.index(
//   { part: 1, pallet_no: 1, variable: 1, sample_time: 1 }, // ✅ changed
//   { unique: true }
// );

// const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// // === MongoDB Connection ===
// async function connectMongo() {
//   if (mongoose.connection.readyState === 0) {
//     await mongoose.connect("mongodb://10.69.218.16:27017/main_line");
//     logger.info("✅ Connected to MongoDB.");
//   }
// }

// // === Push Records to API ===
// async function pushToCloud() {
//   try {
//     await connectMongo();

//     // fetch up to 10000 pending records
//     const pendingRecords = await SentRecord.find({ sentToApi: false }).limit(10000);

//     if (!pendingRecords.length) {
//       logger.info("⌛ No new records to push.");
//       return;
//     }

//     // ✅ Deduplicate by sol_sino + variable
//     const uniqueRecordsMap = new Map();
//     pendingRecords.forEach((rec) => {
//       const key = `${rec.pallet_no}_${rec.variable}`;
//       if (!uniqueRecordsMap.has(key)) {
//         uniqueRecordsMap.set(key, rec);
//       }
//     });

//     const uniqueRecords = Array.from(uniqueRecordsMap.values());

//     // prepare payload
//     const payload = uniqueRecords.map((record) => ({
//       equipment: record.equipment,
//       part: record.part,
//       vc: record.variable, // ✅ API expects "vc"
//       data_point: record.data,
//       time_stamp: moment(record.sample_time).format("YYYY-MM-DD HH:mm:ss"),
//       serial_no: record.pallet_no, // ✅ API expects serial_no
//       remarks: "",
//     }));

//     try {
//       const res = await axios.post(API_URL, payload, {
//         headers: {
//           "Content-Type": "application/json",
//           Authorization: `Token ${API_TOKEN}`,
//         },
//         timeout: 5000,
//       });

//       if (res.status === 200 || res.status === 201) {
//         const ids = uniqueRecords.map((r) => r._id);
//         await SentRecord.updateMany(
//           { _id: { $in: ids } },
//           { $set: { sentToApi: true, updatedAt: new Date() } }
//         );
//         logger.info(
//           `✅ Successfully pushed ${uniqueRecords.length} unique (pallet_no + vc) records to API.`
//         );
//       } else {
//         logger.warn(
//           `⚠️ API responded with status ${res.status}: ${JSON.stringify(res.data)}`
//         );
//       }
//     } catch (err) {
//       logger.error(`❌ API push failed: ${err.response?.data || err.message}`);
//     }
//   } catch (err) {
//     logger.error(`❌ Push Error: ${err.stack || err.message}`);
//   }
// }



// // === Cleanup Old Records ===
// async function cleanupOldRecords() {
//   try {
//     await connectMongo();

//     const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hrs ago

//     const oldRecords = await SentRecord.find({ createdAt: { $lt: cutoff } })
//       .limit(10000)
//       .select("_id");

//     if (oldRecords.length === 0) {
//       logger.info("⌛ No old records to delete.");
//       return;
//     }

//     const ids = oldRecords.map((r) => r._id);

//     await SentRecord.deleteMany({ _id: { $in: ids } });

//     logger.info(`🗑️ Deleted ${ids.length} old records (>24 hrs).`);
//   } catch (err) {
//     logger.error(`❌ Cleanup Error: ${err.stack || err.message}`);
//   }
// }


// // === Run every 7 seconds ===
// setInterval(pushToCloud, 7000);

// === Run cleanup every 24 hrs (86400000 ms) ===
// setInterval(cleanupOldRecords, 7000);




// const axios = require("axios");

// // // === API Config ===
// const API_URL = "https://intelliqs.zometric.com/ax/lucastvs/api/imrpost/";
// const API_TOKEN = "7be2c7165c76f60e68b3fd6f6dfc828c3815356b";

// // === Test Payload (example) ===
// const testPayload = [{
//     "equipment": "Main assy Line 3",
//     "part": "YED MSIL",
//     "vc": "ptr_rtspeed",
//     "data_point": 1580,
//     "time_stamp": "2025-09-04 06:48:58",
//     "serial_no": "Lucas TVS :26264688:260825:2:2162 N",
//     "remarks": ""
//   }
// ]

// // === Send Request ===
// axios.post(API_URL, testPayload, {
//   headers: {
//     Authorization: `Token ${API_TOKEN}`,
//     "Content-Type": "application/json"
//   }
// })
// .then(res => {
//   console.log("✅ Success:", res.data);
// })
// .catch(err => {
//   if (err.response) {
//     console.error("❌ Error:", err.response.status, err.response.data);
//   } else {
//     console.error("❌ Error:", err.message);
//   }
// });



const mongoose = require("mongoose");
const axios = require("axios");
const moment = require("moment-timezone");
const winston = require("winston");

// === API Config ===
const API_URL = "http://localhost:8000/ax/demo/api/imrpost/";
const API_TOKEN = "70c96e34de6b331712b83207e2f17268402bd76f";

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
    pallet_no: { type: String, required: true },
    final_indicator: { type: String, required: true },
    equipment: { type: String, required: true },
    part: { type: String, required: true },
    sentToApi: { type: Boolean, default: false }, // ✅ only this matters
  },
  { timestamps: true }
);

// ✅ Prevent duplicate records
sentRecordSchema.index(
  { part: 1, pallet_no: 1, variable: 1, sample_time: 1 },
  { unique: true }
);

const SentRecord = mongoose.model("SentRecord", sentRecordSchema);

// === MongoDB Connection ===
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect("mongodb://10.69.218.16:27017/main_line");
    logger.info("✅ Connected to MongoDB.");
  }
}

// === Helper: Split array into chunks ===
function chunkArray(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

// === Push Records to API ===
async function pushToCloud() {
  try {
    await connectMongo();

    // fetch up to 5000 pending records
    const pendingRecords = await SentRecord.find({
      sentToApi: false,
    }).limit(5000);

    if (!pendingRecords.length) {
      logger.info("⌛ No new records to push.");
      return;
    }

    // ✅ Deduplicate by pallet_no + variable + sample_time
    const uniqueRecordsMap = new Map();
    pendingRecords.forEach((rec) => {
      const key = `${rec.pallet_no}_${rec.variable}_${rec.sample_time.getTime()}`;
      if (!uniqueRecordsMap.has(key)) {
        uniqueRecordsMap.set(key, rec);
      }
    });

    const uniqueRecords = Array.from(uniqueRecordsMap.values());

    // ✅ Split into chunks of 1000
    const chunks = chunkArray(uniqueRecords, 1000);

    for (const [index, chunk] of chunks.entries()) {
      const payload = chunk.map((record) => ({
        equipment: record.equipment,
        part: record.part,
        vc: record.variable,
        data_point: record.data,
        time_stamp: moment(record.sample_time).format("YYYY-MM-DD HH:mm:ss"),
        serial_no: record.pallet_no,
        remarks: "",
      }));

      try {
        const res = await axios.post(API_URL, payload, {
          headers: {
            "Content-Type": "application/json",
            Authorization: `Token ${API_TOKEN}`,
          },
          timeout: 15000, // ⏱ extra time for large batches
        });

        if (res.status === 200 || res.status === 201) {
          const ids = chunk.map((r) => r._id);
          await SentRecord.updateMany(
            { _id: { $in: ids } },
            { $set: { sentToApi: true, updatedAt: new Date() } }
          );
          logger.info(
            `✅ Successfully pushed chunk ${index + 1}/${chunks.length} (${chunk.length} records).`
          );
        } else {
          logger.warn(
            `⚠️ API responded with status ${res.status}: ${JSON.stringify(res.data)}`
          );
        }
      } catch (err) {
        logger.error(
          `❌ API push failed for chunk ${index + 1}/${chunks.length}: ${
            err.response?.data || err.message
          }`
        );
        // ⚠️ Leave sentToApi:false → will retry next cycle
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

    const oldRecords = await SentRecord.find({
      createdAt: { $lt: cutoff },
      sentToApi: true,
    })
      .limit(10000)
      .select("_id");

    if (oldRecords.length === 0) {
      logger.info("⌛ No old records to delete.");
      return;
    }

    const ids = oldRecords.map((r) => r._id);

    await SentRecord.deleteMany({ _id: { $in: ids } });

    logger.info(`🗑️ Deleted ${ids.length} old records (>24 hrs, sentToApi:true).`);
  } catch (err) {
    logger.error(`❌ Cleanup Error: ${err.stack || err.message}`);
  }
}

// === Run every 7 seconds ===
setInterval(pushToCloud, 6000);

// === Run cleanup every 24 hrs (86400000 ms) ===
setInterval(cleanupOldRecords, 86400000);
