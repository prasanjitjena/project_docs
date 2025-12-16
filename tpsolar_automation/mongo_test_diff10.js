const { MongoClient } = require("mongodb");
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);
const axios = require('axios');
const path = require('path');  // ← ADD THIS

const postURL = 'https://intelliqs.zometric.com/ax/tpsolar/api/xbarpost/';
const token = "55b4bffb2fd3d55ba632f4e8c974ff9440ff989f";

// === IP → DiffusionX (same as tpsolar.js) ===
const IP_TO_DIFFUSION = {
  '10.71.138.57': 'Diffusion10',
};


// function getDbNameFromPath(filePath) {
//   // UNC example: \\10.71.139.158\d\Results\file.txt
//   const match = filePath.match(/^\\\\([^\\]+)\\d\\Results/);
//   if (!match) return null;
//   const ip = match[1];
//   return IP_TO_DIFFUSION[ip] || null;
// }

function getDbNameFromPath(filePath) {
  // Normalize path (to handle / and \ correctly)
  const normalizedPath = path.normalize(filePath).replace(/\//g, '\\');

  // 2️⃣ UNC without drive (Diffusion10 style): \\10.71.x.x\Results
  match = normalizedPath.match(/^\\\\([^\\]+)\\Results/);
  if (match) {
    const ip = match[1];
    return IP_TO_DIFFUSION[ip] || null;
  }

  // If none matched
  return null;
}


const postData = async (filePath) => {
  try {
    await client.connect();
    const dbName = getDbNameFromPath(filePath);  // ← ONLY CHANGE
    if (!dbName) {
      console.log("Unknown IP in postData:", filePath);
      return;
    }
    console.log(dbName, 'dbname');
    const db = client.db(dbName);
    console.log(`Posting data from: ${dbName}`);

    const collection = db.collection("values");
    const recentTime = db.collection("PostTimeStamp");

    let lastetTime = await recentTime.findOne({});
    if (!lastetTime) {
      const defaultTime = new Date("2020-03-20T13:02:46.000Z");
      const insertResult = await recentTime.insertOne({ Time: defaultTime });
      lastetTime = { _id: insertResult.insertedId, Time: defaultTime };
    }

    const data = await collection.find(
      { Time: { $gt: lastetTime.Time } },
      { sort: { Time: 1 } }
    ).toArray();

    if (!data.length) {
      console.log(`${dbName}: No new data to post.`);
      return;
    }

    const payload = data.map(doc => {
      // console.log(doc.Tube,'check')
      const time = new Date(doc.Time);
      const pad = (n) => n.toString().padStart(2, '0');
      const time_stamp = `${time.getFullYear()}-${pad(time.getMonth() + 1)}-${pad(time.getDate())}T${pad(time.getHours())}:${pad(time.getMinutes())}:${pad(time.getSeconds())}+05:30`;
      // 🔹 spec logic: 160 for Diffusion10, else 122
      // const specValue = (dbName === "Diffusion10") ? 160 : 122;
      const specValue = 160;
      return {
        time_stamp,
        tag_id_default: dbName,
        individual_data_array: [doc.P1, doc.P2, doc.P3, doc.P4, doc.P5, doc.P6, doc.P7, doc.P8, doc.P9],
        mapchoicekey: [
          {
            spec: specValue,
            choice_key: "Zone",
            value: doc.ZoneID
          },
          {
            spec: specValue,
            choice_key: "Position",
            value: doc.FurnaceID
          },
          {
            spec: specValue,
            choice_key: "Stack",
            value: doc.Tube
          }
        ]
      };
    });

    try {
      const response = await axios.post(postURL, payload, {
        headers: {
          'Authorization': `Token ${token}`,
          'Content-Type': 'application/json'
        }
      });
      console.log(`${dbName}: ${payload.length} Posted successfully`, response.data);
    } catch (error) {
      console.error(`${dbName}: Post error:`, error.response?.data || error.message);
    }

  } catch (err) {
    console.error("MongoDB Error:", err);
  } finally {
    await client.close();
  }
};

module.exports = { postData };