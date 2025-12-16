const fsp = require('fs').promises;
const { MongoClient } = require('mongodb');
const path = require('path');
const { postData } = require('./mongo_test_latest');

const uri = 'mongodb://localhost:27017';

let client;
let isConnected = false;

async function getClient() {
  if (!client) {
    client = new MongoClient(uri, {
      connectTimeoutMS: 5000,
      serverSelectionTimeoutMS: 5000,
      maxPoolSize: 10,
      useUnifiedTopology: true,
    });
  }
  if (!isConnected) {
    try {
      await client.connect();
      isConnected = true;
      console.log('[OK] MongoDB client connected');
    } catch (e) {
      isConnected = false;
      throw e;
    }
  }
  return client;
}

const IP_TO_DIFFUSION = {
  '10.71.138.62': 'Diffusion1',
  '10.71.138.63': 'Diffusion2',
  '10.71.138.64': 'Diffusion3',
  '10.71.139.173': 'Diffusion4',
  '10.71.139.158': 'Diffusion5',
  '10.71.139.79': 'Diffusion6',
  '10.71.138.160': 'Diffusion7',
  '10.71.134.250': 'Diffusion8',
  '10.71.139.238': 'Diffusion9',
};

const ROOT_FOLDERS = Object.keys(IP_TO_DIFFUSION).map(ip => `\\\\${ip}\\d\\Results`);

// const ROOT_FOLDERS = Object.keys(IP_TO_DIFFUSION).map(ip => {
//   // Diffusion10 (10.71.138.57) has no \d folder
//   if (IP_TO_DIFFUSION[ip] === 'Diffusion10') {
//     return `\\\\${ip}\\Results`;
//   }
//   // Others have \d\Results
//   return `\\\\${ip}\\d\\Results`;
// });


const processedCache = new Map();

function getDbNameFromPath(filePath) {
  const m = filePath.match(/^\\\\([^\\]+)\\d\\Results/);
  if (!m) return null;
  return IP_TO_DIFFUSION[m[1]] || null;
}

// function getDbNameFromPath(filePath) {
//   // Normalize path (to handle / and \ correctly)
//   const normalizedPath = path.normalize(filePath).replace(/\//g, '\\');

//   // 1️⃣ UNC with drive letter: \\10.71.x.x\d\Results
//   let match = normalizedPath.match(/^\\\\([^\\]+)\\d\\Results/);
//   if (match) {
//     const ip = match[1];
//     return IP_TO_DIFFUSION[ip] || null;
//   }

//   // 2️⃣ UNC without drive (Diffusion10 style): \\10.71.x.x\Results
//   match = normalizedPath.match(/^\\\\([^\\]+)\\Results/);
//   if (match) {
//     const ip = match[1];
//     return IP_TO_DIFFUSION[ip] || null;
//   }

//   // If none matched
//   return null;
// }

const csvFilePath = 'C:/zometric_diff/tpsolar_latest_aut/Diffusion/converted.csv';

async function convertToCSV(txtPath) {
  try {
    const data = await fsp.readFile(txtPath, 'utf8');
    const lines = data.trim().split('\n');
    const headers = lines[0].trim().split('\t');
    const timeIdx = headers.indexOf('Time');
    if (timeIdx === -1) {
      console.warn(`[WARN] No "Time" column in ${path.basename(txtPath)}`);
      return false;
    }
    const records = lines.slice(1).map(l => {
      const cols = l.trim().split(/\t+/);
      const rec = {};
      headers.forEach((h, i) => (rec[h] = cols[i]));
      return rec;
    });
    const csvLines = [
      headers.join(','),
      ...records.map(r =>
        headers.map(h => `"${(r[h] ?? '').toString().replace(/"/g, '""')}"`).join(',')
      ),
    ];
    await fsp.writeFile(csvFilePath, csvLines.join('\n'), 'utf8');
    return true;
  } catch (e) {
    console.error('[ERROR] convertToCSV', e);
    return false;
  }
}

async function readCsv() {
  try {
    const data = await fsp.readFile(csvFilePath, 'utf8');
    const lines = data.trim().split('\n');
    const headers = lines[0].trim().split(',');
    const req = ['Time', 'FurnaceID','TubeID', 'ZoneID', 'P1', 'P2', 'P3', 'P4', 'P5'];
    const idx = req.map(c => headers.indexOf(c)).map((i, pos) => (i < 0 ? null : pos));

    const rows = lines.slice(1).map(l => {
      const vals = l.trim().split(',');
      const rec = {};
      idx.forEach((pos, i) => {
        if (pos === null) return;
        let v = vals[headers.indexOf(req[i])];

        if (req[i] === 'FurnaceID') {
          const n = Number(v.replace(/[^\d.]/g, ''));
          v = n === 2 ? 'Left' : n === 1 ? 'Right' : v;
        }
        if (req[i] === 'ZoneID') {
          const n = Number(v.replace(/[^\d.]/g, ''));
          if ([1, 2, 3, 4].includes(n)) v = 'LZ';
          else if ([5, 6, 7, 8].includes(n)) v = 'CZ';
          else if ([9, 10, 11, 12].includes(n)) v = 'GZ';
        }
        
        //Universal mapping for TubeID → S1–S5 (works for Diffusion01–09)
        if (req[i] === 'TubeID') {
          // Match patterns like DIFFX01-05, DIFFX09-03, etc.
          const m = v.match(/DIFFX\d{2}-(\d+)/i);
          if (m) {
            const num = parseInt(m[1]);
            if (num >= 1 && num <= 5) rec['Tube'] = `S${num}`;
          }
        }

        
        if (['P1', 'P2', 'P3', 'P4', 'P5'].includes(req[i])) {
          v = parseFloat(v.replace(/"/g, '')) || 0;
        }
        if (req[i] === 'Time') {
          v = new Date(v.replace(/"/g, ''));
        }
        rec[req[i]] = v;
      });
      return rec;
    });

    // Skip row if any sample value (P1–P5) is 0.1 or 1
    const filtered = rows.filter(r => {
    const hasInvalidSample =
        [r.P1, r.P2, r.P3, r.P4, r.P5].some(v => v === 0.1 || v === 1);
    return !hasInvalidSample;
    });

    return filtered;
  } catch (e) {
    console.error('[ERROR] readCsv', e);
    return [];
  }
}

async function pushData(records, txtPath) {
  const dbName = getDbNameFromPath(txtPath);
  if (!dbName) return;

  if (records.length === 0) {
    console.log(`[SKIP] ${dbName} → all rows are 0.1 (machine down)`);
    return;
  }

  let mongo;
  try {
    mongo = await getClient();
    const db = mongo.db(dbName);
    const coll = db.collection('values');
    const tsColl = db.collection('TimeStamp');
    const postTsColl = db.collection('PostTimeStamp');

    const last = await tsColl.find().sort({ Time: -1 }).limit(1).toArray();
    const latestTs = last.length ? last[0].Time : new Date('2020-01-01');

    let postDoc = await postTsColl.findOne({});
    if (!postDoc) {
      const ins = await postTsColl.insertOne({ Time: latestTs });
      postDoc = { _id: ins.insertedId };
    }
    await postTsColl.updateOne(
      { _id: postDoc._id },
      { $set: { Time: latestTs } },
      { upsert: true }
    );

    const newRecs = records.filter(r => new Date(r.Time) > latestTs);
    if (newRecs.length) {
      const res = await coll.insertMany(newRecs);
      console.log(`[OK] ${dbName} → inserted ${Object.keys(res.insertedIds).length} rows`);
    } else {
      console.log(`[SKIP] ${dbName} → no new rows after timestamp filter`);
    }
  } catch (e) {
    if (e.name === 'MongoServerSelectionError') {
      console.warn(`[WARN] Mongo timeout for ${dbName} – will retry next poll`);
    } else {
      console.error('[ERROR] pushData', e);
    }
  }
}

async function updateTime(txtPath) {
  const dbName = getDbNameFromPath(txtPath);
  if (!dbName) return;

  try {
    const mongo = await getClient();
    const db = mongo.db(dbName);
    const coll = db.collection('values');
    const tsColl = db.collection('TimeStamp');

    const last = await coll.find().sort({ Time: -1 }).limit(1).toArray();
    const newest = last.length ? last[0].Time : new Date();

    let doc = await tsColl.findOne({});
    if (!doc) {
      const ins = await tsColl.insertOne({ Time: newest });
      doc = { _id: ins.insertedId };
    }
    await tsColl.updateOne(
      { _id: doc._id },
      { $set: { Time: newest } },
      { upsert: true }
    );
  } catch (e) {
    if (e.name !== 'MongoServerSelectionError') {
      console.error('[ERROR] updateTime', e);
    }
  }
}

async function getTodayLatestTwo(folder) {
  try {
    const entries = await fsp.readdir(folder);
    const txt = entries
      .filter(f => f.toLowerCase().endsWith('.txt'))
      .map(f => ({ name: f, full: path.join(folder, f) }));

    if (!txt.length) return [];

    const withStat = await Promise.all(
      txt.map(async o => {
        const s = await fsp.stat(o.full);
        return { ...o, mtimeMs: s.mtimeMs };
      })
    );

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    return withStat
      .filter(f => f.mtimeMs >= today.getTime())
      .sort((a, b) => b.mtimeMs - a.mtimeMs)
      .slice(0, 2)
      .map(f => ({ path: f.full, mtimeMs: f.mtimeMs }));
  } catch (e) {
    if (e.code === 'EPERM' || e.code === 'ENOENT') {
      console.warn(`[WARN] ${folder} inaccessible`);
    } else {
      console.error(`[ERROR] reading ${folder}`, e.message);
    }
    return [];
  }
}

async function processFile({ path: filePath, mtimeMs }) {
  const dbName = getDbNameFromPath(filePath);
  if (!dbName) {
    console.log(`[SKIP] Unknown IP → ${filePath}`);
    return;
  }

  const folder = path.dirname(filePath);
  const base = path.basename(filePath);

  // Cache check (allow re-process if file changed in last 2 min)
  const folderCache = processedCache.get(folder) || new Map();
  const cachedMtime = folderCache.get(base);
  const ageMs = Date.now() - mtimeMs;

  if (cachedMtime === mtimeMs && ageMs > 120_000) {
    return; // skip unchanged & older than 2 min
  }

  folderCache.set(base, mtimeMs);
  if (folderCache.size > 20) {
    const keys = Array.from(folderCache.keys()).slice(-10);
    const newMap = new Map();
    keys.forEach(k => newMap.set(k, folderCache.get(k)));
    processedCache.set(folder, newMap);
  } else {
    processedCache.set(folder, folderCache);
  }

  console.log(`\n[PROCESS] ${base} → ${dbName}`);

  try {
    if (!(await convertToCSV(filePath))) return;
    const rows = await readCsv();

    if (rows.length === 0) {
      console.log(`[SKIP] ${dbName} → file contains only 0.1 rows (machine down)`);
      return;
    }

    await pushData(rows, filePath);
    await updateTime(filePath);

    console.log('[WAIT] 5 s before postData...');
    await new Promise(r => setTimeout(r, 5000));
    await postData(filePath);
    console.log(`[DONE] ${base}\n`);
  } catch (e) {
    console.error('[ERROR] processFile', e);
  }
}

async function pollAllFolders() {
  console.log(`\n[${new Date().toISOString()}] Polling 9 Diffusion PCs...`);
  for (const folder of ROOT_FOLDERS) {
    const files = await getTodayLatestTwo(folder);
    for (const f of files) {
      await processFile(f);
      await new Promise(r => setTimeout(r, 800));
    }
  }
}

const POLL_MS = 60_000;

async function pollLoop() {
  try {
    await pollAllFolders();
  } catch (e) {
    console.error('[ERROR] poll cycle', e);
  }
  setTimeout(pollLoop, POLL_MS); // schedule next poll after this finishes
}

async function startPolling() {
  console.log('\n=== TPSolar Polling (every 1 min) + 0.1 SKIP ===');
  Object.entries(IP_TO_DIFFUSION).forEach(([ip, db]) => console.log(` ${ip} → ${db}`));
  console.log(' Strategy: today → newest 2 TXT files | skip rows where P1-P5 = 0.1\n');

  try {
    await getClient();
  } catch (e) {
    console.error('[FATAL] MongoDB connection failed at startup', e);
    process.exit(1);
  }

  await pollAllFolders();

  pollLoop();

  process.on('SIGINT', async () => {
    console.log('\nSIGINT received – shutting down...');
    if (client && isConnected) await client.close();
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    console.log('\nSIGTERM received – shutting down...');
    if (client && isConnected) await client.close();
    process.exit(0);
  });
}

startPolling().catch(e => {
  console.error('[FATAL] startup', e);
  process.exit(1);
});
