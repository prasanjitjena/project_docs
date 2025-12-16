

// // ============================================================
// // single_pc_watch_test.js
// // Watches one PC's Results folder for .txt file changes
// // ============================================================

// const chokidar = require("chokidar");
// const fs = require("fs");
// const path = require("path");

// const folder = '\\\\10.71.139.159\\d\\Results'; // ✅ Working path

// console.log("🔍 Watching folder:", folder);

// // Store last file sizes to detect row additions
// const fileSizeMap = new Map();

// const watcher = chokidar.watch(path.join(folder, "*.txt"), {
//   ignored: /(^|[\/\\])\../, // ignore hidden files
//   persistent: true,
//   ignoreInitial: false,
//   usePolling: true,  // reliable for network drives
//   interval: 3000,    // poll every 3 seconds
// });

// watcher
//   .on("add", filePath => {
//     console.log(`📄 [NEW FILE] ${filePath}`);
//     recordFileSize(filePath);
//   })
//   .on("change", filePath => {
//     checkFileUpdate(filePath);
//   })
//   .on("unlink", filePath => {
//     console.log(`🗑️ [DELETED FILE] ${filePath}`);
//     fileSizeMap.delete(filePath);
//   })
//   .on("error", error => console.error(`⚠️ Watcher error: ${error}`));

// // ==== Helper functions ====

// async function recordFileSize(filePath) {
//   try {
//     const stats = await fs.promises.stat(filePath);
//     fileSizeMap.set(filePath, stats.size);
//   } catch (err) {
//     console.error("Error recording size:", err.message);
//   }
// }

// async function checkFileUpdate(filePath) {
//   try {
//     const stats = await fs.promises.stat(filePath);
//     const oldSize = fileSizeMap.get(filePath) || 0;
//     const newSize = stats.size;

//     if (newSize > oldSize) {
//       console.log(`➕ [ROW ADDED] ${path.basename(filePath)} — new data detected`);

//       // ✅ Optional: show the last line (newest data)
//       const content = await fs.promises.readFile(filePath, "utf8");
//       const lines = content.trim().split(/\r?\n/);
//       console.log("🆕 Last line:", lines[lines.length - 1]);
//     } else {
//       console.log(`✏️ [UPDATED FILE] ${path.basename(filePath)} — modified`);
//     }

//     fileSizeMap.set(filePath, newSize);
//   } catch (err) {
//     console.error("Error checking file update:", err.message);
//   }
// }

// console.log("👀 File watcher running... Press Ctrl+C to stop.");




// ============================================================
// watch_txt_rows.js
// Robust watcher for detecting new rows in network .txt files
// ============================================================

const fs = require("fs");
const path = require("path");

// ✅ Path to the shared folder (network drive)
const folderPath = '\\\\10.71.139.159\\d\\Results';

// Store previous file sizes
const fileSizeMap = new Map();

// Polling interval (in milliseconds)
const POLL_INTERVAL = 5000; // 5 seconds

console.log("📁 Monitoring folder:", folderPath);
console.log("🔁 Checking every", POLL_INTERVAL / 1000, "seconds...\n");

// === Function to scan folder and detect new rows ===
function checkFolder() {
  try {
    const files = fs.readdirSync(folderPath)
      .filter(f => f.toLowerCase().endsWith(".txt"))
      .map(f => path.join(folderPath, f));

    for (const filePath of files) {
      try {
        const stats = fs.statSync(filePath);
        const prevSize = fileSizeMap.get(filePath) || 0;
        const newSize = stats.size;

        // New file
        if (!fileSizeMap.has(filePath)) {
          console.log(`📄 [NEW FILE] ${path.basename(filePath)} detected (${newSize} bytes)`);
        }

        // File grew → new rows added
        if (newSize > prevSize) {
          console.log(`➕ [ROW ADDED] ${path.basename(filePath)} grew from ${prevSize} → ${newSize} bytes`);

          // Read last few lines
          const content = fs.readFileSync(filePath, "utf8");
          const lines = content.trim().split(/\r?\n/);
          const lastLine = lines[lines.length - 1];
          console.log("🆕 Last row data:", lastLine);
        }

        // File shrunk or modified (edge case)
        else if (newSize < prevSize) {
          console.log(`✏️ [UPDATED/OVERWRITTEN] ${path.basename(filePath)} (size decreased)`);
        }

        // Update size record
        fileSizeMap.set(filePath, newSize);
      } catch (err) {
        console.error(`⚠️ Error reading file ${filePath}:`, err.message);
      }
    }

    // Remove deleted files from tracking
    for (const tracked of Array.from(fileSizeMap.keys())) {
      if (!files.includes(tracked)) {
        console.log(`🗑️ [DELETED FILE] ${path.basename(tracked)}`);
        fileSizeMap.delete(tracked);
      }
    }
  } catch (err) {
    console.error("❌ Error reading folder:", err.message);
  }
}

// === Start polling ===
setInterval(checkFolder, POLL_INTERVAL);

// Initial scan
checkFolder();
