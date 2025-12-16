// ============================================================
// watch_results_today.js
// Monitors shared folder for new/updated .txt files *from today*
// ============================================================

const chokidar = require("chokidar");
const fs = require("fs");
const path = require("path");

// ✅ Folder to watch
const folderPath = "\\\\10.71.139.158\\d\\Results";

// ✅ Store today's date (start of day)
const today = new Date();
today.setHours(0, 0, 0, 0);

// ✅ Map to store last known file sizes
const fileSizes = new Map();

console.log(`👀 Watching folder from today (${today.toLocaleString()}):`, folderPath);

// ✅ Initialize watcher
const watcher = chokidar.watch(folderPath, {
  persistent: true,
  ignoreInitial: false,
  depth: 0,
  awaitWriteFinish: {
    stabilityThreshold: 1500,
    pollInterval: 100,
  },
  usePolling: true, // Important for network shares
  interval: 3000,
});

// ✅ Handle new files created today
watcher.on("add", (filePath) => {
  if (path.extname(filePath).toLowerCase() === ".txt") {
    try {
      const stats = fs.statSync(filePath);
      const createdTime = stats.birthtime;

      // Only react if created today
      if (createdTime >= today) {
        console.log(`🆕 [${new Date().toLocaleString()}] New file created today: ${path.basename(filePath)}`);
        fileSizes.set(filePath, stats.size);
      } else {
        // Ignore older files
        fileSizes.set(filePath, stats.size);
      }
    } catch (err) {
      console.error(`⚠️ Error reading file info: ${filePath}`, err.message);
    }
  }
});

// ✅ Handle file changes (new rows or updates) only if changed today
watcher.on("change", (filePath) => {
  if (path.extname(filePath).toLowerCase() === ".txt") {
    try {
      const stats = fs.statSync(filePath);
      const modifiedTime = stats.mtime;

      // Only process if modified today
      if (modifiedTime >= today) {
        const prevSize = fileSizes.get(filePath) || 0;
        if (stats.size > prevSize) {
          console.log(`✏️ [${new Date().toLocaleString()}] New row(s) added today: ${path.basename(filePath)}`);
        } else {
          console.log(`⚙️ [${new Date().toLocaleString()}] File updated (no size change): ${path.basename(filePath)}`);
        }
        fileSizes.set(filePath, stats.size);
      }
    } catch (err) {
      console.error(`⚠️ Error reading file size: ${filePath}`, err.message);
    }
  }
});

// ✅ Handle file deletion
watcher.on("unlink", (filePath) => {
  if (path.extname(filePath).toLowerCase() === ".txt") {
    console.log(`🗑️ [${new Date().toLocaleString()}] File deleted: ${path.basename(filePath)}`);
    fileSizes.delete(filePath);
  }
});

// ✅ Handle watcher errors
watcher.on("error", (error) => {
  console.error("❌ Watcher error:", error);
});
