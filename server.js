require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const multer = require("multer");
const path = require("path");
const http = require("http");
const { Server } = require("socket.io");
const mqtt = require("mqtt");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json());
app.use("/uploads", express.static(path.join(__dirname, "uploads")));

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASS,
  port: process.env.DB_PORT,
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, "uploads/"),
  filename: (req, file, cb) =>
    cb(null, Date.now() + path.extname(file.originalname)),
});
const upload = multer({ storage: storage });

const mqttClient = mqtt.connect("mqtt://localhost:1883");

mqttClient.on("connect", () => {
  console.log("[MQTT] Connected to broker");
  mqttClient.subscribe("pothole/detection");
  mqttClient.subscribe("pothole/telemetry");
});

mqttClient.on("message", async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    if (topic === "pothole/detection") {
      const existing = await pool.query(
        `SELECT id FROM potholes 
         WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 10)`,
        [data.lng, data.lat],
      );
      if (existing.rowCount > 0) {
        console.log("[MQTT] Dedup: skip, too close");
        return;
      }
      const result = await pool.query(
        `INSERT INTO potholes (geom, severity, det_id)
         VALUES (ST_SetSRID(ST_MakePoint($1, $2), 4326), $3, $4)
         RETURNING id, created_at`,
        [data.lng, data.lat, data.severity, data.det_id],
      );
      io.emit("new_pothole", {
        id: result.rows[0].id,
        lat: data.lat,
        lng: data.lng,
        severity: data.severity,
        image_name: null,
        created_at: result.rows[0].created_at,
      });
      console.log(
        `[MQTT] Pothole saved: ${data.lat}, ${data.lng} (${data.det_id})`,
      );
    }

    if (topic === "pothole/telemetry") {
      io.emit("log", {
        message: `Speed: ${data.speed} km/h | FPS: ${data.fps} | Det: ${data.detections}`,
        type: data.detections > 0 ? "det" : "normal",
        speed: data.speed,
        fps: data.fps,
      });
    }
  } catch (err) {
    console.error("[MQTT] Error:", err.message);
  }
});

let opi5Socket = null;

io.on("connection", (socket) => {
  const role =
    socket.handshake.query.role ||
    (socket.handshake.auth && socket.handshake.auth.role);

  if (role === "edge") {
    opi5Socket = socket;
    io.emit("edge_status", { online: true });
    socket.on("log", (data) => io.emit("log", data));
    socket.on("disconnect", () => {
      opi5Socket = null;
      io.emit("edge_status", { online: false });
    });
  }

  if (role === "admin") {
    socket.emit("edge_status", { online: opi5Socket !== null });
    socket.on("command", (cmd) => {
      if (opi5Socket) opi5Socket.emit("command", cmd);
    });
  }
});

app.get("/api/potholes", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, severity, created_at, image_name, det_id,
              ST_X(geom) as lng, ST_Y(geom) as lat
       FROM potholes ORDER BY created_at DESC`,
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).send("Error database");
  }
});

app.post("/api/potholes/image", upload.single("image"), async (req, res) => {
  const { det_id } = req.body;
  const imageName = req.file ? req.file.filename : null;
  if (!det_id || !imageName) {
    return res.status(400).send("det_id dan image wajib");
  }
  try {
    const result = await pool.query(
      "UPDATE potholes SET image_name = $1 WHERE det_id = $2 RETURNING id",
      [imageName, det_id],
    );
    if (result.rowCount > 0) {
      io.emit("pothole_image", { det_id, image_name: imageName });
      res.status(200).send("Foto berhasil diupload");
    } else {
      res.status(404).send("det_id tidak ditemukan");
    }
  } catch (err) {
    res.status(500).send("Gagal update foto");
  }
});

app.post("/api/potholes", upload.single("image"), async (req, res) => {
  const { lat, lng, severity } = req.body;
  const imageName = req.file ? req.file.filename : null;
  try {
    const result = await pool.query(
      `INSERT INTO potholes (geom, severity, image_name)
       VALUES (ST_SetSRID(ST_MakePoint($1, $2), 4326), $3, $4)
       RETURNING id, created_at`,
      [lng, lat, severity, imageName],
    );
    io.emit("new_pothole", {
      id: result.rows[0].id,
      lat,
      lng,
      severity,
      image_name: imageName,
      created_at: result.rows[0].created_at,
    });
    res.status(201).send("Data sukses masuk!");
  } catch (err) {
    res.status(500).send("Gagal menyimpan data");
  }
});

app.get("/api/stats", async (req, res) => {
  try {
    const total = await pool.query("SELECT COUNT(*) FROM potholes");
    const today = await pool.query(
      "SELECT COUNT(*) FROM potholes WHERE created_at::date = CURRENT_DATE",
    );
    const severity = await pool.query(
      "SELECT severity, COUNT(*) FROM potholes GROUP BY severity",
    );
    res.json({
      total: parseInt(total.rows[0].count),
      today: parseInt(today.rows[0].count),
      severity: severity.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/potholes/:id", async (req, res) => {
  const { id } = req.params;
  try {
    const pothole = await pool.query(
      "SELECT image_name FROM potholes WHERE id = $1",
      [id],
    );
    if (pothole.rows.length > 0 && pothole.rows[0].image_name) {
      const imgPath = path.join(
        __dirname,
        "uploads",
        pothole.rows[0].image_name,
      );
      if (require("fs").existsSync(imgPath)) require("fs").unlinkSync(imgPath);
    }
    await pool.query("DELETE FROM potholes WHERE id = $1", [id]);
    res.json({ deleted: id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server WebGIS + WebSocket + MQTT on port ${PORT}`);
});
