require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json());
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASS,
  port: process.env.DB_PORT,
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'uploads/'),
  filename: (req, file, cb) => cb(null, Date.now() + path.extname(file.originalname))
});
const upload = multer({ storage: storage });

const mqtt = require('mqtt');
const mqttClient = mqtt.connect('mqtt://localhost:1883');

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to broker');
  mqttClient.subscribe('pothole/detection');
  mqttClient.subscribe('pothole/telemetry');
});

mqttClient.on('message', async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    if (topic === 'pothole/detection') {
      const result = await pool.query(
        `INSERT INTO potholes (geom, severity, image_name)
         VALUES (ST_SetSRID(ST_MakePoint($1, $2), 4326), $3, $4)
         RETURNING id, created_at`,
        [data.lng, data.lat, data.severity, data.image_name || null]
      );
      io.emit('new_pothole', {
        id: result.rows[0].id,
        lat: data.lat, lng: data.lng,
        severity: data.severity,
        image_name: data.image_name || null,
        created_at: result.rows[0].created_at
      });
      console.log(`[MQTT] Pothole saved: ${data.lat}, ${data.lng}`);
    }

    if (topic === 'pothole/telemetry') {
      io.emit('log', {
        message: `Speed: ${data.speed} km/h | FPS: ${data.fps} | Det: ${data.detections}`,
        type: data.detections > 0 ? 'det' : 'normal',
        speed: data.speed,
        fps: data.fps
      });
    }
  } catch (err) {
    console.error('[MQTT] Error:', err.message);
  }
}); 

let opi5Socket = null;

io.on('connection', (socket) => {
  const role = socket.handshake.query.role || (socket.handshake.auth && socket.handshake.auth.role);

  if (role === 'edge') {
    opi5Socket = socket;
    io.emit('edge_status', { online: true });
    socket.on('log', (data) => io.emit('log', data));
    socket.on('disconnect', () => {
      opi5Socket = null;
      io.emit('edge_status', { online: false });
    });
  }

  if (role === 'admin') {
    socket.emit('edge_status', { online: opi5Socket !== null });
    socket.on('command', (cmd) => {
      if (opi5Socket) opi5Socket.emit('command', cmd);
    });
  }
});

app.get('/api/potholes', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, severity, created_at, image_name,
              ST_X(geom) as lng, ST_Y(geom) as lat
       FROM potholes ORDER BY created_at DESC`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).send("Error database");
  }
});

app.post('/api/potholes', upload.single('image'), async (req, res) => {
  const { lat, lng, severity } = req.body;
  const imageName = req.file ? req.file.filename : null;
  try {
    const result = await pool.query(
      `INSERT INTO potholes (geom, severity, image_name)
       VALUES (ST_SetSRID(ST_MakePoint($1, $2), 4326), $3, $4)
       RETURNING id, created_at`,
      [lng, lat, severity, imageName]
    );
    io.emit('new_pothole', {
      id: result.rows[0].id, lat, lng, severity,
      image_name: imageName, created_at: result.rows[0].created_at
    });
    res.status(201).send("Data sukses masuk!");
  } catch (err) {
    res.status(500).send("Gagal menyimpan data");
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const total = await pool.query('SELECT COUNT(*) FROM potholes');
    const today = await pool.query(
      "SELECT COUNT(*) FROM potholes WHERE created_at::date = CURRENT_DATE"
    );
    const severity = await pool.query(
      "SELECT severity, COUNT(*) FROM potholes GROUP BY severity"
    );
    res.json({
      total: parseInt(total.rows[0].count),
      today: parseInt(today.rows[0].count),
      severity: severity.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server WebGIS + WebSocket on port ${PORT}`);
});