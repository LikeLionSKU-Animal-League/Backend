/**
 * Animal League — 백엔드 전용
 * 프론트에서 fetch로 연동 예시:
 *   POST /api/scores  { "school": "○○대", "game": "grade-hunt", "score": 120 }
 *   GET  /api/rankings?school=○○대   (선택: 내 학교 순위 요약)
 *   GET  /api/rankings               (학교별 누적 점수 TOP)
 *
 * 저장소: DATABASE_URL 이 있으면 PostgreSQL, 없으면 data/scores.json
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const rateLimit = require("express-rate-limit");

const PORT = Number(process.env.PORT) || 3000;
const DATABASE_URL = process.env.DATABASE_URL || "";
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "scores.json");

/** Render Postgres 등 외부 호스트는 TLS 필요 */
function createPool() {
  if (!DATABASE_URL) return null;
  const useSsl =
    DATABASE_URL.includes("sslmode=require") || /\.render\.com\//.test(DATABASE_URL);
  const rejectUnauthorized = process.env.DB_SSL_REJECT_UNAUTHORIZED !== "false";
  return new Pool({
    connectionString: DATABASE_URL,
    ...(useSsl ? { ssl: { rejectUnauthorized } } : {})
  });
}

const pool = createPool();

const ANIMAL_TIERS = [
  { min: 5000, emoji: "🐯", name: "호랑이" },
  { min: 3000, emoji: "🐴", name: "말" },
  { min: 2000, emoji: "🦜", name: "앵무새" },
  { min: 1200, emoji: "🐱", name: "고양이" },
  { min: 600, emoji: "🐿️", name: "다람쥐" },
  { min: 300, emoji: "🐰", name: "토끼" },
  { min: 0, emoji: "🦝", name: "너구리" }
];

function ensureDataFile() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!fs.existsSync(DATA_FILE)) {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ records: [] }, null, 2), "utf8");
  }
}

function readStore() {
  ensureDataFile();
  try {
    const raw = fs.readFileSync(DATA_FILE, "utf8");
    const data = JSON.parse(raw);
    if (!data.records || !Array.isArray(data.records)) {
      return { records: [] };
    }
    return data;
  } catch {
    return { records: [] };
  }
}

function writeStore(data) {
  ensureDataFile();
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), "utf8");
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS score_records (
      id VARCHAR(64) PRIMARY KEY,
      school VARCHAR(60) NOT NULL,
      game VARCHAR(40) NOT NULL,
      score INTEGER NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT score_records_score_range CHECK (score >= -100000 AND score <= 1000000)
    );
    CREATE INDEX IF NOT EXISTS idx_score_records_school ON score_records (school);
    CREATE INDEX IF NOT EXISTS idx_score_records_created_at ON score_records (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_score_records_dedup ON score_records (school, game, score, created_at DESC);
  `);
}

function animalForTotalScore(total) {
  for (const t of ANIMAL_TIERS) {
    if (total >= t.min) {
      return { emoji: t.emoji, name: t.name };
    }
  }
  return { emoji: "🦝", name: "너구리" };
}

function normalizeSchool(s) {
  if (typeof s !== "string") return "";
  return s.trim().slice(0, 60);
}

/** 프론트에서 school / university 등 다른 키로 보내는 경우 모두 허용 */
function schoolFromBody(body) {
  if (!body || typeof body !== "object") return "";
  const raw = body.school ?? body.university ?? body.schoolName;
  return normalizeSchool(typeof raw === "string" ? raw : "");
}

/** 한 판 점수 상한·하한 (악의적 값 방지, 음수는 학점 사냥 등에서 허용) */
const SCORE_MIN = -100000;
const SCORE_MAX = 1000000;

function clampScore(n) {
  const x = Math.round(Number(n));
  if (!Number.isFinite(x)) return 0;
  return Math.min(SCORE_MAX, Math.max(SCORE_MIN, x));
}

function buildLeaderboardFromRecords(records) {
  const bySchool = new Map();

  for (const r of records) {
    const key = r.school;
    if (!key) continue;
    const prev = bySchool.get(key) || { totalScore: 0, playCount: 0 };
    prev.totalScore += Number(r.score) || 0;
    prev.playCount += 1;
    bySchool.set(key, prev);
  }

  const rows = [...bySchool.entries()].map(([university, agg]) => {
    const animal = animalForTotalScore(agg.totalScore);
    return {
      university,
      score: agg.totalScore,
      playCount: agg.playCount,
      animal: animal.emoji,
      animalName: animal.name
    };
  });

  rows.sort((a, b) => b.score - a.score);
  return rows;
}

// ⭐ 캐싱 변수 추가
let cachedBoard = null;
let lastFetch = 0;
let cachedLimit = 0;
const CACHE_TTL = 3000; // 3초

async function buildLeaderboard(limit = 30) {
  if (pool) {

    // ⭐ 캐싱 적용
    if (cachedBoard && limit <= cachedLimit && Date.now() - lastFetch < CACHE_TTL) {
      return cachedBoard.slice(0, limit);
    }

    const { rows } = await pool.query(`
      SELECT school AS university,
             SUM(score)::bigint AS total_score,
             COUNT(*)::int AS play_count
      FROM score_records
      GROUP BY school
      ORDER BY total_score DESC
      LIMIT $1
    `, [limit]);

    const result = rows.map((r) => {
      const score = Number(r.total_score);
      const animal = animalForTotalScore(score); // 기존 기능 유지

      return {
        university: r.university,
        score,
        playCount: r.play_count,
        animal: animal.emoji,
        animalName: animal.name
      };
    });

    // ⭐ 캐시 저장
    cachedBoard = result;
    cachedLimit = limit;
    lastFetch = Date.now();

    return result;
  }

  return buildLeaderboardFromRecords(readStore().records);
}

// ⭐ insertRecord 수정 (캐시 초기화 + 중복 방지 추가)
async function insertRecord(record) {
  if (pool) {
    // 동일 school+game+score 조합이 10초 내 존재하면 중복으로 거부
    const { rows: dup } = await pool.query(
      `SELECT id FROM score_records
       WHERE school = $1 AND game = $2 AND score = $3
         AND created_at > NOW() - INTERVAL '10 seconds'
       LIMIT 1`,
      [record.school, record.game, record.score]
    );
    if (dup.length > 0) {
      const err = new Error("duplicate submission");
      err.code = "DUPLICATE";
      throw err;
    }

    await pool.query(
      `INSERT INTO score_records (id, school, game, score, created_at)
       VALUES ($1, $2, $3, $4, $5::timestamptz)`,
      [record.id, record.school, record.game, record.score, record.createdAt]
    );

    // ⭐ 데이터 변경 → 캐시 초기화
    cachedBoard = null;
    cachedLimit = 0;

    return;
  }

  const store = readStore();
  store.records.push(record);
  writeStore(store);
}

async function fetchSchoolSummary(school) {
  if (!school) return null;

  if (pool) {
    const { rows } = await pool.query(
      `WITH school_totals AS (
         SELECT school,
                SUM(score)::bigint AS total_score,
                COUNT(*)::int AS play_count
         FROM score_records
         GROUP BY school
       )
       SELECT t.school AS university,
              t.total_score,
              t.play_count,
              (
                SELECT COUNT(*)::int + 1
                FROM school_totals t2
                WHERE t2.total_score > t.total_score
              ) AS rank
       FROM school_totals t
       WHERE t.school = $1`,
      [school]
    );

    if (!rows.length) return null;

    const score = Number(rows[0].total_score);
    const animal = animalForTotalScore(score);
    return {
      rank: Number(rows[0].rank),
      university: rows[0].university,
      score,
      playCount: Number(rows[0].play_count),
      animal: animal.emoji,
      animalName: animal.name
    };
  }

  const board = buildLeaderboardFromRecords(readStore().records);
  const idx = board.findIndex((r) => r.university === school);
  if (idx < 0) return null;
  return {
    rank: idx + 1,
    ...board[idx]
  };
}

async function fetchRecentRecords(n) {
  if (pool) {
    const { rows } = await pool.query(
      `SELECT id, school, game, score, created_at
       FROM score_records
       ORDER BY created_at DESC
       LIMIT $1`,
      [n]
    );
    return rows.map((r) => ({
      id: r.id,
      school: r.school,
      game: r.game,
      score: r.score,
      createdAt: r.created_at.toISOString()
    }));
  }
  const { records } = readStore();
  return records.slice(-n).reverse();
}

/** 허용 게임 목록 (환경변수 미설정 시 검증 생략) */
const VALID_GAMES = process.env.VALID_GAMES
  ? process.env.VALID_GAMES.split(",").map((g) => g.trim()).filter(Boolean)
  : null;

/** Rate limiters */
const scoreLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Too many requests. Please try again later." }
});

const readLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Too many requests. Please try again later." }
});

/** 허용 출처 화이트리스트 (환경변수 미설정 시 전체 허용) */
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(",").map((o) => o.trim()).filter(Boolean)
  : null;

const corsOptions = allowedOrigins
  ? {
      origin: (origin, callback) => {
        if (!origin || allowedOrigins.includes(origin)) {
          callback(null, true);
        } else {
          callback(new Error("Not allowed by CORS"));
        }
      }
    }
  : {};

const app = express();
app.use(cors(corsOptions));
app.use(express.json({ limit: "32kb" }));

/** Render 등 크론 keep-alive용 — 파일/DB 없이 즉시 응답 */
app.get("/health", (req, res) => {
  res.status(200).json({ ok: true });
});

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "animal-league-api",
    storage: pool ? "postgresql" : "json-file"
  });
});

/**
 * 점수 기록 (게임 종료 시 프론트에서 호출)
 * body: { school: string, game: string, score: number }
 *      (또는 university / schoolName — 동일하게 학교명으로 저장)
 */
app.post("/api/scores", scoreLimiter, async (req, res) => {
  const school = schoolFromBody(req.body);
  const game = typeof req.body.game === "string" ? req.body.game.trim().slice(0, 40) : "";
  const score = Number(req.body.score);

  if (!school) {
    return res.status(400).json({ error: "school is required" });
  }
  if (!game) {
    return res.status(400).json({ error: "game is required" });
  }
  if (VALID_GAMES && !VALID_GAMES.includes(game)) {
    return res.status(400).json({ error: `game must be one of: ${VALID_GAMES.join(", ")}` });
  }
  if (!Number.isFinite(score)) {
    return res.status(400).json({ error: "score must be a number" });
  }

  const finalScore = clampScore(score);
  const record = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    school,
    game,
    score: finalScore,
    createdAt: new Date().toISOString()
  };

  try {
    await insertRecord(record);
    const mine = await fetchSchoolSummary(school);

    res.status(201).json({
      saved: record,
      mySchool:
        mine || {
          university: school,
          score: 0,
          playCount: 0,
          animal: "🦝",
          animalName: "너구리"
        }
    });
  } catch (err) {
    if (err.code === "DUPLICATE") {
      return res.status(409).json({ error: "duplicate submission" });
    }
    console.error("POST /api/scores", err);
    res.status(500).json({ error: "failed to save score" });
  }
});

/**
 * 학교별 누적 점수 랭킹
 * query: limit (기본 30)
 */
// ⭐ rankings API 수정 (핵심)
app.get("/api/rankings", readLimiter, async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 30));

    // ⭐ limit 전달
    const board = await buildLeaderboard(limit);

    const slice = board.map((row, index) => ({
      rank: index + 1,
      ...row
    }));

    const schoolQ = normalizeSchool(
      req.query.school || req.query.university || req.query.schoolName || ""
    );

    let myRank = null;
    if (schoolQ) {
      myRank = await fetchSchoolSummary(schoolQ);
    }

    res.json({ rankings: slice, mySchool: myRank });
  } catch (err) {
    console.error("GET /api/rankings", err);
    res.status(500).json({ error: "failed to load rankings" });
  }
});

/**
 * 원시 기록 조회 (디버그·관리용, 최근 N건)
 */
app.get("/api/scores/recent", readLimiter, async (req, res) => {
  try {
    const n = Math.min(200, Math.max(1, parseInt(req.query.n, 10) || 50));
    const records = await fetchRecentRecords(n);
    res.json({ records });
  } catch (err) {
    console.error("GET /api/scores/recent", err);
    res.status(500).json({ error: "failed to load records" });
  }
});

async function start() {
  if (pool) {
    try {
      await initDb();
      console.log("PostgreSQL connected (DATABASE_URL)");
    } catch (err) {
      console.error("PostgreSQL init failed:", err.message);
      process.exit(1);
    }
  } else {
    ensureDataFile();
    console.log("Using local JSON file:", DATA_FILE);
  }

  app.listen(PORT, () => {
    console.log(`Animal League API http://localhost:${PORT}`);
    console.log(`  GET  /health         (크론 keep-alive)`);
    console.log(`  POST /api/scores   { school, game, score }`);
    console.log(`  GET  /api/rankings?limit=30&school=학교명`);
  });
}

start();
