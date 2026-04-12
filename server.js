/**
 * Animal League — 백엔드 전용
 * 프론트에서 fetch로 연동 예시:
 *   POST /api/scores  { "school": "○○대", "game": "grade-hunt", "score": 120 }
 *   GET  /api/rankings?school=○○대   (선택: 내 학교 순위 요약)
 *   GET  /api/rankings               (학교별 누적 점수 TOP)
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");

const PORT = Number(process.env.PORT) || 3000;
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "scores.json");

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

function buildLeaderboard() {
  const { records } = readStore();
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

const app = express();
app.use(cors());
app.use(express.json({ limit: "32kb" }));

/** Render 등 크론 keep-alive용 — 파일/DB 없이 즉시 응답 */
app.get("/health", (req, res) => {
  res.status(200).json({ ok: true });
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, service: "animal-league-api" });
});

/**
 * 점수 기록 (게임 종료 시 프론트에서 호출)
 * body: { school: string, game: string, score: number }
 *      (또는 university / schoolName — 동일하게 학교명으로 저장)
 */
app.post("/api/scores", (req, res) => {
  const school = schoolFromBody(req.body);
  const game = typeof req.body.game === "string" ? req.body.game.trim().slice(0, 40) : "";
  const score = Number(req.body.score);

  if (!school) {
    return res.status(400).json({ error: "school is required" });
  }
  if (!game) {
    return res.status(400).json({ error: "game is required" });
  }
  if (!Number.isFinite(score)) {
    return res.status(400).json({ error: "score must be a number" });
  }

  const finalScore = clampScore(score);

  const store = readStore();
  const record = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    school,
    game,
    score: finalScore,
    createdAt: new Date().toISOString()
  };
  store.records.push(record);
  writeStore(store);

  const board = buildLeaderboard();
  const mine = board.find((r) => r.university === school);

  res.status(201).json({
    saved: record,
    mySchool: mine || { university: school, score: 0, playCount: 0, animal: "🦝", animalName: "너구리" }
  });
});

/**
 * 학교별 누적 점수 랭킹
 * query: limit (기본 30)
 */
app.get("/api/rankings", (req, res) => {
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 30));
  const board = buildLeaderboard();
  const slice = board.slice(0, limit).map((row, index) => ({
    rank: index + 1,
    ...row
  }));

  const schoolQ = normalizeSchool(
    req.query.school || req.query.university || req.query.schoolName || ""
  );
  let myRank = null;
  if (schoolQ) {
    const idx = board.findIndex((r) => r.university === schoolQ);
    if (idx >= 0) {
      myRank = { rank: idx + 1, ...board[idx] };
    }
  }

  res.json({ rankings: slice, mySchool: myRank });
});

/**
 * 원시 기록 조회 (디버그·관리용, 최근 N건)
 */
app.get("/api/scores/recent", (req, res) => {
  const n = Math.min(200, Math.max(1, parseInt(req.query.n, 10) || 50));
  const { records } = readStore();
  const recent = records.slice(-n).reverse();
  res.json({ records: recent });
});

ensureDataFile();

app.listen(PORT, () => {
  console.log(`Animal League API http://localhost:${PORT}`);
  console.log(`  GET  /health         (크론 keep-alive)`);
  console.log(`  POST /api/scores   { school, game, score }`);
  console.log(`  GET  /api/rankings?limit=30&school=학교명`);
});
