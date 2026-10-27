#!/usr/bin/env node
// Horsy 0.1.0 — built from packages/*/src — see scripts/build-bundle.mjs
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __esm = (fn, res, err) => function __init() {
  if (err) throw err[0];
  try {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  } catch (e) {
    throw err = [e], e;
  }
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// packages/core/build/store.js
import { Buffer as Buffer2 } from "node:buffer";
import { randomUUID } from "node:crypto";
import { appendFileSync, chmodSync, closeSync, fstatSync, linkSync, lstatSync, mkdirSync, openSync, readFileSync, readSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
function tighten(path, mode) {
  if (tightened.has(path))
    return;
  tightened.add(path);
  try {
    const st = lstatSync(path);
    if (!st.isSymbolicLink() && (st.mode & 63) !== 0)
      chmodSync(path, mode);
  } catch {
  }
}
function horsyHome() {
  return process.env["HORSY_HOME"] ?? join(homedir(), ".horsy");
}
function ensureHome() {
  const dir2 = horsyHome();
  mkdirSync(dir2, { recursive: true, mode: 448 });
  tighten(dir2, 448);
  return dir2;
}
function ensureDir(dir2) {
  ensureHome();
  mkdirSync(dir2, { recursive: true, mode: 448 });
  tighten(dir2, 448);
}
function sleep(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}
function readId(file) {
  try {
    const id = readFileSync(file, "utf8").trim();
    return ID_RE.test(id) ? id : null;
  } catch {
    return null;
  }
}
function publishId(file, id, how) {
  const tmp = `${file}.${process.pid}.${randomUUID()}`;
  writeFileSync(tmp, id, PRIVATE_FILE);
  try {
    if (how === "link")
      linkSync(tmp, file);
    else
      renameSync(tmp, file);
  } catch (err) {
    if (how !== "link" || err.code !== "EEXIST")
      throw err;
  } finally {
    rmSync(tmp, { force: true });
  }
}
function peekInstallId() {
  return readId(join(horsyHome(), "install_id"));
}
function getInstallId() {
  const home = ensureHome();
  const cached = idCache.get(home);
  if (cached)
    return cached;
  const file = join(home, "install_id");
  tighten(file, 384);
  let id = readId(file);
  if (!id) {
    publishId(file, randomUUID(), "link");
    const deadline = Date.now() + 50;
    while (!(id = readId(file)) && Date.now() < deadline)
      sleep(5);
  }
  if (!id) {
    publishId(file, randomUUID(), "rename");
    id = readId(file);
    if (!id)
      throw new Error("install_id unreadable");
  }
  idCache.set(home, id);
  return id;
}
function capStrings(v2, exempt = false, max = MAX_STRING) {
  if (typeof v2 === "string")
    return exempt || v2.length <= max ? v2 : v2.slice(0, max);
  if (Array.isArray(v2))
    return v2.map((x) => capStrings(x, exempt, max));
  if (typeof v2 === "object" && v2 !== null) {
    return Object.fromEntries(Object.entries(v2).map(([k, x]) => [k, capStrings(x, EXEMPT_KEYS.has(k), k === "reason" ? MAX_REASON : MAX_STRING)]));
  }
  return v2;
}
function appendEvent(e) {
  const capped = e.type === "attempt" && e.file_hashes.length > MAX_FILE_HASHES ? { ...e, file_hashes: e.file_hashes.slice(0, MAX_FILE_HASHES) } : e;
  const line = JSON.stringify(capStrings(capped)) + "\n";
  if (Buffer2.byteLength(line) >= MAX_LINE_BYTES)
    throw new Error("event line too large");
  const file = join(ensureHome(), "events.jsonl");
  appendFileSync(file, line, PRIVATE_FILE);
  tighten(file, 384);
}
function readEvents() {
  let raw;
  try {
    raw = readFileSync(join(horsyHome(), "events.jsonl"), "utf8");
  } catch {
    return [];
  }
  const events = [];
  for (const line of raw.split("\n")) {
    if (!line)
      continue;
    const e = parseEvent(line);
    if (e)
      events.push(e);
  }
  return events;
}
function readRecentEvents(maxBytes) {
  let fd = null;
  try {
    fd = openSync(join(horsyHome(), "events.jsonl"), "r");
    const size = fstatSync(fd).size;
    const start = Math.max(0, size - maxBytes);
    const buf = Buffer2.alloc(size - start);
    let got = 0;
    while (got < buf.length) {
      const n = readSync(fd, buf, got, buf.length - got, start + got);
      if (n === 0)
        break;
      got += n;
    }
    const text = buf.subarray(0, got).toString("utf8");
    const lines = text.split("\n");
    if (start > 0)
      lines.shift();
    const events = [];
    for (const line of lines) {
      const e = line ? parseEvent(line) : null;
      if (e)
        events.push(e);
    }
    return events;
  } catch {
    return [];
  } finally {
    if (fd !== null)
      closeSync(fd);
  }
}
var MAX_LINE_BYTES, MAX_FILE_HASHES, ID_RE, PRIVATE_FILE, tightened, idCache, EXEMPT_KEYS, MAX_STRING, MAX_REASON;
var init_store = __esm({
  "packages/core/build/store.js"() {
    "use strict";
    init_events();
    MAX_LINE_BYTES = 4096;
    MAX_FILE_HASHES = 50;
    ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
    PRIVATE_FILE = { mode: 384 };
    tightened = /* @__PURE__ */ new Set();
    __name(tighten, "tighten");
    __name(horsyHome, "horsyHome");
    __name(ensureHome, "ensureHome");
    __name(ensureDir, "ensureDir");
    __name(sleep, "sleep");
    idCache = /* @__PURE__ */ new Map();
    __name(readId, "readId");
    __name(publishId, "publishId");
    __name(peekInstallId, "peekInstallId");
    __name(getInstallId, "getInstallId");
    EXEMPT_KEYS = /* @__PURE__ */ new Set([
      "schema_version",
      "type",
      "event_id",
      "ts",
      "install_id",
      "session_id",
      "repo_hash",
      "task_id",
      "parent_task_id",
      "agent_id",
      "file_hashes",
      "desc_hash"
    ]);
    MAX_STRING = 64;
    MAX_REASON = 200;
    __name(capStrings, "capStrings");
    __name(appendEvent, "appendEvent");
    __name(readEvents, "readEvents");
    __name(readRecentEvents, "readRecentEvents");
  }
});

// packages/core/build/events.js
import { randomUUID as randomUUID2 } from "node:crypto";
function makeEvent(fields, ctx) {
  return {
    schema_version: SCHEMA_VERSION,
    event_id: randomUUID2(),
    ts: (/* @__PURE__ */ new Date()).toISOString(),
    install_id: getInstallId(),
    session_id: ctx.session_id,
    repo_hash: ctx.repo_hash,
    ...fields
  };
}
function parseEvent(line) {
  let v2;
  try {
    v2 = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof v2 !== "object" || v2 === null || Array.isArray(v2))
    return null;
  const o = v2;
  if (o["schema_version"] !== SCHEMA_VERSION)
    return null;
  if (typeof o["type"] !== "string" || !TYPES.includes(o["type"]))
    return null;
  for (const k of ["event_id", "ts", "session_id", "repo_hash"]) {
    if (typeof o[k] !== "string")
      return null;
  }
  return v2;
}
var SCHEMA_VERSION, TYPES;
var init_events = __esm({
  "packages/core/build/events.js"() {
    "use strict";
    init_store();
    SCHEMA_VERSION = 1;
    TYPES = ["task_created", "route_decision", "attempt", "verification", "outcome_delayed", "override"];
    __name(makeEvent, "makeEvent");
    __name(parseEvent, "parseEvent");
  }
});

// packages/core/build/repo.js
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { realpathSync } from "node:fs";
function salted(value, id = getInstallId()) {
  return createHash("sha256").update(id + value).digest("hex").slice(0, 16);
}
function repoRoot(cwd) {
  try {
    const top = execFileSync("git", ["rev-parse", "--show-toplevel"], {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 2e3
    }).trim();
    return top ? realpathSync(top) : null;
  } catch {
    return null;
  }
}
function repoHash(cwd) {
  let fallback2 = cwd;
  try {
    fallback2 = realpathSync(cwd);
  } catch {
  }
  return salted(repoRoot(cwd) ?? fallback2);
}
function repoHashReadOnly(cwd) {
  let fallback2 = cwd;
  try {
    fallback2 = realpathSync(cwd);
  } catch {
  }
  return salted(repoRoot(cwd) ?? fallback2, peekInstallId() ?? "");
}
function hashPath(relPath) {
  return salted(relPath);
}
var init_repo = __esm({
  "packages/core/build/repo.js"() {
    "use strict";
    init_store();
    __name(salted, "salted");
    __name(repoRoot, "repoRoot");
    __name(repoHash, "repoHash");
    __name(repoHashReadOnly, "repoHashReadOnly");
    __name(hashPath, "hashPath");
  }
});

// packages/core/build/classify.js
function v(phrase, ...extra) {
  const [head = "", ...tail] = phrase.split(" ");
  let h;
  if (/[^aeiou]e$/.test(head))
    h = `${head.slice(0, -1)}(?:e|es|ed|ing)`;
  else if (/[^aeiou]y$/.test(head))
    h = `${head.slice(0, -1)}(?:y|ies|ied|ying)`;
  else
    h = `${head}(?:s|es|ed|ing)?`;
  const base = [h, ...tail].join("\\s+");
  return extra.length ? `(?:${base}|${extra.join("|")})` : base;
}
function dropProhibitions(text) {
  const parts = text.toLowerCase().replaceAll("\u2019", "'").split(/([.!?\n])/);
  let out = "";
  for (let i = 0; i < parts.length; i += 2) {
    const sentence = (parts[i] ?? "").trim();
    if (PROHIBITION_SENTENCE_RE.test(sentence) || PROHIBITION_PHRASES.has(sentence))
      continue;
    out += (parts[i] ?? "") + (parts[i + 1] ?? "");
  }
  return out;
}
function isPath(token) {
  if (FRAMEWORK_JS.test(token))
    return false;
  if (FILE_EXT.test(token) && token.length > 3)
    return true;
  if (token.startsWith("./") || token.startsWith("../"))
    return true;
  const segments = token.split("/");
  if (segments.length < 2)
    return false;
  return segments.length >= 3 || PATH_DIRS.has(segments[0] ?? "");
}
function normalizeDescription(s) {
  return s.toLowerCase().replace(/\s+/g, " ").trim();
}
function firstMatch(table, text) {
  return table.find(([, r]) => r.test(text))?.[0] ?? null;
}
function onlyReadClauses(description, prompt) {
  let source = description.trim();
  if (!source)
    source = prompt.trim().split(/(?<=[.!?])\s+|\n/)[0] ?? "";
  const clauses = ` ${dropProhibitions(source)} `.replace(IF_AFTER_RE, "$1").split(CLAUSE_SPLIT_RE).map((c) => c.trim()).filter(Boolean);
  if (clauses.length === 0)
    return false;
  return clauses.every((c) => /\/|\w\.\w/.test(c.split(/\s+/)[0] ?? "") || CLAUSE_START_RE.test(c) || QUESTION_RE.test(c));
}
function pickVerb(text, description, prompt, subagentType) {
  if (subagentType === "Explore") {
    if (READ_ONLY_RE.find(([k]) => k === "explain")?.[1].test(text))
      return "explain";
    if (DIAGNOSIS_RE.test(text))
      return "review";
    return READ_ONLY_RE.find(([k]) => k === "read")?.[1].test(text) ? "read" : "search";
  }
  let verb = firstMatch(STRONG_EDIT_RE, text);
  if (subagentType === "Plan")
    return verb ? "other" : "plan";
  if (verb === "implement") {
    if (TEST_NOUN_RE.test(text))
      return "test";
    return DOCS_NOUN_RE.test(text) ? "docs" : verb;
  }
  if (verb)
    return verb;
  verb = firstMatch(READ_ONLY_RE, text);
  if (verb && onlyReadClauses(description, prompt))
    return DIAGNOSIS_RE.test(text) ? "review" : verb;
  if (WEAK_FIX_RE.test(text))
    return "fix";
  if (TEST_NOUN_RE.test(text))
    return "test";
  return DOCS_NOUN_RE.test(text) ? "docs" : "other";
}
function pickBreadth(text) {
  const tokens = (text.match(/[\w.@/-]+/g) ?? []).map((t) => t.replace(/[./]+$/, ""));
  const paths = new Set(tokens.filter(isPath)).size;
  if (WIDE_RE.test(text) || paths > 4)
    return "many";
  return paths >= 2 ? "few" : "single";
}
function classify(input, ctx) {
  const description = input.description ?? "";
  const prompt = input.prompt ?? "";
  const subagent_type = input.subagent_type ?? null;
  const tail = prompt.length > PROMPT_WINDOW ? prompt.slice(-PROMPT_WINDOW) : "";
  const text = [description, prompt.slice(0, PROMPT_WINDOW), tail].map(dropProhibitions).join(" ");
  const verb = pickVerb(text, description, prompt, subagent_type);
  const breadth = pickBreadth(text);
  const features = {
    verb,
    read_only: READ_ONLY_VERBS.includes(verb),
    breadth,
    has_tests: ctx.has_tests,
    prompt_chars: prompt.length,
    subagent_type: loggedType(subagent_type, ctx.hash_description !== false),
    desc_hash: ctx.hash_description === false ? "" : hashPath(normalizeDescription(description))
  };
  return { task_class: breadth === "many" ? `${verb}-wide` : verb, features };
}
var READ_ONLY_VERBS, SAFE_CLASSES, STRONG_EDIT, READ_ONLY, DIAGNOSIS, WEAK_FIX, TEST_NOUN, DOCS_NOUN, PROHIBITION_SENTENCE_RE, PROHIBITION_PHRASES, re, STRONG_EDIT_RE, READ_ONLY_RE, DIAGNOSIS_RE, WEAK_FIX_RE, TEST_NOUN_RE, DOCS_NOUN_RE, CODE_NOUNS, WIDE_RE, FILE_EXT, FRAMEWORK_JS, PATH_DIRS, PROMPT_WINDOW, WHITELIST, QUESTION_OPENERS, FILLERS, CLAUSE_START_RE, QUESTION_RE, CLAUSE_SPLIT_RE, IF_AFTER_RE, BUILT_IN_TYPES, loggedType;
var init_classify = __esm({
  "packages/core/build/classify.js"() {
    "use strict";
    init_repo();
    READ_ONLY_VERBS = ["search", "read", "explain", "review", "plan"];
    SAFE_CLASSES = ["search", "read", "explain", "rename", "search-wide", "read-wide", "explain-wide"];
    __name(v, "v");
    STRONG_EDIT = [
      ["migrate", [v("migrate"), v("port to")]],
      ["refactor", [v("refactor"), v("restructure"), "clean(?:s|ed|ing)?\\s+(?:it\\s+|this\\s+|that\\s+|things\\s+)?up", "cleanups?", v("simplify")]],
      // Explicit fix words only. Beats the generic edit group ("apply fixes" is a fix).
      ["fix", [v("fix"), v("debug", "debugg(?:ed|ing)"), v("resolve"), v("patch")]],
      [
        "implement",
        [
          ...["implement", "update", "change", "modify", "replace", "apply", "edit", "correct", "remove", "delete", "optimize", "optimise"].map((w) => v(w)),
          ...["add", "create", "rewrite", "write", "make", "wire", "convert", "upgrade", "bump"].map((w) => v(w)),
          ...[
            "tidy",
            "prune",
            "harden",
            "inline",
            "swap",
            "substitute",
            "squash",
            "push",
            "deploy",
            "release",
            "publish",
            "toggle",
            "annotate",
            "ensure",
            "merge",
            "rebase",
            "revert",
            "install",
            "uninstall",
            "enable",
            "disable",
            "configure",
            "generate",
            "regenerate",
            "stabilize",
            "stabilise",
            "normalize",
            "normalise",
            "parameterize",
            "parameterise",
            "restyle",
            "split",
            "move",
            "extract",
            "insert",
            "append",
            "fill in",
            "sort out",
            "take care of",
            "go ahead"
          ].map((w) => v(w)),
          ...["drop", "truncate", "purge", "wipe", "destroy"].map((w) => v(w)),
          // destructive: never read-only, wherever they appear
          "rm\\s+-\\w+",
          v("commit", "committ(?:ed|ing)"),
          v("reformat", "reformatt(?:ed|ing)"),
          v("shrink", "shrunk"),
          v("deal with", "dealt\\s+with"),
          "handle(?:s|d)?",
          // not "handling": "error handling" is a noun
          "speed(?:s|ed|ing)?\\s+(?:it\\s+)?up",
          "get(?:s|ting)?\\s+(?:it|this|ci|the build|tests?)\\s+(?:working|green|passing)",
          "wrote",
          "written",
          "rewrote",
          "rewritten",
          "made",
          "buil(?:d|ds|ding|t)\\s+(?:a|an|the|out)",
          "set(?:s|ting)?\\s+up",
          "hook(?:s|ed|ing)?\\s+up"
        ]
      ],
      ["rename", [v("rename")]]
    ];
    READ_ONLY = [
      ["review", [v("review"), v("audit"), v("critique")]],
      ["plan", [v("plan", "plann(?:ed|ing)"), "roadmap", "strategy"]],
      ["explain", [v("explain"), v("describe"), v("walk through"), "what (?:does|do|did|is|are)", "why", "how (?:does|do|did|is|are)", v("understand", "understood")]],
      [
        "read",
        [v("read"), v("summarize"), v("summarise"), v("inspect"), v("check"), v("investigate"), v("trace"), v("compare"), v("analyze"), v("analyse"), v("look at"), v("look into"), v("look through"), v("go through"), v("examine"), v("explore"), v("scan"), v("study"), v("identify"), v("research"), v("verify")]
      ],
      ["search", [v("find", "found"), v("search"), v("locate"), v("grep", "grepp(?:ed|ing)"), "where (?:is|are)", v("look for"), v("list")]]
    ];
    DIAGNOSIS = ["investigat\\w*", "analy[sz]\\w*", "audit\\w*", "diagnos\\w*", "root[\\s-]cause", "why", "security", "vulnerab\\w*", "threat\\w*", "exploit\\w*"];
    WEAK_FIX = [v("bug"), v("error"), v("fail"), "broken", "flaky"];
    TEST_NOUN = ["tests?", "testing", "specs?", "coverage", "vitest", "jest"];
    DOCS_NOUN = ["docs?", "documentation", "readme", "docstrings?", "jsdoc"];
    PROHIBITION_SENTENCE_RE = /^(?:please\s+)?(?:do not|don't|dont)\s+(?:make\s+any\s+changes|modify|edit|change|write|create|touch|delete|update)(?:\s+(?:any|the))?(?:\s+(?:files?|code|anything))?\s*$/;
    PROHIBITION_PHRASES = /* @__PURE__ */ new Set(["no changes", "make no changes", "read-only", "read only", "this is read-only", "this is a read-only task"]);
    __name(dropProhibitions, "dropProhibitions");
    re = /* @__PURE__ */ __name((words) => new RegExp(`\\b(?:${words.join("|")})\\b`), "re");
    STRONG_EDIT_RE = STRONG_EDIT.map(([verb, words]) => [verb, re(words)]);
    READ_ONLY_RE = READ_ONLY.map(([verb, words]) => [verb, re(words)]);
    DIAGNOSIS_RE = re(DIAGNOSIS);
    WEAK_FIX_RE = re(WEAK_FIX);
    TEST_NOUN_RE = re(TEST_NOUN);
    DOCS_NOUN_RE = re(DOCS_NOUN);
    CODE_NOUNS = "files|usages|callers|call sites|references|occurrences|modules|packages|tests|components|endpoints";
    WIDE_RE = new RegExp(`\\b(?:all|every|each)(?:\\s+[\\w-]+){0,3}?\\s+(?:${CODE_NOUNS})\\b|\\b(?:entire|whole)\\s+(?:codebase|repo(?:sitory)?)\\b|\\bacross\\s+(?:the\\s+)?(?:codebase|repo(?:sitory)?)\\b|\\brepo-wide\\b`);
    FILE_EXT = /\.(?:ts|tsx|js|jsx|mjs|cjs|json|md|py|go|rs|java|rb|css|html|ya?ml|toml|sh)$/;
    FRAMEWORK_JS = /^(?:node|next|vue|react|express|three|d3|nuxt|nest|ember|backbone)\.js$/;
    PATH_DIRS = /* @__PURE__ */ new Set(["src", "lib", "test", "tests", "packages", "app", "apps", "docs", "scripts", "bin", "components"]);
    __name(isPath, "isPath");
    PROMPT_WINDOW = 2e3;
    __name(normalizeDescription, "normalizeDescription");
    __name(firstMatch, "firstMatch");
    WHITELIST = [
      v("find", "found"),
      v("search"),
      v("locate"),
      v("grep", "grepp(?:ed|ing)"),
      v("list"),
      v("read"),
      "look(?:s|ed|ing)?\\s+(?:at|into|for|through)",
      v("inspect"),
      v("check"),
      v("verify"),
      v("review"),
      v("audit"),
      v("explain"),
      v("summarize"),
      v("summarise"),
      v("describe"),
      v("understand", "understood"),
      v("investigate"),
      v("trace"),
      v("analyze"),
      v("analyse"),
      v("examine"),
      v("explore"),
      v("identify"),
      v("compare"),
      v("show", "shown"),
      v("report"),
      v("count"),
      v("map"),
      v("research"),
      v("scan"),
      v("study"),
      v("determine"),
      "figur(?:e|es|ed|ing)\\s+out",
      v("answer"),
      v("tell"),
      v("fetch"),
      v("plan", "plann(?:ed|ing)"),
      v("note"),
      "walk(?:s|ed|ing)?\\s+through",
      "go(?:es|ing)?\\s+through"
    ];
    QUESTION_OPENERS = "what|where|why|how|which|who";
    FILLERS = "(?:(?:please|also|first|now|just|quickly|carefully|then|and)\\s+)*";
    CLAUSE_START_RE = new RegExp(`^${FILLERS}(?:${WHITELIST.join("|")})\\b`);
    QUESTION_RE = new RegExp(`^${FILLERS}(?:${QUESTION_OPENERS})\\b.*\\?`);
    CLAUSE_SPLIT_RE = /(?<=\?)\s*|\.(?=\s)|[!:;,()[\]\n…+|→–—]|->|\s-\s|\s\/\s|\s(?:and|then|if|so|to|but|or|by|before|after|while|once|when|until)\s|\s*&\s*/;
    IF_AFTER_RE = /\b(check|verify|see|determine|figure out|find out|tell me|explain|understand|investigate)\s+(?:if|whether)\b/g;
    __name(onlyReadClauses, "onlyReadClauses");
    __name(pickVerb, "pickVerb");
    __name(pickBreadth, "pickBreadth");
    BUILT_IN_TYPES = ["general-purpose", "Explore", "Plan"];
    loggedType = /* @__PURE__ */ __name((t, salted2) => t === null || BUILT_IN_TYPES.includes(t) ? t : salted2 ? `custom:${hashPath(t).slice(0, 8)}` : "custom", "loggedType");
    __name(classify, "classify");
  }
});

// packages/core/build/models.js
function modelFamily(idOrAlias) {
  const m = /(haiku|sonnet|opus|fable|mythos)/i.exec(idOrAlias);
  return m?.[1] ? m[1].toLowerCase() : null;
}
var FAMILY_RANK, CURRENT;
var init_models = __esm({
  "packages/core/build/models.js"() {
    "use strict";
    FAMILY_RANK = { haiku: 0, sonnet: 1, opus: 2, fable: 3, mythos: 3 };
    CURRENT = {
      haiku: "claude-haiku-5-5",
      sonnet: "claude-sonnet-5-5",
      opus: "claude-opus-5-5",
      fable: "claude-fable-5-1"
    };
    __name(modelFamily, "modelFamily");
  }
});

// packages/core/build/pricing.json
var pricing_default;
var init_pricing = __esm({
  "packages/core/build/pricing.json"() {
    pricing_default = {
      as_of: "2026-10-19",
      source: "https://platform.claude.com/docs/en/about-claude/pricing",
      models: {
        "claude-fable-5-1": {
          input: 10,
          output: 50,
          cache_read: 0.25,
          cache_write_5m: 12.5,
          cache_write_1h: 20
        },
        "claude-fable-5": {
          input: 10,
          output: 50,
          cache_read: 1,
          cache_write_5m: 12.5,
          cache_write_1h: 20
        },
        "claude-mythos-5-1": {
          input: 10,
          output: 50,
          cache_read: 0.25,
          cache_write_5m: 12.5,
          cache_write_1h: 20
        },
        "claude-mythos-5": {
          input: 10,
          output: 50,
          cache_read: 1,
          cache_write_5m: 12.5,
          cache_write_1h: 20
        },
        "claude-opus-5-5": {
          input: 4,
          output: 20,
          cache_read: 0.2,
          cache_write_5m: 5,
          cache_write_1h: 8
        },
        "claude-opus-5": {
          input: 5,
          output: 25,
          cache_read: 0.5,
          cache_write_5m: 6.25,
          cache_write_1h: 10
        },
        "claude-opus-4-8": {
          input: 5,
          output: 25,
          cache_read: 0.5,
          cache_write_5m: 6.25,
          cache_write_1h: 10
        },
        "claude-opus-4-7": {
          input: 5,
          output: 25,
          cache_read: 0.5,
          cache_write_5m: 6.25,
          cache_write_1h: 10
        },
        "claude-opus-4-6": {
          input: 5,
          output: 25,
          cache_read: 0.5,
          cache_write_5m: 6.25,
          cache_write_1h: 10
        },
        "claude-opus-4-5": {
          input: 5,
          output: 25,
          cache_read: 0.5,
          cache_write_5m: 6.25,
          cache_write_1h: 10
        },
        "claude-sonnet-5-5": {
          input: 2,
          output: 10,
          cache_read: 0.1,
          cache_write_5m: 2.5,
          cache_write_1h: 4
        },
        "claude-sonnet-5": {
          input: 2,
          output: 10,
          cache_read: 0.2,
          cache_write_5m: 2.5,
          cache_write_1h: 4
        },
        "claude-sonnet-4-6": {
          input: 3,
          output: 15,
          cache_read: 0.3,
          cache_write_5m: 3.75,
          cache_write_1h: 6
        },
        "claude-sonnet-4-5": {
          input: 3,
          output: 15,
          cache_read: 0.3,
          cache_write_5m: 3.75,
          cache_write_1h: 6
        },
        "claude-haiku-5-5": {
          input: 0.1,
          output: 0.5,
          cache_read: 0.01,
          cache_write_5m: 0.125,
          cache_write_1h: 0.2,
          long_context: {
            threshold_tokens: 1e5,
            input: 0.5,
            output: 2.5
          }
        },
        "claude-haiku-4-5": {
          input: 1,
          output: 5,
          cache_read: 0.1,
          cache_write_5m: 1.25,
          cache_write_1h: 2
        }
      }
    };
  }
});

// packages/core/build/pricing.js
function normalizeModelId(id) {
  return id.replace(/\[[^\]]*\]$/, "").replace(/^(?:(?:us|eu|apac|global)\.)?anthropic\./, "").replace(/-v\d+(?::\d+)?$/, "").replace(/@.*$/, "").replace(/-\d{8}$/, "");
}
function entry(key) {
  return Object.hasOwn(TABLE, key) ? TABLE[key] : void 0;
}
function priceFor(modelId) {
  const meta = { as_of: pricing_default.as_of, source: pricing_default.source };
  const id = normalizeModelId(modelId);
  const exact = entry(modelId) ?? entry(id);
  if (exact)
    return { ...exact, ...meta };
  const fam = CLAUDE_ID_RE.test(id) ? modelFamily(id) : null;
  const current = fam === null ? void 0 : entry(CURRENT[fam === "mythos" ? "fable" : fam]);
  return current ? { ...current, ...meta, approx: true } : null;
}
function costOf(modelId, u, opts) {
  return costInfo(modelId, u, opts).cost;
}
function costInfo(modelId, u, opts) {
  const p = priceFor(modelId);
  if (!p)
    return { cost: null, approx: false };
  const lc = p.long_context;
  const total = u.input_tokens + u.cache_creation_input_tokens + u.cache_read_input_tokens;
  const long = opts?.aggregate !== true && lc !== void 0 && total > lc.threshold_tokens;
  const inRate = long ? lc.input : p.input;
  const outRate = long ? lc.output : p.output;
  const k = inRate / p.input;
  const w1h = Math.max(u.cache_creation_1h_input_tokens, 0);
  const w5m = Math.max(u.cache_creation_input_tokens - u.cache_creation_1h_input_tokens, 0);
  const cost = (u.input_tokens * inRate + u.output_tokens * outRate + u.cache_read_input_tokens * p.cache_read * k + w5m * p.cache_write_5m * k + w1h * p.cache_write_1h * k) / 1e6;
  return { cost, approx: p.approx === true };
}
var PRICING_AS_OF, PRICING_SOURCE_URL, TABLE, CLAUDE_ID_RE;
var init_pricing2 = __esm({
  "packages/core/build/pricing.js"() {
    "use strict";
    init_models();
    init_pricing();
    PRICING_AS_OF = pricing_default.as_of;
    PRICING_SOURCE_URL = "https://platform.claude.com/docs/en/about-claude/pricing";
    TABLE = pricing_default.models;
    __name(normalizeModelId, "normalizeModelId");
    CLAUDE_ID_RE = /^claude-(?:haiku|sonnet|opus|fable|mythos)\b/;
    __name(entry, "entry");
    __name(priceFor, "priceFor");
    __name(costOf, "costOf");
    __name(costInfo, "costInfo");
  }
});

// packages/core/build/config.js
import { readFileSync as readFileSync2, renameSync as renameSync2, rmSync as rmSync2, writeFileSync as writeFileSync2 } from "node:fs";
import { join as join2 } from "node:path";
function merge(raw) {
  const d = DEFAULT_CONFIG;
  const jev = isObj(raw["jev"]) ? raw["jev"] : {};
  const verify = isObj(raw["verify"]) ? raw["verify"] : {};
  const ac = raw["active_classes"];
  const ladder = raw["ladder"];
  return {
    mode: typeof raw["mode"] === "string" && MODES.includes(raw["mode"]) ? raw["mode"] : d.mode,
    active_classes: ac === "all" || Array.isArray(ac) && ac.every((x) => typeof x === "string") ? ac : d.active_classes,
    decider: typeof raw["decider"] === "string" && DECIDERS.includes(raw["decider"]) ? raw["decider"] : d.decider,
    ladder: Array.isArray(ladder) && ladder.length > 0 && ladder.every(isArm) && ladder[ladder.length - 1]?.model === "inherit" ? ladder.map((a) => ({ model: a.model, effort: a.effort })) : d.ladder,
    tau: unit(raw["tau"], d.tau),
    min_outcomes: Number.isInteger(raw["min_outcomes"]) && raw["min_outcomes"] >= 0 ? raw["min_outcomes"] : d.min_outcomes,
    explore_rate: exploreRate(raw["explore_rate"], d.explore_rate),
    jev: {
      enabled: bool(jev["enabled"], d.jev.enabled),
      timeout_ms: pos(jev["timeout_ms"], d.jev.timeout_ms),
      confidence_floor: num(jev["confidence_floor"], d.jev.confidence_floor)
    },
    verify: {
      timeout_s: pos(verify["timeout_s"], d.verify.timeout_s),
      max_blocks: pos(verify["max_blocks"], d.verify.max_blocks),
      repos: strings(verify["repos"], d.verify.repos),
      env_passthrough: strings(verify["env_passthrough"], d.verify.env_passthrough)
    }
  };
}
function readFile() {
  try {
    const v2 = JSON.parse(readFileSync2(join2(horsyHome(), "config.json"), "utf8"));
    return merge(isObj(v2) ? v2 : {});
  } catch {
    return structuredClone(DEFAULT_CONFIG);
  }
}
function loadConfig() {
  const c = readFile();
  const env = process.env["HORSY_MODE"];
  if (env !== void 0 && MODES.includes(env))
    c.mode = env;
  return c;
}
function saveConfig(patch) {
  const cur = readFile();
  const p = defined(patch);
  const next = merge({ ...cur, ...p, jev: { ...cur.jev, ...defined(patch.jev) }, verify: { ...cur.verify, ...defined(patch.verify) } });
  const file = join2(ensureHome(), "config.json");
  const tmp = `${file}.${process.pid}.tmp`;
  try {
    writeFileSync2(tmp, JSON.stringify(next, null, 2) + "\n", PRIVATE_FILE);
    renameSync2(tmp, file);
  } finally {
    rmSync2(tmp, { force: true });
  }
  return next;
}
var DEFAULT_CONFIG, MODES, DECIDERS, EFFORTS, isObj, num, unit, exploreRate, pos, defined, strings, bool, isArm, loadSavedConfig;
var init_config = __esm({
  "packages/core/build/config.js"() {
    "use strict";
    init_store();
    DEFAULT_CONFIG = {
      mode: "shadow",
      active_classes: "all",
      decider: "rules",
      ladder: [
        { model: "haiku", effort: "low" },
        { model: "sonnet", effort: "medium" },
        { model: "inherit", effort: null }
      ],
      tau: 0.8,
      min_outcomes: 20,
      explore_rate: 0.07,
      jev: { enabled: false, timeout_ms: 800, confidence_floor: 0.6 },
      verify: { timeout_s: 300, max_blocks: 2, repos: [], env_passthrough: [] }
    };
    MODES = ["shadow", "active", "off"];
    DECIDERS = ["rules", "bandit", "jev"];
    EFFORTS = ["low", "medium", "high", "xhigh", "max"];
    isObj = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    num = /* @__PURE__ */ __name((v2, d) => typeof v2 === "number" && Number.isFinite(v2) ? v2 : d, "num");
    unit = /* @__PURE__ */ __name((v2, d) => typeof v2 === "number" && Number.isFinite(v2) ? Math.min(Math.max(v2, 0), 1) : d, "unit");
    exploreRate = /* @__PURE__ */ __name((v2, d) => typeof v2 === "number" && Number.isFinite(v2) ? Math.min(Math.max(v2, 0), 0.1) : d, "exploreRate");
    pos = /* @__PURE__ */ __name((v2, d) => typeof v2 === "number" && Number.isFinite(v2) && v2 > 0 ? v2 : d, "pos");
    defined = /* @__PURE__ */ __name((o) => Object.fromEntries(Object.entries(o ?? {}).filter(([, v2]) => v2 !== void 0)), "defined");
    strings = /* @__PURE__ */ __name((v2, d) => Array.isArray(v2) && v2.every((x) => typeof x === "string") ? [...v2] : [...d], "strings");
    bool = /* @__PURE__ */ __name((v2, d) => typeof v2 === "boolean" ? v2 : d, "bool");
    isArm = /* @__PURE__ */ __name((v2) => isObj(v2) && typeof v2["model"] === "string" && (v2["effort"] === null || typeof v2["effort"] === "string" && EFFORTS.includes(v2["effort"])), "isArm");
    __name(merge, "merge");
    __name(readFile, "readFile");
    __name(loadConfig, "loadConfig");
    loadSavedConfig = /* @__PURE__ */ __name(() => readFile(), "loadSavedConfig");
    __name(saveConfig, "saveConfig");
  }
});

// packages/core/build/bandit.js
import { Buffer as Buffer3 } from "node:buffer";
import { createHash as createHash2 } from "node:crypto";
import { closeSync as closeSync2, fstatSync as fstatSync2, openSync as openSync2, readFileSync as readFileSync3, readSync as readSync2, renameSync as renameSync3, rmSync as rmSync3, statSync, writeFileSync as writeFileSync3 } from "node:fs";
import { join as join3 } from "node:path";
function mulberry32(seed) {
  let s = seed >>> 0;
  return () => {
    s = s + 1831565813 >>> 0;
    let t = s;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
function normal(rng) {
  return Math.sqrt(-2 * Math.log(1 - rng())) * Math.cos(2 * Math.PI * rng());
}
function sampleGamma(shape, rng) {
  if (shape < 1)
    return sampleGamma(shape + 1, rng) * Math.pow(1 - rng(), 1 / shape);
  const d = shape - 1 / 3;
  const c = 1 / Math.sqrt(9 * d);
  for (; ; ) {
    let x;
    let v2;
    do {
      x = normal(rng);
      v2 = 1 + c * x;
    } while (v2 <= 0);
    v2 = v2 * v2 * v2;
    const u = 1 - rng();
    if (u < 1 - 0.0331 * x * x * x * x || Math.log(u) < 0.5 * x * x + d * (1 - v2 + Math.log(v2)))
      return d * v2;
  }
}
function sampleBeta(a, b, rng) {
  const x = sampleGamma(a, rng);
  const y = sampleGamma(b, rng);
  return x + y > 0 ? x / (x + y) : 0.5;
}
function priorsFor(taskClass, ladder) {
  const table = PRIORS[taskClass.replace(/-wide$/, "")] ?? HARD_PRIOR;
  return ladder.map((rung) => {
    const f = modelFamily(rung.model);
    const [a, b] = table[f === "haiku" ? 0 : f === "sonnet" ? 1 : 2];
    return { a, b, n: 0 };
  });
}
function rungKey(arm, ladder) {
  const f = modelFamily(arm.model);
  return f && ladder.find((r) => modelFamily(r.model) === f)?.model || "inherit";
}
function clone(stats) {
  const out = {};
  for (const [r, classes] of Object.entries(stats)) {
    out[r] = {};
    for (const [c, rungs] of Object.entries(classes)) {
      out[r][c] = {};
      for (const [m, cell] of Object.entries(rungs))
        out[r][c][m] = { a: cell.a, b: cell.b, n: cell.n };
    }
  }
  return out;
}
function foldOutcomes(stats, events, ladder) {
  const out = clone(stats);
  for (const e of events) {
    let da;
    let db;
    let dn;
    if (e.type === "verification") {
      if (e.scope !== "subagent" || e.result === "no_signal" || e.blocked)
        continue;
      [da, db, dn] = e.result === "pass" ? [WEAK, 0, 1] : [0, WEAK, 1];
    } else if (e.type === "outcome_delayed") {
      if (e.success === null || !Number.isFinite(e.weight))
        continue;
      [da, db] = e.success ? [e.weight, 0] : [0, e.weight];
      if (e.supersedes === "pass")
        da -= WEAK;
      else if (e.supersedes === "fail")
        db -= WEAK;
      dn = e.supersedes ? 0 : 1;
    } else
      continue;
    if (!e.task_class || !e.arm)
      continue;
    const classes = out[e.repo_hash] ??= {};
    const rungs = classes[e.task_class] ??= {};
    const cell = rungs[rungKey(e.arm, ladder)] ??= { a: 0, b: 0, n: 0 };
    cell.a = Math.max(0, cell.a + da);
    cell.b = Math.max(0, cell.b + db);
    cell.n += dn;
  }
  return out;
}
function parseLines(buf) {
  const events = [];
  for (const line of buf.toString("utf8").split("\n")) {
    if (!line)
      continue;
    const e = parseEvent(line);
    if (e)
      events.push(e);
  }
  return events;
}
function preadAll(fd, buf, pos2) {
  let got = 0;
  while (got < buf.length) {
    const r = readSync2(fd, buf, got, buf.length - got, pos2 + got);
    if (r === 0)
      break;
    got += r;
  }
  return got;
}
function headOf(fd, offset) {
  const buf = Buffer3.alloc(Math.min(HEAD_BYTES, offset));
  return sha1(buf.subarray(0, preadAll(fd, buf, 0)));
}
function readFrom(file, from, check, cap2) {
  const fd = openSync2(file, "r");
  try {
    const size = fstatSync2(fd).size;
    let rebuilt = false;
    if (check) {
      const nl = Buffer3.alloc(1);
      const ok = size >= from && size >= check.size_at && headOf(fd, from) === check.head && (from === 0 || preadAll(fd, nl, from - 1) === 1 && nl[0] === 10);
      if (!ok) {
        if (cap2 !== void 0 && size > cap2)
          return null;
        from = 0;
        rebuilt = true;
      }
    }
    const buf = Buffer3.alloc(Math.max(size - from, 0));
    const got = preadAll(fd, buf, from);
    const end = buf.subarray(0, got).lastIndexOf(10) + 1;
    return { events: parseLines(buf.subarray(0, end)), offset: from + end, size, head: headOf(fd, from + end), rebuilt };
  } finally {
    closeSync2(fd);
  }
}
function readCache(file) {
  try {
    const c = JSON.parse(readFileSync3(file, "utf8"));
    if (!isObj2(c) || c["v"] !== 1 || !isObj2(c["stats"]))
      return null;
    const { offset, size_at, head, ladder } = c;
    if (typeof offset !== "number" || typeof size_at !== "number" || !(offset >= 0) || !(size_at >= 0))
      return null;
    if (typeof head !== "string" || typeof ladder !== "string")
      return null;
    for (const classes of Object.values(c["stats"])) {
      if (!isObj2(classes))
        return null;
      for (const rungs of Object.values(classes)) {
        if (!isObj2(rungs))
          return null;
        for (const cell of Object.values(rungs)) {
          if (!isObj2(cell))
            return null;
          for (const k of ["a", "b", "n"]) {
            const x = cell[k];
            if (typeof x !== "number" || !(x >= 0))
              return null;
          }
        }
      }
    }
    return c;
  } catch {
    return null;
  }
}
function loadCellStats(ladder, opts = {}) {
  const cap2 = opts.inline_max_bytes;
  try {
    const home = horsyHome();
    const eventsFile = join3(home, "events.jsonl");
    const cacheFile = join3(home, "cells.json");
    const lk = ladderKey(ladder);
    let cached = readCache(cacheFile);
    if (cached && cached.ladder !== lk)
      cached = null;
    if (cap2 !== void 0) {
      const size = statSync(eventsFile).size;
      if (!cached && size > cap2)
        return {};
      if (cached && size - cached.offset > cap2)
        return {};
    }
    const read = cached ? readFrom(eventsFile, cached.offset, cached, cap2) : readFrom(eventsFile, 0);
    if (!read)
      return {};
    const reuse = cached && !read.rebuilt ? cached : null;
    const stats = foldOutcomes(reuse?.stats ?? {}, read.events, ladder);
    if (opts.save !== false && (!reuse || read.offset !== reuse.offset)) {
      const tmp = `${cacheFile}.${process.pid}.tmp`;
      try {
        ensureHome();
        const body = { v: 1, offset: read.offset, size_at: read.size, head: read.head, ladder: lk, stats };
        writeFileSync3(tmp, JSON.stringify(body), PRIVATE_FILE);
        renameSync3(tmp, cacheFile);
      } catch {
      } finally {
        rmSync3(tmp, { force: true });
      }
    }
    return stats;
  } catch {
    if (cap2 !== void 0)
      return {};
    try {
      return foldOutcomes({}, readFrom(join3(horsyHome(), "events.jsonl"), 0)?.events ?? [], ladder);
    } catch {
      return {};
    }
  }
}
function posteriors(stats, repoHash2, taskClass, ladder) {
  const observed = stats[repoHash2]?.[taskClass] ?? {};
  return priorsFor(taskClass, ladder).map((p, i) => {
    const o = observed[ladder[i].model];
    return o ? { a: p.a + o.a, b: p.b + o.b, n: o.n } : p;
  });
}
function thompsonChoose(cells, tau, rng) {
  for (let i = 0; i < cells.length - 1; i++) {
    const c = cells[i];
    if (sampleBeta(c.a, c.b, rng) >= tau)
      return i;
  }
  return cells.length - 1;
}
function propensity(cells, tau, chosen, rng, opts) {
  const samples = opts.samples && opts.samples > 0 ? opts.samples : 200;
  let hits = 0;
  for (let s = 0; s < samples; s++)
    if (thompsonChoose(cells, tau, rng) === chosen)
      hits++;
  const frac = hits / samples;
  const p = opts.explore ? (1 - opts.explore_rate) * frac + opts.explore_rate / cells.length : frac;
  return Math.min(1, Math.max(1e-3, p));
}
var mean, READ_ONLY_PRIOR, MECHANICAL_PRIOR, HARD_PRIOR, PRIORS, WEAK, isObj2, HEAD_BYTES, sha1, ladderKey;
var init_bandit = __esm({
  "packages/core/build/bandit.js"() {
    "use strict";
    init_events();
    init_models();
    init_store();
    __name(mulberry32, "mulberry32");
    __name(normal, "normal");
    __name(sampleGamma, "sampleGamma");
    __name(sampleBeta, "sampleBeta");
    mean = /* @__PURE__ */ __name((c) => c.a / (c.a + c.b), "mean");
    READ_ONLY_PRIOR = [[7, 3], [8.5, 1.5], [9, 1]];
    MECHANICAL_PRIOR = [[4, 6], [7.5, 2.5], [9, 1]];
    HARD_PRIOR = [[2, 8], [6, 4], [9, 1]];
    PRIORS = {
      search: READ_ONLY_PRIOR,
      read: READ_ONLY_PRIOR,
      explain: READ_ONLY_PRIOR,
      review: READ_ONLY_PRIOR,
      plan: READ_ONLY_PRIOR,
      rename: MECHANICAL_PRIOR,
      docs: MECHANICAL_PRIOR,
      test: MECHANICAL_PRIOR
    };
    __name(priorsFor, "priorsFor");
    WEAK = 0.5;
    __name(rungKey, "rungKey");
    __name(clone, "clone");
    __name(foldOutcomes, "foldOutcomes");
    isObj2 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    __name(parseLines, "parseLines");
    HEAD_BYTES = 256;
    __name(preadAll, "preadAll");
    sha1 = /* @__PURE__ */ __name((b) => createHash2("sha1").update(b).digest("hex"), "sha1");
    __name(headOf, "headOf");
    __name(readFrom, "readFrom");
    ladderKey = /* @__PURE__ */ __name((ladder) => ladder.map((r) => r.model).join("|"), "ladderKey");
    __name(readCache, "readCache");
    __name(loadCellStats, "loadCellStats");
    __name(posteriors, "posteriors");
    __name(thompsonChoose, "thompsonChoose");
    __name(propensity, "propensity");
  }
});

// packages/core/build/deciders/bandit.js
var banditDecider;
var init_bandit2 = __esm({
  "packages/core/build/deciders/bandit.js"() {
    "use strict";
    init_bandit();
    banditDecider = {
      name: "bandit",
      decide(ctx) {
        const { cells, rng, config } = ctx;
        const arm_index = thompsonChoose(cells, config.tau, rng);
        const p = propensity(cells, config.tau, arm_index, rng, { explore_rate: config.explore_rate, explore: false });
        return Promise.resolve({ arm_index, propensity: p, decider: "bandit", reason: "thompson" });
      }
    };
  }
});

// packages/core/build/deciders/rules.js
function rulesDecision(ctx) {
  const last = Math.max(ctx.config.ladder.length - 1, 0);
  const want = RUNG0.has(ctx.task_class) ? 0 : RUNG1.has(ctx.task_class) ? 1 : last;
  return { arm_index: Math.min(want, last), propensity: 1, decider: "rules", reason: `rules:${ctx.task_class}` };
}
var RUNG0, RUNG1, rulesDecider;
var init_rules = __esm({
  "packages/core/build/deciders/rules.js"() {
    "use strict";
    RUNG0 = /* @__PURE__ */ new Set(["search", "read", "explain", "search-wide", "read-wide", "explain-wide"]);
    RUNG1 = /* @__PURE__ */ new Set(["rename", "docs", "test", "review"]);
    __name(rulesDecision, "rulesDecision");
    rulesDecider = { name: "rules", decide: /* @__PURE__ */ __name((ctx) => Promise.resolve(rulesDecision(ctx)), "decide") };
  }
});

// packages/core/build/deciders/jev.js
import { readFileSync as readFileSync4 } from "node:fs";
import { join as join4 } from "node:path";
function keyFromDotenv(names) {
  let raw;
  try {
    raw = readFileSync4(join4(horsyHome(), ".env"), "utf8");
  } catch {
    return null;
  }
  for (const name of names) {
    for (const line of raw.split("\n")) {
      const t = line.trim();
      if (!t.startsWith(`${name}=`))
        continue;
      const v2 = t.slice(name.length + 1).trim().replace(/^(['"])(.*)\1$/, "$2");
      if (v2)
        return v2;
    }
  }
  return null;
}
function jevKey(env) {
  const names = ["HORSY_JEV_KEY", standardVar()];
  for (const n of names) {
    const v2 = env[n];
    if (v2)
      return v2;
  }
  return keyFromDotenv(names);
}
function redactDescription(text) {
  const out = text.replace(URL_RE, "<url>").replace(EMAIL_RE, "<email>").replace(/\S+/g, (w) => {
    const m = EDGE_RE.exec(w);
    return m && isPathLike(m[2] ?? "") ? `${m[1]}<path>${m[3]}` : w;
  }).replace(TOKEN_RE, "<token>");
  return out.slice(0, 200);
}
function fallback(ctx, code) {
  const d = rulesDecision(ctx);
  return { ...d, reason: `jev_fallback:${code}` };
}
async function call(ctx, key) {
  const doFetch = ctx.fetchImpl ?? globalThis.fetch;
  const { timeout_ms } = ctx.config.jev;
  const ac = new AbortController();
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      ac.abort();
      reject(new Error("timeout"));
    }, timeout_ms);
  });
  const body = {
    model: "jev-latest",
    state: {
      task: redactDescription(ctx.description),
      task_class: ctx.task_class,
      read_only: ctx.features.read_only,
      breadth: ctx.features.breadth,
      has_tests: ctx.features.has_tests
    },
    questions: {
      tier: {
        type: "choice",
        instructions: "Which model tier is enough to do this coding sub-task well?",
        criteria: {
          haiku: "Trivial: lookup, search, read, explain, rename, small single-file edit",
          sonnet: "Moderate: typical coding, multi-step but well scoped, tests, review",
          opus: "Hard: architecture, multi-file refactor, subtle debugging, ambiguous"
        }
      },
      high_effort: {
        type: "noul",
        instructions: "The task needs deep multi-step reasoning",
        criteria: { true: "Hard reasoning needed", false: "Routine task" }
      }
    }
  };
  try {
    const res = await Promise.race([
      doFetch(ENDPOINT, {
        method: "POST",
        headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: ac.signal
      }),
      deadline
    ]);
    if (!res.ok)
      throw new Error(`http_${res.status}`);
    const json = await Promise.race([res.json(), deadline]);
    if (!isObj3(json))
      throw new Error("bad_response");
    return json;
  } finally {
    clearTimeout(timer);
  }
}
var ENDPOINT, COST_PER_INPUT_TOKEN, isObj3, standardVar, URL_RE, EMAIL_RE, TOKEN_RE, EDGE_RE, isPathLike, jevDecider;
var init_jev = __esm({
  "packages/core/build/deciders/jev.js"() {
    "use strict";
    init_store();
    init_rules();
    ENDPOINT = "https://api.typesafe.ai/v1/systemone";
    COST_PER_INPUT_TOKEN = 42e-9;
    isObj3 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    standardVar = /* @__PURE__ */ __name(() => ["TYPESAFE", "API", "KEY"].join("_"), "standardVar");
    __name(keyFromDotenv, "keyFromDotenv");
    __name(jevKey, "jevKey");
    URL_RE = /\b(?:[a-z][a-z0-9+.-]*:\/\/|www\.)\S+/gi;
    EMAIL_RE = /[\w.+-]+@[\w-]+(?:\.[\w-]+)+/g;
    TOKEN_RE = /[A-Za-z0-9+/=_-]{24,}/g;
    EDGE_RE = /^([^\w~$%/\\]*)(.*?)([^\w/\\]*)$/s;
    isPathLike = /* @__PURE__ */ __name((t) => {
      if (/^(?:~|\$HOME|%USERPROFILE%)(?:[/\\]|$)/i.test(t))
        return true;
      const seps = (t.match(/[/\\]/g) ?? []).length;
      return seps >= 2 || seps >= 1 && /\.[A-Za-z0-9]{1,8}$/.test(t);
    }, "isPathLike");
    __name(redactDescription, "redactDescription");
    __name(fallback, "fallback");
    __name(call, "call");
    jevDecider = {
      name: "jev",
      async decide(ctx) {
        if (!ctx.config.jev.enabled)
          return fallback(ctx, "disabled");
        const key = jevKey(ctx.env ?? process.env);
        if (!key)
          return fallback(ctx, "no_key");
        let json;
        try {
          json = await call(ctx, key);
        } catch (err) {
          const m = err instanceof Error ? err.message : "";
          return fallback(ctx, m === "timeout" ? "timeout" : m.startsWith("http_") ? m : m === "bad_response" || err instanceof SyntaxError ? "bad_response" : "network");
        }
        const answers = isObj3(json["answers"]) ? json["answers"] : {};
        const tier = isObj3(answers["tier"]) ? answers["tier"] : {};
        const choice = tier["choice"];
        const confidence = tier["confidence"];
        if (choice !== "haiku" && choice !== "sonnet" && choice !== "opus" || typeof confidence !== "number") {
          return fallback(ctx, "bad_response");
        }
        const last = Math.max(ctx.config.ladder.length - 1, 0);
        let idx = choice === "haiku" ? 0 : choice === "sonnet" ? 1 : last;
        let reason = `jev:${choice}@${confidence.toFixed(2)}`;
        if (confidence < ctx.config.jev.confidence_floor) {
          idx += 1;
          reason += ":up";
        }
        const noul = isObj3(answers["high_effort"]) ? answers["high_effort"]["noul"] : void 0;
        if (typeof noul === "number" && noul > 0.5) {
          idx = Math.max(idx, 1);
          reason += ":hard";
        }
        const usage = isObj3(json["usage"]) ? json["usage"] : {};
        const tokens = typeof usage["input_tokens"] === "number" ? usage["input_tokens"] : 0;
        return { arm_index: Math.min(idx, last), propensity: 1, decider: "jev", reason, cost_usd: tokens * COST_PER_INPUT_TOKEN };
      }
    };
  }
});

// packages/core/build/deciders/index.js
function getDecider(name) {
  return name === "bandit" ? banditDecider : name === "jev" ? jevDecider : rulesDecider;
}
var init_deciders = __esm({
  "packages/core/build/deciders/index.js"() {
    "use strict";
    init_bandit2();
    init_jev();
    init_rules();
    init_rules();
    init_jev();
    __name(getDecider, "getDecider");
  }
});

// packages/core/build/agent-perms.js
import { readFileSync as readFileSync5, statSync as statSync2 } from "node:fs";
import { homedir as homedir2 } from "node:os";
import { join as join5 } from "node:path";
function agentSettingsFiles(o) {
  const platform = o.platform ?? process.platform;
  const home = o.home ?? homedir2();
  const managed = platform === "darwin" ? ["/Library/Application Support/ClaudeCode/managed-settings.json"] : platform === "win32" ? [] : ["/etc/claude-code/managed-settings.json"];
  const projects = [...new Set(o.roots)].flatMap((r) => [join5(r, ".claude", "settings.json"), join5(r, ".claude", "settings.local.json")]);
  return [...managed, join5(o.env["CLAUDE_CONFIG_DIR"] || join5(home, ".claude"), "settings.json"), ...projects];
}
function agentPermission(files) {
  let result = "allow";
  for (const f of files) {
    let text;
    try {
      const st = statSync2(f);
      if (!st.isFile() || st.size > MAX_BYTES) {
        result = "ask";
        continue;
      }
      text = readFileSync5(f, "utf8");
    } catch (e) {
      if (e.code !== "ENOENT")
        result = "ask";
      continue;
    }
    try {
      const parsed = JSON.parse(text);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
        throw new Error("not an object");
      const perms = parsed.permissions;
      if (perms === void 0)
        continue;
      if (typeof perms !== "object" || perms === null || Array.isArray(perms))
        throw new Error("bad permissions");
      for (const kind of ["deny", "ask"]) {
        const list2 = perms[kind];
        if (list2 === void 0)
          continue;
        if (!Array.isArray(list2))
          throw new Error("bad list");
        if (list2.some((r) => typeof r === "string" && AGENT_RULE.test(r.trim()))) {
          if (kind === "deny")
            return "deny";
          result = "ask";
        }
      }
    } catch {
      result = "ask";
    }
  }
  return result;
}
var MAX_BYTES, AGENT_RULE;
var init_agent_perms = __esm({
  "packages/core/build/agent-perms.js"() {
    "use strict";
    MAX_BYTES = 1024 * 1024;
    AGENT_RULE = /^(Agent|Task)(\([\s\S]*\))?$/;
    __name(agentSettingsFiles, "agentSettingsFiles");
    __name(agentPermission, "agentPermission");
  }
});

// packages/core/build/transcript-tail.js
import { closeSync as closeSync3, fstatSync as fstatSync3, openSync as openSync3, readSync as readSync3 } from "node:fs";
function modelOf(line) {
  let o;
  try {
    o = JSON.parse(line.toString("utf8"));
  } catch {
    return null;
  }
  if (typeof o !== "object" || o === null)
    return null;
  const rec = o;
  const model = rec.message?.model;
  return rec.type === "assistant" && typeof model === "string" && model && model !== "<synthetic>" ? model : null;
}
function lastMainModel(transcript_path) {
  if (!transcript_path)
    return null;
  try {
    const fd = openSync3(transcript_path, "r");
    try {
      const size = fstatSync3(fd).size;
      let pos2 = size;
      let carry = Buffer.alloc(0);
      while (pos2 > 0 && size - pos2 < MAX_BYTES2) {
        const start = Math.max(0, pos2 - STEP);
        const chunk = Buffer.alloc(pos2 - start);
        let got = 0;
        while (got < chunk.length) {
          const r = readSync3(fd, chunk, got, chunk.length - got, start + got);
          if (r === 0)
            break;
          got += r;
        }
        const buf = Buffer.concat([chunk.subarray(0, got), carry]);
        pos2 = start;
        let complete = buf;
        if (pos2 > 0) {
          const first = buf.indexOf(10);
          carry = first === -1 ? buf : buf.subarray(0, first);
          if (first === -1)
            continue;
          complete = buf.subarray(first + 1);
        }
        let end = complete.length;
        while (end > 0) {
          const nl = complete.lastIndexOf(10, end - 1);
          const model = modelOf(complete.subarray(nl + 1, end));
          if (model)
            return model;
          end = nl;
        }
      }
      return null;
    } finally {
      closeSync3(fd);
    }
  } catch {
    return null;
  }
}
var STEP, MAX_BYTES2;
var init_transcript_tail = __esm({
  "packages/core/build/transcript-tail.js"() {
    "use strict";
    STEP = 64 * 1024;
    MAX_BYTES2 = 1024 * 1024;
    __name(modelOf, "modelOf");
    __name(lastMainModel, "lastMainModel");
  }
});

// packages/core/build/session-model.js
import { randomUUID as randomUUID3 } from "node:crypto";
import { closeSync as closeSync4, openSync as openSync4, readFileSync as readFileSync6, readdirSync, renameSync as renameSync4, rmSync as rmSync4, statSync as statSync3, writeFileSync as writeFileSync4 } from "node:fs";
import { join as join6 } from "node:path";
function recordSessionModel(session_id, model) {
  try {
    const d = dir();
    ensureDir(d);
    const tmp = join6(d, `.tmp-${process.pid}-${randomUUID3()}`);
    try {
      writeFileSync4(tmp, JSON.stringify({ model }), PRIVATE_FILE);
      renameSync4(tmp, join6(d, `${safeName(session_id)}.json`));
    } finally {
      rmSync4(tmp, { force: true });
    }
    const cutoff = Date.now() - WEEK_MS;
    for (const f of readdirSync(d)) {
      try {
        if ((f.endsWith(".json") || f.endsWith(".polled")) && statSync3(join6(d, f)).mtimeMs < cutoff)
          rmSync4(join6(d, f), { force: true });
      } catch {
      }
    }
  } catch {
  }
}
function readSessionModel(session_id) {
  try {
    const m = JSON.parse(readFileSync6(join6(dir(), `${safeName(session_id)}.json`), "utf8")).model;
    return typeof m === "string" && m ? m : null;
  } catch {
    return null;
  }
}
function claimFirstPoll(session_id) {
  try {
    const d = dir();
    ensureDir(d);
    closeSync4(openSync4(join6(d, `${safeName(session_id)}.polled`), "wx", PRIVATE_FILE.mode));
    return true;
  } catch {
    return false;
  }
}
function noteSessionModel(transcript_path, session_id) {
  const model = lastMainModel(transcript_path);
  if (model && model.length <= 64 && modelFamily(model) !== null && readSessionModel(session_id) !== model)
    recordSessionModel(session_id, model);
}
var safeName, WEEK_MS, dir;
var init_session_model = __esm({
  "packages/core/build/session-model.js"() {
    "use strict";
    init_store();
    init_models();
    init_transcript_tail();
    safeName = /* @__PURE__ */ __name((s) => s.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 128) || "_", "safeName");
    WEEK_MS = 7 * 24 * 3600 * 1e3;
    dir = /* @__PURE__ */ __name(() => join6(horsyHome(), "sessions"), "dir");
    __name(recordSessionModel, "recordSessionModel");
    __name(readSessionModel, "readSessionModel");
    __name(claimFirstPoll, "claimFirstPoll");
    __name(noteSessionModel, "noteSessionModel");
  }
});

// packages/core/build/pending.js
import { randomUUID as randomUUID4 } from "node:crypto";
import { readFileSync as readFileSync7, readdirSync as readdirSync2, renameSync as renameSync5, rmSync as rmSync5, statSync as statSync4, utimesSync, writeFileSync as writeFileSync5 } from "node:fs";
import { join as join7 } from "node:path";
function readEntries(dir2, suffixes, raced) {
  const out = [];
  for (const file of readdirSync2(dir2)) {
    if (!/^\d+-/.test(file) || !suffixes.some((s) => file.endsWith(s)))
      continue;
    try {
      const e = JSON.parse(readFileSync7(join7(dir2, file), "utf8"));
      if (typeof e === "object" && e !== null && typeof e.task_id === "string")
        out.push({ file, entry: e });
    } catch (err) {
      if (raced && err.code === "ENOENT")
        raced.v = true;
    }
  }
  return out.sort((a, b) => tsOf(a.file) - tsOf(b.file) || (a.file < b.file ? -1 : 1));
}
function pruneDir(dir2, cutoff, tmpCutoff) {
  for (const file of readdirSync2(dir2)) {
    try {
      if (/^\d+-/.test(file) ? tsOf(file) < cutoff : file.startsWith(".tmp-") && statSync4(join7(dir2, file)).mtimeMs < tmpCutoff)
        rmSync5(join7(dir2, file), { force: true });
    } catch {
    }
  }
}
function prunePending(ref, session_id) {
  try {
    const cutoff = ref - DAY_MS;
    const tmpCutoff = ref - TMP_MAX_AGE_MS;
    if (session_id !== void 0)
      return pruneDir(sessionDir(session_id), cutoff, tmpCutoff);
    for (const d of readdirSync2(root()))
      pruneDir(join7(root(), d), cutoff, tmpCutoff);
  } catch {
  }
}
function savePending(session_id, entry2) {
  const dir2 = sessionDir(session_id);
  ensureDir(dir2);
  const tmp = join7(dir2, `.tmp-${process.pid}-${randomUUID4()}`);
  try {
    writeFileSync5(tmp, JSON.stringify({ ...entry2, subagent_type: normType(entry2.subagent_type) }), PRIVATE_FILE);
    renameSync5(tmp, join7(dir2, `${entry2.ts}-${safeName(entry2.desc_hash ?? "")}-${safeName(entry2.task_id)}.json`));
  } finally {
    rmSync5(tmp, { force: true });
  }
  try {
    if (Math.random() < PRUNE_ODDS)
      prunePending(entry2.ts);
    else if (readdirSync2(dir2).length > PRUNE_ABOVE)
      prunePending(entry2.ts, session_id);
  } catch {
  }
}
function takePending(session_id, prompt_hash, subagent_type) {
  try {
    const dir2 = sessionDir(session_id);
    const type = normType(subagent_type);
    const freshAfter = Date.now() - FALLBACK_MAX_AGE_MS;
    for (let round2 = 0; round2 < 100; round2++) {
      const raced = { v: false };
      const all = readEntries(dir2, [".json"], raced);
      const byHash = all.filter((c) => c.entry.prompt_hash === prompt_hash);
      const byType = all.filter((c) => c.entry.prompt_hash !== prompt_hash && c.entry.subagent_type === type && c.entry.ts >= freshAfter);
      const candidates = [...byHash, ...byType];
      if (candidates.length === 0) {
        if (raced.v)
          continue;
        return null;
      }
      for (const c of candidates) {
        try {
          renameSync5(join7(dir2, c.file), join7(dir2, `${c.file}.taken`));
          try {
            const t = /* @__PURE__ */ new Date();
            utimesSync(join7(dir2, `${c.file}.taken`), t, t);
          } catch {
          }
          return c.entry;
        } catch (err) {
          if (err.code !== "ENOENT")
            throw err;
        }
      }
    }
    return null;
  } catch {
    return null;
  }
}
function retryParent(session_id, desc_hash, now) {
  try {
    const dir2 = sessionDir(session_id);
    const want = safeName(desc_hash);
    const floor = now - FALLBACK_MAX_AGE_MS;
    const names = readdirSync2(dir2).filter((f) => /^\d+-/.test(f) && f.endsWith(".json.taken") && f.split("-")[1] === want && takenAt(join7(dir2, f)) >= floor).sort((a, b) => tsOf(a) - tsOf(b) || (a < b ? -1 : 1));
    for (const f of names) {
      try {
        const e = JSON.parse(readFileSync7(join7(dir2, f), "utf8"));
        if (typeof e === "object" && e !== null && typeof e.task_id === "string")
          return e.task_id;
      } catch {
      }
    }
    return null;
  } catch {
    return null;
  }
}
var DAY_MS, PRUNE_ABOVE, PRUNE_ODDS, FALLBACK_MAX_AGE_MS, TMP_MAX_AGE_MS, DEFAULT_TYPE, normType, promptHash, root, sessionDir, tsOf, takenAt;
var init_pending = __esm({
  "packages/core/build/pending.js"() {
    "use strict";
    init_repo();
    init_session_model();
    init_store();
    DAY_MS = 24 * 3600 * 1e3;
    PRUNE_ABOVE = 50;
    PRUNE_ODDS = 1 / 20;
    FALLBACK_MAX_AGE_MS = 30 * 60 * 1e3;
    TMP_MAX_AGE_MS = 3600 * 1e3;
    DEFAULT_TYPE = "general-purpose";
    normType = /* @__PURE__ */ __name((t) => t || DEFAULT_TYPE, "normType");
    promptHash = /* @__PURE__ */ __name((prompt) => hashPath(prompt), "promptHash");
    root = /* @__PURE__ */ __name(() => join7(horsyHome(), "pending"), "root");
    sessionDir = /* @__PURE__ */ __name((session_id) => join7(root(), safeName(session_id)), "sessionDir");
    tsOf = /* @__PURE__ */ __name((file) => Number.parseInt(file, 10), "tsOf");
    __name(readEntries, "readEntries");
    __name(pruneDir, "pruneDir");
    __name(prunePending, "prunePending");
    __name(savePending, "savePending");
    __name(takePending, "takePending");
    takenAt = /* @__PURE__ */ __name((file) => {
      try {
        return statSync4(file).mtimeMs;
      } catch {
        return -Infinity;
      }
    }, "takenAt");
    __name(retryParent, "retryParent");
  }
});

// packages/core/build/router.js
import { randomUUID as randomUUID5 } from "node:crypto";
import { existsSync, readFileSync as readFileSync8, realpathSync as realpathSync2 } from "node:fs";
import { join as join8 } from "node:path";
function hasTests(root2) {
  try {
    const pkg = join8(root2, "package.json");
    if (existsSync(pkg)) {
      const scripts = JSON.parse(readFileSync8(pkg, "utf8")).scripts;
      const t = scripts?.test;
      if (typeof t === "string" && t.trim() && !/no test specified/.test(t))
        return true;
    }
  } catch {
  }
  return ["tests", "test", "pyproject.toml", "Cargo.toml", "go.mod"].some((n) => existsSync(join8(root2, n)));
}
async function route(input, deps) {
  const { config, env } = deps;
  if (config.mode === "off")
    return { events: [], output: null };
  const t0 = deps.now();
  const ti = input.tool_input;
  const description = str(ti["description"]);
  const prompt = str(ti["prompt"]);
  const subagentType = typeof ti["subagent_type"] === "string" && ti["subagent_type"] ? ti["subagent_type"] : null;
  const cwdRoot = repoRoot(input.cwd);
  let root2 = cwdRoot ?? input.cwd;
  if (!cwdRoot) {
    try {
      root2 = realpathSync2(input.cwd);
    } catch {
    }
  }
  const repo_hash = hashPath(root2);
  const { task_class, features } = classify({ description, prompt, subagent_type: subagentType }, { has_tests: hasTests(cwdRoot ?? input.cwd) });
  const task_id = input.tool_use_id || randomUUID5();
  const ctx = { session_id: input.session_id, repo_hash };
  const envModel = env["CLAUDE_CODE_SUBAGENT_MODEL"];
  const force = env["CLAUDE_CODE_SUBAGENT_MODEL_FORCE"];
  const explicitRaw = str(ti["model"]).trim();
  const explicit = explicitRaw && explicitRaw !== "inherit" ? explicitRaw : null;
  const known = /* @__PURE__ */ __name((m) => m ? modelFamily(m) : null, "known");
  const mainModel = /* @__PURE__ */ __name(() => known(lastMainModel(input.transcript_path)) ?? known(readSessionModel(input.session_id)), "mainModel");
  const needsMain = !envModel && !force && !explicit;
  let main2 = needsMain ? mainModel() : null;
  if (needsMain && main2 === null && input.transcript_path && existsSync(input.transcript_path) && claimFirstPoll(input.session_id)) {
    for (let waited = 0; main2 === null && waited < FIRST_CALL_WAIT_MS; waited += POLL_MS) {
      await new Promise((r) => setTimeout(r, POLL_MS));
      const raw = lastMainModel(input.transcript_path);
      main2 = known(raw);
      if (raw && main2 !== null)
        recordSessionModel(input.session_id, raw);
    }
  }
  let defaultModel = (envModel ? known(envModel) : main2) ?? "unknown";
  if (force)
    defaultModel = modelFamily(force) ?? "custom";
  else if (explicit)
    defaultModel = modelFamily(explicit) ?? "custom";
  const default_arm = { model: defaultModel, effort: null };
  const ladder = config.ladder;
  const active = config.mode === "active";
  const cells = posteriors(deps.cellStats(), repo_hash, task_class, ladder);
  const dctx = { task_class, features, description, config, cells, rng: deps.rng, env, ...deps.fetchImpl ? { fetchImpl: deps.fetchImpl } : {} };
  const respect = force ? "force_env" : explicit ? "respect_explicit" : envModel ? "env_pin" : null;
  const routable = ROUTABLE.has(subagentType);
  let decision;
  try {
    decision = config.decider === "jev" && (respect || !routable) ? rulesDecision(dctx) : await getDecider(config.decider).decide(dctx);
  } catch {
    decision = { ...rulesDecision(dctx), reason: "decider_error" };
  }
  let armIdx = Math.min(Math.max(decision.arm_index, 0), ladder.length - 1);
  let arm = ladder[armIdx];
  const classActive = config.active_classes === "all" || config.active_classes.includes(task_class);
  const defaultRank = rankOf(default_arm.model);
  const defaultKnown = Number.isFinite(defaultRank);
  const cheaper = /* @__PURE__ */ __name((a) => defaultKnown && modelFamily(a.model) !== null && rankOf(a.model) < defaultRank, "cheaper");
  const parts = [];
  const eligible = ladder.flatMap((r, i) => cheaper(r) ? [i] : []);
  const uncheckedEdit = !features.read_only && !config.verify.repos.includes(repo_hash);
  const policyGate = !classActive ? "class_inactive" : !routable ? "custom_agent" : uncheckedEdit ? "checks_off" : !defaultKnown ? "default_unknown" : null;
  const gate = respect ? respect === "force_env" ? "force_env" : respect === "env_pin" ? "env_pin" : "explicit_model" : policyGate;
  const blocked = !active && !respect ? defaultKnown ? "shadow" : "shadow;default_unknown" : gate;
  const starterIdx = features.read_only && subagentType !== "Explore" ? ladder.findIndex((r) => modelFamily(r.model) === "sonnet" && cheaper(r)) : -1;
  const starterCell = cells[starterIdx];
  const starterOk = starterCell !== void 0 && mean(starterCell) >= config.tau;
  const resolveWith = /* @__PURE__ */ __name((block, d) => {
    if (block)
      return { idx: null, status: block, starter: false };
    const r = ladder[d];
    if (r.model === "inherit" || !cheaper(r))
      return { idx: null, status: "no_cheaper", starter: false };
    if (subagentType === "Explore" && modelFamily(r.model) !== "haiku")
      return { idx: null, status: "below_gate", starter: false };
    const cell = cells[d];
    if (cell !== void 0 && cell.n >= config.min_outcomes && mean(cell) >= config.tau)
      return { idx: d, status: "applied", starter: false };
    return starterOk ? { idx: starterIdx, status: "applied", starter: true } : { idx: null, status: "below_gate", starter: false };
  }, "resolveWith");
  const resolve3 = /* @__PURE__ */ __name((d) => resolveWith(blocked, d), "resolve");
  const would = resolveWith(policyGate, Math.min(Math.max(decision.arm_index, 0), ladder.length - 1));
  const apply_arm = would.idx === null ? null : ladder[would.idx];
  const explorable = active && !respect && routable && subagentType !== "Explore" && !uncheckedEdit && classActive && SAFE_CLASSES.includes(task_class) && eligible.length > 0 && config.explore_rate > 0;
  const eps = config.explore_rate;
  const k = eligible.length;
  let explored = false;
  if (explorable && deps.rng() < eps) {
    armIdx = eligible[Math.min(Math.floor(deps.rng() * k), k - 1)];
    arm = ladder[armIdx];
    explored = true;
  }
  const res = explored ? { idx: armIdx, status: "applied", starter: false } : resolve3(armIdx);
  let applied = false;
  let starter = false;
  let status = res.status;
  let perm = "allow";
  if (res.idx !== null) {
    try {
      perm = agentPermission(deps.permissionFiles ?? agentSettingsFiles({ env, roots: [root2, input.cwd] }));
    } catch {
      perm = "ask";
    }
    if (perm === "deny") {
      status = "permission_deny";
      if (explored) {
        explored = false;
        armIdx = Math.min(Math.max(decision.arm_index, 0), ladder.length - 1);
        arm = ladder[armIdx];
      }
    }
  }
  if (res.idx !== null && perm !== "deny") {
    armIdx = res.idx;
    arm = ladder[armIdx];
    applied = true;
    starter = res.starter;
  }
  const decided = Math.min(Math.max(decision.arm_index, 0), ladder.length - 1);
  let pNo;
  if (applied) {
    if (decision.decider === "bandit") {
      let hits = 0;
      for (let s = 0; s < 200; s++)
        if (resolve3(thompsonChoose(cells, config.tau, deps.rng)).idx === armIdx)
          hits++;
      pNo = hits / 200;
    } else
      pNo = resolve3(decided).idx === armIdx ? decision.propensity : 0;
  } else
    pNo = armIdx === decided ? decision.propensity : decision.decider === "bandit" ? propensity(cells, config.tau, armIdx, deps.rng, { explore_rate: eps, explore: false }) : 0;
  const p = Math.min(1, Math.max(1e-3, explorable ? (1 - eps) * pNo + (eligible.includes(armIdx) ? eps / k : 0) : pNo));
  if (explored)
    parts.push("explore");
  parts.push(decision.reason, status);
  if (starter)
    parts.push("starter");
  if (perm === "ask")
    parts.push("permission_ask");
  if (!active && !respect && apply_arm === null)
    parts.push(`active:${would.status}`);
  let output = null;
  let effective_type = subagentType;
  if (applied) {
    const updatedInput = { ...ti, model: arm.model };
    if (arm.effort !== null)
      updatedInput["effort"] = arm.effort;
    else
      delete updatedInput["effort"];
    if (features.read_only) {
      updatedInput["subagent_type"] = "Explore";
      effective_type = "Explore";
      parts.push("explore_guard");
    }
    output = { hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: perm === "ask" ? "ask" : "allow", updatedInput } };
  }
  const parent = retryParent(input.session_id, features.desc_hash, deps.now());
  const events = [
    makeEvent({ type: "task_created", task_id, task_class, features, parent_task_id: parent }, ctx),
    makeEvent({
      type: "route_decision",
      task_id,
      arm,
      apply_arm,
      default_arm,
      propensity: p,
      mode: config.mode,
      decider: respect ?? decision.decider,
      applied,
      reason: parts.join(";"),
      features,
      hook_ms: Math.max(0, Math.round(deps.now() - t0))
    }, ctx)
  ];
  if (respect) {
    events.push(makeEvent({ type: "override", task_id, kind: respect === "force_env" ? "force_env" : respect === "env_pin" ? "env_pin" : "explicit_model", from: arm.model, to: default_arm.model }, ctx));
  }
  try {
    savePending(input.session_id, {
      prompt_hash: promptHash(prompt),
      task_id,
      task_class,
      subagent_type: effective_type,
      arm: applied ? arm : default_arm,
      ts: deps.now(),
      desc_hash: features.desc_hash
    });
  } catch {
  }
  return { events, output };
}
var FIRST_CALL_WAIT_MS, POLL_MS, ROUTABLE, str, rankOf;
var init_router = __esm({
  "packages/core/build/router.js"() {
    "use strict";
    init_agent_perms();
    init_bandit();
    init_classify();
    init_deciders();
    init_rules();
    init_events();
    init_models();
    init_pending();
    init_repo();
    init_session_model();
    init_transcript_tail();
    FIRST_CALL_WAIT_MS = 200;
    POLL_MS = 25;
    ROUTABLE = /* @__PURE__ */ new Set([null, "general-purpose", "Explore"]);
    __name(hasTests, "hasTests");
    str = /* @__PURE__ */ __name((v2) => typeof v2 === "string" ? v2 : "", "str");
    rankOf = /* @__PURE__ */ __name((model) => FAMILY_RANK[modelFamily(model) ?? ""] ?? Infinity, "rankOf");
    __name(route, "route");
  }
});

// packages/core/build/verify/detect.js
import { execFileSync as execFileSync2 } from "node:child_process";
import { existsSync as existsSync2, readFileSync as readFileSync9, readdirSync as readdirSync3 } from "node:fs";
import { delimiter, dirname, join as join9, sep } from "node:path";
function scriptsOf(pkg) {
  if (typeof pkg !== "object" || pkg === null)
    return null;
  const scripts = pkg.scripts ?? {};
  return typeof scripts === "object" && scripts !== null ? scripts : null;
}
function readWorkingScripts(root2) {
  try {
    return scriptsOf(JSON.parse(readFileSync9(join9(root2, "package.json"), "utf8")));
  } catch {
    return null;
  }
}
function readHeadScripts(top, rel) {
  try {
    const out = execFileSync2("git", ["show", headPath(rel, "package.json")], {
      cwd: top,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 2e3,
      env: { ...process.env, LC_ALL: "C" }
    });
    const scripts = scriptsOf(JSON.parse(out));
    return scripts ? { kind: "head", scripts } : { kind: "error" };
  } catch (err) {
    const e = err;
    const stderr = String(e.stderr ?? "");
    if (typeof e.status !== "number" || !ABSENT_AT_HEAD.test(stderr))
      return { kind: "error" };
    return /not a git repository/i.test(stderr) ? { kind: "nonrepo" } : { kind: "absent" };
  }
}
function headFileState(top, rel) {
  const git2 = /* @__PURE__ */ __name((args) => execFileSync2("git", args, { cwd: top, stdio: ["ignore", "pipe", "ignore"], timeout: 2e3, maxBuffer: 64 * 1024 * 1024 }), "git");
  const at = /* @__PURE__ */ __name((f) => rel ? `${rel}/${f}` : f, "at");
  try {
    const names = new Set(git2(["ls-tree", "--name-only", "HEAD", ...rel ? [`${rel}/`] : []]).toString("utf8").split("\n").filter(Boolean));
    const changed = [];
    for (const f of PM_CONFIG_FILES) {
      const inWork = existsSync2(join9(top, rel, f));
      const inHead = names.has(at(f));
      if (inWork !== inHead || inWork && !git2(["show", headPath(rel, f)]).equals(readFileSync9(join9(top, rel, f))))
        changed.push(f);
    }
    return { names, changed };
  } catch {
    return null;
  }
}
function packageAt(top, rel, out) {
  const head = readHeadScripts(top, rel);
  if (head.kind === "error")
    return "error";
  if (head.kind === "absent") {
    if (existsSync2(join9(top, rel, "package.json")))
      out.unreviewed = true;
    return "none";
  }
  const scripts = head.kind === "head" ? head.scripts : readWorkingScripts(join9(top, rel));
  return scripts && hasScript(scripts) ? { rel, scripts, fromHead: head.kind === "head" } : "none";
}
function packageChecks(top, pkg, out) {
  const work = readWorkingScripts(join9(top, pkg.rel));
  if (pkg.fromHead) {
    const state = headFileState(top, pkg.rel);
    if (!state)
      return;
    out.modified.push(...state.changed);
  }
  for (const name of SCRIPT_ORDER) {
    const body = pkg.scripts[name];
    if (typeof body !== "string" || body.includes("no test specified"))
      continue;
    if (!work || [name, `pre${name}`, `post${name}`].some((k) => work[k] !== pkg.scripts[k])) {
      out.modified.push(name);
      continue;
    }
    const cwd = join9(top, pkg.rel);
    out.checks.push(pkg.rel ? { name: `${name}@${hashPath(pkg.rel).slice(0, 6)}`, cmd: "/bin/sh", args: ["-c", body], cwd, binRoot: top, where: pkg.rel } : { name, cmd: "/bin/sh", args: ["-c", body], cwd, binRoot: top });
  }
  if (work) {
    for (const k of /* @__PURE__ */ new Set([...Object.keys(pkg.scripts), ...Object.keys(work)])) {
      if (work[k] !== pkg.scripts[k] && !out.modified.includes(k))
        out.modified.push(k);
    }
  }
}
function dirsUp(file) {
  const out = [];
  for (let d = dirname(file.split(sep).join("/")); d !== "." && d !== "/" && d !== ""; d = dirname(d))
    out.push(d);
  out.push("");
  return out;
}
function nearestPackage(top, file, cache, out) {
  for (const rel of dirsUp(file)) {
    let f = cache.get(rel);
    if (f === void 0) {
      f = packageAt(top, rel, out);
      cache.set(rel, f);
    }
    if (f === "error")
      return null;
    if (f !== "none")
      return f;
  }
  return null;
}
function hasPytest(root2) {
  if (existsSync2(join9(root2, "pytest.ini")))
    return true;
  try {
    if (readFileSync9(join9(root2, "pyproject.toml"), "utf8").includes("[tool.pytest"))
      return true;
  } catch {
  }
  try {
    return readdirSync3(join9(root2, "tests")).some((f) => /^test_.*\.py$/.test(f));
  } catch {
    return false;
  }
}
function pickPython(pathEnv = process.env["PATH"] ?? "") {
  const dirs = pathEnv.split(delimiter).filter(Boolean);
  for (const name of ["python3", "python"]) {
    if (dirs.some((d) => existsSync2(join9(d, name))))
      return name;
  }
  return "python3";
}
function otherChecks(root2, checks) {
  if (hasPytest(root2))
    checks.push({ name: "pytest", cmd: pickPython(), args: ["-m", "pytest", "-q", "-x"], cwd: root2 });
  if (existsSync2(join9(root2, "Cargo.toml")))
    checks.push({ name: "cargo", cmd: "cargo", args: ["test", "--quiet"], cwd: root2 });
  if (existsSync2(join9(root2, "go.mod")))
    checks.push({ name: "go", cmd: "go", args: ["test", "./..."], cwd: root2 });
}
function detectChecksForFiles(top, files) {
  const out = { checks: [], modified: [], unreviewed: false };
  const cache = /* @__PURE__ */ new Map();
  const pkgs = /* @__PURE__ */ new Map();
  for (const f of files.length > 0 ? files : ["package.json"]) {
    const p = nearestPackage(top, f, cache, out);
    if (p)
      pkgs.set(p.rel, p);
  }
  if ([...pkgs.values()].some((p) => p.fromHead)) {
    const state = headFileState(top, "");
    if (state) {
      if ([...pkgs.keys()].some((rel) => rel !== "")) {
        for (const f of state.changed)
          if (!out.modified.includes(f))
            out.modified.push(f);
      }
    } else
      pkgs.clear();
  }
  for (const p of [...pkgs.values()].sort((x, y) => x.rel.length - y.rel.length || x.rel.localeCompare(y.rel)))
    packageChecks(top, p, out);
  otherChecks(top, out.checks);
  return out;
}
function detectChecksDetailed(root2) {
  return detectChecksForFiles(root2, []);
}
var SCRIPT_ORDER, ABSENT_AT_HEAD, headPath, PM_CONFIG_FILES, hasScript;
var init_detect = __esm({
  "packages/core/build/verify/detect.js"() {
    "use strict";
    init_repo();
    SCRIPT_ORDER = ["typecheck", "lint", "test"];
    ABSENT_AT_HEAD = /not a git repository|invalid object name 'HEAD'|does not exist in 'HEAD'|exists on disk, but not in 'HEAD'|unknown revision/i;
    __name(scriptsOf, "scriptsOf");
    __name(readWorkingScripts, "readWorkingScripts");
    headPath = /* @__PURE__ */ __name((rel, file) => rel ? `HEAD:${rel}/${file}` : `HEAD:./${file}`, "headPath");
    __name(readHeadScripts, "readHeadScripts");
    PM_CONFIG_FILES = [
      ".npmrc",
      ".yarnrc",
      ".yarnrc.yml",
      ".pnpmfile.cjs",
      "bunfig.toml",
      "package-lock.json",
      "pnpm-lock.yaml",
      "yarn.lock",
      "bun.lock",
      "bun.lockb"
    ];
    __name(headFileState, "headFileState");
    hasScript = /* @__PURE__ */ __name((scripts) => SCRIPT_ORDER.some((n) => typeof scripts[n] === "string" && !scripts[n].includes("no test specified")), "hasScript");
    __name(packageAt, "packageAt");
    __name(packageChecks, "packageChecks");
    __name(dirsUp, "dirsUp");
    __name(nearestPackage, "nearestPackage");
    __name(hasPytest, "hasPytest");
    __name(pickPython, "pickPython");
    __name(otherChecks, "otherChecks");
    __name(detectChecksForFiles, "detectChecksForFiles");
    __name(detectChecksDetailed, "detectChecksDetailed");
  }
});

// packages/core/build/verify/run.js
import { spawn } from "node:child_process";
import { existsSync as existsSync3 } from "node:fs";
import { constants } from "node:os";
import { delimiter as delimiter2, dirname as dirname2, join as join10, relative, sep as sep2 } from "node:path";
function childEnv(passthrough = []) {
  const env = {};
  for (const [k, v2] of Object.entries(process.env)) {
    if (SECRET_VARS.has(k))
      continue;
    if (ENV_NAMES.has(k) || passthrough.includes(k) || ENV_PREFIXES.some((p) => k.startsWith(p)) && !CREDENTIAL_LIKE.test(k) && !URL_USERINFO.test(v2 ?? ""))
      env[k] = v2;
  }
  return {
    ...env,
    CI: "1",
    FORCE_COLOR: "0",
    npm_config_ignore_scripts: "true",
    YARN_ENABLE_SCRIPTS: "false",
    pnpm_config_enable_pre_post_scripts: "false",
    // pnpm would otherwise install (and run the repo's install hooks) before `run` when node_modules is missing from the copy.
    pnpm_config_verify_deps_before_run: "false"
  };
}
function killGroups() {
  for (const pid of groups) {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
    }
  }
  groups.clear();
}
function onSignal(sig) {
  killGroups();
  untrackHandlers();
  if (process.listenerCount(sig) === 0)
    process.exit(128 + (constants.signals[sig] ?? 0));
}
function trackHandlers() {
  if (signalHandlers.size > 0)
    return;
  process.on("exit", killGroups);
  for (const sig of SIGNALS) {
    const h = /* @__PURE__ */ __name(() => onSignal(sig), "h");
    signalHandlers.set(sig, h);
    process.on(sig, h);
  }
}
function untrackHandlers() {
  process.off("exit", killGroups);
  for (const [sig, h] of signalHandlers)
    process.off(sig, h);
  signalHandlers.clear();
}
function binDirs(cwd, top) {
  const out = [];
  for (let d = cwd; ; d = dirname2(d)) {
    const rel = relative(top, d);
    if (rel === ".." || rel.startsWith(`..${sep2}`))
      break;
    const bin = join10(d, "node_modules", ".bin");
    if (existsSync3(bin))
      out.push(bin);
    if (d === top || dirname2(d) === d)
      break;
  }
  return out;
}
function timedOut(name, duration_ms = 0, tail = "") {
  return { name, exit_code: null, duration_ms, timed_out: true, tail };
}
function runOne(check, deadline, opts) {
  const remaining = deadline - Date.now();
  if (remaining <= 0)
    return Promise.resolve(timedOut(check.name));
  if (opts.signal?.aborted)
    return Promise.resolve({ name: check.name, exit_code: null, duration_ms: 0, timed_out: false, tail: "" });
  const start = Date.now();
  return new Promise((resolve3) => {
    let tail = "";
    let done = false;
    const finish = /* @__PURE__ */ __name((run2) => {
      if (done)
        return;
      done = true;
      if (child.pid !== void 0)
        untrack(child.pid);
      clearTimeout(timer);
      opts.signal?.removeEventListener("abort", onAbort);
      resolve3({ name: check.name, duration_ms: Date.now() - start, tail, ...run2 });
    }, "finish");
    const env = childEnv(opts.passthrough);
    if (check.binRoot)
      env["PATH"] = [...binDirs(check.cwd, check.binRoot), ...env["PATH"] ? [env["PATH"]] : []].join(delimiter2);
    const child = spawn(check.cmd, check.args, { cwd: check.cwd, detached: true, stdio: ["ignore", "pipe", "pipe"], env });
    if (child.pid !== void 0)
      track(child.pid);
    const collect = /* @__PURE__ */ __name((chunk) => {
      tail = (tail + chunk).slice(-TAIL_CHARS);
    }, "collect");
    for (const stream of [child.stdout, child.stderr]) {
      stream.setEncoding("utf8");
      stream.on("data", collect);
    }
    const killGroup = /* @__PURE__ */ __name(() => {
      try {
        if (child.pid !== void 0)
          process.kill(-child.pid, "SIGKILL");
      } catch {
      }
    }, "killGroup");
    const onAbort = /* @__PURE__ */ __name(() => {
      killGroup();
      child.stdout.destroy();
      child.stderr.destroy();
      finish({ exit_code: null, timed_out: false });
    }, "onAbort");
    opts.signal?.addEventListener("abort", onAbort, { once: true });
    const timer = setTimeout(() => {
      killGroup();
      child.stdout.destroy();
      child.stderr.destroy();
      finish({ exit_code: null, timed_out: true });
    }, remaining);
    child.on("error", (err) => {
      collect(String(err.message));
      finish({ exit_code: err.code === "ENOENT" ? 127 : 126, timed_out: false });
    });
    child.on("exit", (code, signal) => {
      const exit_code = code ?? (signal ? 128 : 1);
      killGroup();
      child.on("close", () => finish({ exit_code, timed_out: false }));
      setTimeout(() => {
        child.stdout.destroy();
        child.stderr.destroy();
        finish({ exit_code, timed_out: false });
      }, 200).unref();
    });
  });
}
async function runChecks(checks, deadline, opts = {}) {
  const runs = [];
  for (const check of checks)
    runs.push(await runOne(check, deadline, opts));
  return runs;
}
async function judge(checks, deadline, baseline, modified = [], unreviewed = false, opts = {}) {
  if (modified.length > 0)
    return { result: "no_signal", reason: "scripts_modified", runs: [] };
  if (checks.length === 0)
    return { result: "no_signal", reason: unreviewed ? "scripts_unreviewed" : "no_checks", runs: [] };
  const ended = /* @__PURE__ */ __name(() => ({ result: "no_signal", reason: "session_ended", runs: [] }), "ended");
  let runs = await runChecks(checks, deadline, opts);
  if (opts.signal?.aborted)
    return ended();
  const failed = /* @__PURE__ */ __name((rs) => rs.filter((r) => r.exit_code !== 0), "failed");
  if (failed(runs).length === 0)
    return { result: "pass", reason: null, runs };
  if (runs.some((r) => r.timed_out))
    return { result: "no_signal", reason: "timeout", runs };
  const firstFailed = new Set(failed(runs).map((r) => r.name));
  const reruns = await runChecks(checks.filter((c) => firstFailed.has(c.name)), deadline, opts);
  if (opts.signal?.aborted)
    return ended();
  runs = runs.map((r) => reruns.find((x) => x.name === r.name) ?? r);
  if (reruns.some((r) => r.timed_out))
    return { result: "no_signal", reason: "timeout", runs };
  const still = failed(reruns).map((r) => r.name);
  if (still.length === 0)
    return { result: "no_signal", reason: "flaky", runs };
  const base = await baseline(still);
  if (opts.signal?.aborted)
    return ended();
  if (base === null)
    return { result: "no_signal", reason: "baseline_unavailable", runs };
  if (base === "uncertain")
    return { result: "no_signal", reason: "baseline_uncertain", runs };
  if (still.every((n) => base[n] === false))
    return { result: "no_signal", reason: "baseline_red", runs };
  return { result: "fail", reason: `failed: ${still.filter((n) => base[n] !== false).join(", ")}`, runs };
}
var TAIL_CHARS, ENV_NAMES, ENV_PREFIXES, SECRET_VARS, CREDENTIAL_LIKE, URL_USERINFO, groups, SIGNALS, signalHandlers, track, untrack;
var init_run = __esm({
  "packages/core/build/verify/run.js"() {
    "use strict";
    TAIL_CHARS = 1500;
    ENV_NAMES = /* @__PURE__ */ new Set(["PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP", "LANG", "TERM", "TZ"]);
    ENV_PREFIXES = [
      "LC_",
      "npm_config_",
      "PNPM_",
      "COREPACK_",
      "NVM_",
      "VOLTA_",
      "FNM_",
      "YARN_",
      "ASDF_",
      "MISE_",
      "PYENV_",
      "VIRTUAL_ENV",
      "CONDA_",
      "CARGO_",
      "RUSTUP_",
      "GOPATH",
      "GOROOT",
      "GOFLAGS",
      "JAVA_HOME"
    ];
    SECRET_VARS = /* @__PURE__ */ new Set(["HORSY_JEV_KEY", ["TYPESAFE", "API", "KEY"].join("_")]);
    CREDENTIAL_LIKE = /TOKEN|SECRET|PASSW|AUTH|CREDENTIAL|_KEY|[/:]/i;
    URL_USERINFO = /:\/\/[^/@\s]*@/;
    __name(childEnv, "childEnv");
    groups = /* @__PURE__ */ new Set();
    SIGNALS = ["SIGTERM", "SIGHUP", "SIGINT"];
    __name(killGroups, "killGroups");
    __name(onSignal, "onSignal");
    signalHandlers = /* @__PURE__ */ new Map();
    __name(trackHandlers, "trackHandlers");
    __name(untrackHandlers, "untrackHandlers");
    track = /* @__PURE__ */ __name((pid) => {
      groups.add(pid);
      trackHandlers();
    }, "track");
    untrack = /* @__PURE__ */ __name((pid) => {
      groups.delete(pid);
      if (groups.size === 0)
        untrackHandlers();
    }, "untrack");
    __name(binDirs, "binDirs");
    __name(timedOut, "timedOut");
    __name(runOne, "runOne");
    __name(runChecks, "runChecks");
    __name(judge, "judge");
  }
});

// packages/core/build/verify/isolate.js
import { execFileSync as execFileSync3 } from "node:child_process";
import { copyFileSync, existsSync as existsSync4, lstatSync as lstatSync2, mkdirSync as mkdirSync2, mkdtempSync, readdirSync as readdirSync4, readlinkSync, rmSync as rmSync6, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname as dirname3, isAbsolute, join as join11, normalize, sep as sep3 } from "node:path";
function linkStaysInside(name, target) {
  if (isAbsolute(target))
    return false;
  const resolved = normalize(join11(dirname3(name), target));
  return resolved !== ".." && !resolved.startsWith(`..${sep3}`);
}
function sweepOldCopies(now = Date.now()) {
  try {
    for (const f of readdirSync4(tmpdir())) {
      if (!f.startsWith("horsy-verify-"))
        continue;
      try {
        if (now - lstatSync2(join11(tmpdir(), f)).mtimeMs > DAY_MS2)
          rmSync6(join11(tmpdir(), f), { recursive: true, force: true });
      } catch {
      }
    }
  } catch {
  }
}
function makeCopy(top, pkgRels, limits = DEFAULT_LIMITS) {
  let names;
  try {
    const out = execFileSync3("git", ["ls-files", "-z", "-co", "--exclude-standard"], { cwd: top, encoding: "utf8", timeout: 1e4, maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "ignore"] });
    names = [...new Set(out.split("\0").filter(Boolean))];
  } catch (err) {
    return { skip: err.code === "ENOBUFS" ? "repo_too_large" : "no_git" };
  }
  if (names.length > limits.maxFiles)
    return { skip: "repo_too_large" };
  const files = [];
  const links = [];
  let bytes = 0;
  for (const name of names) {
    let st;
    try {
      st = lstatSync2(join11(top, name));
    } catch {
      continue;
    }
    if (st.isSymbolicLink()) {
      const target = readlinkSync(join11(top, name));
      if (linkStaysInside(name, target))
        links.push([name, target]);
    } else if (st.isFile()) {
      bytes += st.size;
      if (bytes > limits.maxBytes)
        return { skip: "repo_too_large" };
      files.push(name);
    }
  }
  const dir2 = mkdtempSync(join11(tmpdir(), "horsy-verify-"));
  const cleanup = /* @__PURE__ */ __name(() => {
    process.off("exit", cleanup);
    rmSync6(dir2, { recursive: true, force: true });
  }, "cleanup");
  process.on("exit", cleanup);
  try {
    const made = /* @__PURE__ */ new Set();
    const parent = /* @__PURE__ */ __name((name) => {
      const d = dirname3(join11(dir2, name));
      if (!made.has(d)) {
        mkdirSync2(d, { recursive: true });
        made.add(d);
      }
    }, "parent");
    for (const name of files) {
      parent(name);
      copyFileSync(join11(top, name), join11(dir2, name));
    }
    for (const [name, target] of links) {
      parent(name);
      symlinkSync(target, join11(dir2, name));
    }
    for (const rel of /* @__PURE__ */ new Set(["", ...pkgRels])) {
      const src = join11(top, rel, "node_modules");
      const dst = join11(dir2, rel, "node_modules");
      if (existsSync4(src) && !existsSync4(dst)) {
        mkdirSync2(dirname3(dst), { recursive: true });
        symlinkSync(src, dst, "dir");
      }
    }
  } catch (err) {
    cleanup();
    throw err;
  }
  return { dir: dir2, cleanup };
}
var DEFAULT_LIMITS, DAY_MS2;
var init_isolate = __esm({
  "packages/core/build/verify/isolate.js"() {
    "use strict";
    DEFAULT_LIMITS = { maxFiles: 2e4, maxBytes: 300 * 1024 * 1024 };
    __name(linkStaysInside, "linkStaysInside");
    DAY_MS2 = 24 * 3600 * 1e3;
    __name(sweepOldCopies, "sweepOldCopies");
    __name(makeCopy, "makeCopy");
  }
});

// packages/core/build/verify/baseline.js
import { execFileSync as execFileSync4 } from "node:child_process";
import { lstatSync as lstatSync3, mkdirSync as mkdirSync3, readFileSync as readFileSync10, realpathSync as realpathSync3, renameSync as renameSync6, rmSync as rmSync7, symlinkSync as symlinkSync2, writeFileSync as writeFileSync6 } from "node:fs";
import { dirname as dirname4, isAbsolute as isAbsolute2, join as join12, relative as relative2, sep as sep4 } from "node:path";
function readCache2(file) {
  try {
    const raw = JSON.parse(readFileSync10(file, "utf8"));
    return typeof raw === "object" && raw !== null && !Array.isArray(raw) ? raw : {};
  } catch {
    return {};
  }
}
function writeCache(file, cache) {
  ensureHome();
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync6(tmp, JSON.stringify(cache), PRIVATE_FILE);
  renameSync6(tmp, file);
}
function dirtyFiles(top) {
  const out = /* @__PURE__ */ new Set();
  for (const args of [["diff", "--name-only", "-z", "HEAD"], ["ls-files", "-o", "--exclude-standard", "-z"]]) {
    for (const f of gitOut(top, args).toString("utf8").split("\0"))
      if (f)
        out.add(f);
  }
  return [...out];
}
function undoEdit(content, e) {
  if (e.all || e.new === "")
    return void 0;
  const parts = content.split(e.new);
  return parts.length === 2 ? parts.join(e.old) : void 0;
}
function undoPatch(content, patch) {
  const lines = content.split("\n");
  for (const h of [...patch].sort((a, b) => b.newStart - a.newStart)) {
    const body = h.lines.filter((l) => !l.startsWith("\\"));
    const fresh = body.filter((l) => l[0] === " " || l[0] === "+").map((l) => l.slice(1));
    const old = body.filter((l) => l[0] === " " || l[0] === "-").map((l) => l.slice(1));
    const at = h.newLines === 0 ? h.newStart : h.newStart - 1;
    if (at < 0 || fresh.length !== h.newLines || fresh.some((l, i) => lines[at + i] !== l))
      return void 0;
    lines.splice(at, fresh.length, ...old);
  }
  return lines.join("\n");
}
function contentBefore(current, chain) {
  let state = current;
  for (let i = chain.length - 1; i >= 0; i--) {
    const c = chain[i];
    if (c.before !== void 0) {
      state = c.before;
      continue;
    }
    if (typeof state !== "string") {
      state = void 0;
      continue;
    }
    let back = c.patch && c.patch.length > 0 ? undoPatch(state, c.patch) : void 0;
    if (back === void 0 && c.edits !== null) {
      back = state;
      for (let j = c.edits.length - 1; j >= 0 && back !== void 0; j--)
        back = undoEdit(back, c.edits[j]);
    }
    state = back;
  }
  return state;
}
function restoreHead(top, copy, files) {
  const head = /* @__PURE__ */ new Map();
  if (files.length > 0) {
    for (const rec of gitOut(top, ["ls-tree", "-r", "-z", "HEAD", "--", ...files]).toString("utf8").split("\0")) {
      const m = /^(\d+) \w+ ([0-9a-f]+)\t(.*)$/s.exec(rec);
      if (m)
        head.set(m[3], { mode: m[1], sha: m[2] });
    }
  }
  const realCopy = realpathSync3(copy);
  for (const f of files) {
    const dest = join12(copy, f);
    const rel = relative2(copy, dest);
    if (rel === "" || rel === ".." || rel.startsWith(`..${sep4}`) || isAbsolute2(rel))
      throw new Error("path escapes the copy");
    if (f.split(/[\\/]/).includes("node_modules") || !insideCopy(realCopy, dirname4(dest)))
      continue;
    const h = head.get(f);
    rmSync7(dest, { force: true });
    if (!h || h.mode === "160000")
      continue;
    const body = gitOut(top, ["cat-file", "blob", h.sha]);
    mkdirSync3(dirname4(dest), { recursive: true });
    if (h.mode === "120000") {
      const target = body.toString("utf8");
      if (linkStaysInside(f, target))
        symlinkSync2(target, dest);
    } else
      writeFileSync6(dest, body, { mode: h.mode === "100755" ? 493 : 420 });
  }
}
function priorStates(top, files, changes) {
  const out = /* @__PURE__ */ new Map();
  for (const f of files) {
    if (skipped(f))
      continue;
    const chain = changes.get(f);
    if (!chain || chain.length === 0)
      return void 0;
    let current = null;
    try {
      const abs = join12(top, f);
      if (!lstatSync3(abs).isFile())
        return void 0;
      current = readFileSync10(abs, "utf8");
    } catch (err) {
      if (err.code !== "ENOENT")
        return void 0;
    }
    const before = contentBefore(current, chain);
    if (before === void 0)
      return void 0;
    out.set(f, before);
  }
  return out;
}
function sameAsHead(top, f, state) {
  let head = null;
  try {
    head = gitOut(top, ["cat-file", "blob", `HEAD:${f}`]).toString("utf8");
  } catch {
  }
  return head === state;
}
function restoreStates(copy, states) {
  const realCopy = realpathSync3(copy);
  for (const [f, body] of states) {
    const dest = join12(copy, f);
    const rel = relative2(copy, dest);
    if (rel === "" || rel === ".." || rel.startsWith(`..${sep4}`) || isAbsolute2(rel))
      throw new Error("path escapes the copy");
    if (!insideCopy(realCopy, dirname4(dest)))
      continue;
    let mode = 420;
    try {
      mode = lstatSync3(dest).mode & 511;
    } catch {
    }
    rmSync7(dest, { force: true });
    if (body === null)
      continue;
    mkdirSync3(dirname4(dest), { recursive: true });
    writeFileSync6(dest, body, { mode });
  }
}
function insideCopy(realCopy, dir2) {
  for (let d = dir2; ; d = dirname4(d)) {
    try {
      const real = realpathSync3(d);
      return real === realCopy || real.startsWith(realCopy + sep4);
    } catch {
      if (dirname4(d) === d)
        return false;
    }
  }
}
async function headBaseline(root2, checks, deadline, opts = {}) {
  const top = repoRoot(root2);
  if (!top)
    return null;
  let sha;
  try {
    sha = execFileSync4("git", ["rev-parse", "--verify", "HEAD"], { cwd: top, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 2e3 }).trim();
  } catch {
    return null;
  }
  let restore;
  let atHead;
  let prior;
  try {
    const dirty = dirtyFiles(top);
    restore = opts.files ?? dirty;
    if (opts.changes) {
      prior = priorStates(top, restore, opts.changes);
      if (!prior)
        return UNCERTAIN;
      const states = prior;
      atHead = dirty.every((f) => skipped(f) || states.has(f) && sameAsHead(top, f, states.get(f) ?? null));
    } else
      atHead = dirty.every((f) => restore.includes(f));
  } catch {
    return null;
  }
  const cacheFile = join12(horsyHome(), "baseline.json");
  const cache = readCache2(cacheFile);
  const known = atHead ? cache[sha] ?? {} : {};
  const result = {};
  const todo = [];
  for (const c of checks) {
    if (known[c.name] === true)
      result[c.name] = true;
    else
      todo.push(c);
  }
  if (todo.length === 0)
    return result;
  const rels = /* @__PURE__ */ new Map();
  for (const c of todo) {
    let rel;
    try {
      rel = relative2(top, realpathSync3(c.cwd));
    } catch {
      return null;
    }
    if (rel === ".." || rel.startsWith(`..${sep4}`) || isAbsolute2(rel))
      return null;
    rels.set(c, rel);
  }
  let copy = null;
  try {
    copy = makeCopy(top, [...rels.values()], opts.limits);
    if ("skip" in copy)
      return null;
    if (prior)
      restoreStates(copy.dir, prior);
    else
      restoreHead(top, copy.dir, restore);
    const dir2 = copy.dir;
    const runs = await runChecks(todo.map((c) => ({ ...c, cwd: join12(dir2, rels.get(c) ?? ""), binRoot: dir2 })), deadline, opts);
    if (runs.some((r) => r.timed_out) || opts.signal?.aborted)
      return null;
    for (const r of runs)
      result[r.name] = r.exit_code === 0;
    const green = Object.fromEntries(runs.filter((r) => r.exit_code === 0).map((r) => [r.name, true]));
    if (atHead && Object.keys(green).length > 0) {
      try {
        const next = { ...cache };
        delete next[sha];
        next[sha] = { ...known, ...green };
        writeCache(cacheFile, Object.fromEntries(Object.entries(next).slice(-MAX_CACHED_SHAS)));
      } catch {
      }
    }
    return result;
  } catch {
    return null;
  } finally {
    if (copy && !("skip" in copy))
      copy.cleanup();
  }
}
var MAX_CACHED_SHAS, gitOut, UNCERTAIN, skipped;
var init_baseline = __esm({
  "packages/core/build/verify/baseline.js"() {
    "use strict";
    init_repo();
    init_store();
    init_isolate();
    init_run();
    MAX_CACHED_SHAS = 200;
    __name(readCache2, "readCache");
    __name(writeCache, "writeCache");
    gitOut = /* @__PURE__ */ __name((top, args) => execFileSync4("git", args, { cwd: top, stdio: ["ignore", "pipe", "ignore"], timeout: 1e4, maxBuffer: 64 * 1024 * 1024 }), "gitOut");
    __name(dirtyFiles, "dirtyFiles");
    UNCERTAIN = "uncertain";
    __name(undoEdit, "undoEdit");
    __name(undoPatch, "undoPatch");
    __name(contentBefore, "contentBefore");
    __name(restoreHead, "restoreHead");
    skipped = /* @__PURE__ */ __name((f) => f.split(/[\\/]/).includes("node_modules"), "skipped");
    __name(priorStates, "priorStates");
    __name(sameAsHead, "sameAsHead");
    __name(restoreStates, "restoreStates");
    __name(insideCopy, "insideCopy");
    __name(headBaseline, "headBaseline");
  }
});

// packages/core/build/receipt.js
import { randomUUID as randomUUID6 } from "node:crypto";
import { readFileSync as readFileSync11, renameSync as renameSync7, rmSync as rmSync8, writeFileSync as writeFileSync7 } from "node:fs";
import { join as join13 } from "node:path";
function repriced(model, usage) {
  const fam = modelFamily(model);
  if (!fam)
    return null;
  return costOf(CURRENT[fam === "mythos" ? "fable" : fam], usage, { aggregate: true });
}
function buildReceipt(events, session_id, opts = {}) {
  const parents = /* @__PURE__ */ new Map();
  const decisions = /* @__PURE__ */ new Map();
  for (const e of events) {
    if (e.type === "task_created")
      parents.set(e.task_id, e.parent_task_id);
    else if (e.type === "route_decision" && e.session_id === session_id)
      decisions.set(e.task_id, e);
  }
  const root2 = /* @__PURE__ */ __name((id) => {
    const seen = /* @__PURE__ */ new Set();
    let cur = id;
    for (let p = parents.get(cur); p && !seen.has(p); p = parents.get(cur)) {
      seen.add(cur);
      cur = p;
    }
    return cur;
  }, "root");
  const r = {
    session_id,
    dispatched: 0,
    finished: 0,
    unknown: 0,
    explicit: 0,
    pinned: 0,
    forced: 0,
    unpriced_tasks: 0,
    tasks: 0,
    by_model: {},
    actual_cost: 0,
    default_cost: 0,
    net: 0,
    shadow_saving: 0,
    shadow_pick_saving: 0,
    shadow_pick_model: null,
    pinned_saving: 0,
    checks_passed: 0,
    checks_failed: 0,
    no_signal: 0,
    blocked: 0,
    check_runs: 0,
    unverified: 0,
    failing_checks: [],
    no_result: 0,
    said_unfinished: 0,
    reattempt_cost: 0,
    check_ms: 0,
    readonly_completed: 0,
    no_signal_edit: 0,
    baseline_unknown_tasks: 0,
    time_ms: 0,
    hook_ms: 0,
    jev_cost: 0,
    mode: opts.mode ?? "shadow"
  };
  const tasks = /* @__PURE__ */ new Set();
  const dispatched = /* @__PURE__ */ new Set();
  const explicit = /* @__PURE__ */ new Set();
  const pinned = /* @__PURE__ */ new Set();
  const forced = /* @__PURE__ */ new Set();
  const unpriced = /* @__PURE__ */ new Set();
  const namedFailures = /* @__PURE__ */ new Set();
  const unknownBaseline = /* @__PURE__ */ new Set();
  const blockedAgents = /* @__PURE__ */ new Set();
  const noResult = /* @__PURE__ */ new Set();
  const saidUnfinished = /* @__PURE__ */ new Set();
  let anon = 0;
  for (const e of events) {
    if (e.session_id !== session_id)
      continue;
    if (e.type === "route_decision") {
      r.hook_ms += e.hook_ms;
      r.mode = e.mode;
      const key = root2(e.task_id);
      dispatched.add(key);
      if (e.decider === "respect_explicit")
        explicit.add(key);
      else if (e.decider === "env_pin")
        pinned.add(key);
      else if (e.decider === "force_env")
        forced.add(key);
    } else if (e.type === "attempt") {
      const actual = e.cost_usd ?? repriced(e.arm.model, e.usage) ?? 0;
      const d = e.task_id === null ? void 0 : decisions.get(e.task_id);
      const ran = repriced(e.arm.model, e.usage);
      let dflt = null;
      let baselineUnknown = false;
      if (d?.applied && ran !== null) {
        let defaultModel = d.default_arm.model;
        if (defaultModel !== "custom" && !modelFamily(defaultModel) && opts.session_model)
          defaultModel = opts.session_model;
        dflt = repriced(defaultModel, e.usage);
        baselineUnknown = defaultModel !== "custom" && !modelFamily(defaultModel);
      }
      const premium = e.cost_usd !== null && ran !== null && hasLongContext(e.arm.model) ? Math.max(e.cost_usd - ran, 0) : 0;
      r.actual_cost += actual;
      if (blockedAgents.has(e.agent_id))
        r.reattempt_cost += actual;
      r.default_cost += dflt === null || ran === null ? actual : actual + (dflt - ran) - premium;
      if (d && ran !== null && !d.applied) {
        const save = /* @__PURE__ */ __name((m) => {
          const alt = m === void 0 ? null : repriced(m, e.usage);
          return alt !== null && alt < ran ? ran - alt : 0;
        }, "save");
        const pick = save(d.arm.model);
        const applicable = d.apply_arm === void 0 ? pick : save(d.apply_arm?.model);
        if (d.decider === "respect_explicit" || d.decider === "force_env" || d.decider === "env_pin")
          r.pinned_saving += applicable;
        else if (d.mode === "shadow") {
          r.shadow_saving += applicable;
          r.shadow_pick_saving += Math.max(pick, applicable);
          const fam = modelFamily(d.arm.model);
          if (pick > applicable && fam && (r.shadow_pick_model === null || (FAMILY_RANK[fam] ?? 99) < (FAMILY_RANK[r.shadow_pick_model] ?? 99)))
            r.shadow_pick_model = fam;
        }
      }
      r.time_ms += e.duration_ms ?? 0;
      const key = root2(e.task_id ?? `agent:${e.agent_id}`);
      if (baselineUnknown)
        unknownBaseline.add(key);
      if (!d)
        r.unknown++;
      if (e.cost_usd === null && ran === null)
        unpriced.add(key);
      if (!tasks.has(key)) {
        tasks.add(key);
        r.by_model[e.arm.model] = (r.by_model[e.arm.model] ?? 0) + 1;
      }
    } else if (e.type === "verification") {
      r.blocked += e.blocked ? 1 : 0;
      if (e.agent_id)
        blockedAgents[e.blocked ? "add" : "delete"](e.agent_id);
      const who = e.agent_id ?? `v${anon++}`;
      if (e.reason === "incomplete")
        noResult.add(who);
      else if (e.reason === "incomplete_text")
        saidUnfinished.add(who);
      if (e.result === "no_signal")
        r.no_signal++;
      else if (e.checks.length > 0)
        r[e.result === "pass" ? "checks_passed" : "checks_failed"]++;
      if (e.checks.length > 0)
        r.check_runs++;
      if (e.reason === "unverified_block_limit")
        r.unverified++;
      if (e.reason === "completed")
        r.readonly_completed++;
      if (e.result === "no_signal" && e.reason !== null && UNVERIFIED_REASONS.has(e.reason))
        r.no_signal_edit++;
      for (const c of e.checks)
        r.check_ms += c.duration_ms;
      if (e.result === "fail") {
        const failer = e.agent_id ?? "session";
        const named = e.reason?.startsWith("failed: ") ? e.reason.slice("failed: ".length).split(", ") : null;
        if (named)
          namedFailures.add(failer);
        const names = named ?? (e.reason === "unverified_block_limit" && namedFailures.has(failer) ? [] : e.checks.filter(failedAndRan).map((c) => c.name));
        for (const n of names)
          if (!r.failing_checks.includes(n))
            r.failing_checks.push(n);
      }
    }
  }
  r.time_ms += r.check_ms;
  r.tasks = r.finished = tasks.size;
  r.no_result = noResult.size;
  r.said_unfinished = saidUnfinished.size;
  r.dispatched = dispatched.size;
  r.unpriced_tasks = unpriced.size;
  r.explicit = explicit.size;
  r.pinned = pinned.size;
  r.forced = forced.size;
  r.baseline_unknown_tasks = unknownBaseline.size;
  r.net = r.default_cost - r.actual_cost - r.jev_cost;
  return r;
}
function formatUsd(n) {
  const a = Math.abs(n);
  if (a === 0)
    return "$0.00";
  if (a < 5e-4)
    return "<$0.001";
  return `$${a < 0.1 ? a.toFixed(3) : a.toFixed(2)}`;
}
function netVerdict(net) {
  const delta = Math.round(Math.abs(net) * 1e3) / 1e3;
  if (delta === 0)
    return "same cost";
  return net > 0 ? `\u2212${usd(delta)}` : `+${usd(delta)} more`;
}
function receiptLine(r, opts = {}) {
  const parts = ["Horsy"];
  if (r.tasks === 0 && r.dispatched === 0)
    parts.push("no subagent tasks this session");
  else {
    const counts = `${r.dispatched} dispatched, ${r.finished} finished${r.unknown > 0 ? `, ${r.unknown} unknown` : ""}`;
    const models = Object.entries(r.by_model).sort(([a], [b]) => (FAMILY_RANK[a] ?? 99) - (FAMILY_RANK[b] ?? 99)).map(([m, n]) => `${n} ${m}`).join(", ");
    parts.push(models ? `${counts} (${models})` : counts);
  }
  if (r.tasks > 0) {
    if (r.baseline_unknown_tasks >= r.tasks)
      parts.push(`${usd(r.actual_cost)} (baseline unknown: default model not seen)`);
    else {
      const note = r.baseline_unknown_tasks > 0 ? `; baseline unknown for ${r.baseline_unknown_tasks} of ${r.tasks}` : "";
      parts.push(`${usd(r.actual_cost)} vs ${usd(r.default_cost)} on default (${netVerdict(r.net)}${note})`);
    }
  }
  if (r.explicit > 0)
    parts.push(`${r.explicit} where the agent chose the model itself (Horsy never overrides that)`);
  if (r.pinned > 0)
    parts.push(`${r.pinned} with model pinned by CLAUDE_CODE_SUBAGENT_MODEL`);
  if (r.forced > 0)
    parts.push(`${r.forced} forced by CLAUDE_CODE_SUBAGENT_MODEL_FORCE`);
  if (r.pinned_saving > 0)
    parts.push(`explicit/pinned picks: Horsy would have saved ~${usd(r.pinned_saving)}`);
  if (r.unpriced_tasks > 0)
    parts.push(`(${plural(r.unpriced_tasks, "task")} on unpriced models not counted)`);
  const checks = [];
  if (r.blocked > 0)
    checks.push(`${r.tasks > 0 ? `${plural(r.tasks, "task")}, ` : ""}${plural(r.check_runs, "check run")} (${r.blocked} blocked, finally allowed)`);
  else if (r.checks_passed + r.checks_failed > 0)
    checks.push(`${r.checks_passed} \u2713 ${r.checks_failed} \u2717`);
  if (r.unverified > 0)
    checks.push(`${r.unverified} unverified`);
  if (checks.length > 0)
    parts.push(`checks ${checks.join(", ")}${r.failing_checks.length > 0 ? ` (failing: ${r.failing_checks.join(", ")})` : ""}`);
  if (r.check_ms > 0)
    parts.push(`checks took ${(r.check_ms / 1e3).toFixed(1)} s`);
  if (r.readonly_completed > 0)
    parts.push(`${r.readonly_completed} read-only completed (no checks run)`);
  if (r.no_result > 0)
    parts.push(`${r.no_result} returned no usable result`);
  if (r.said_unfinished > 0)
    parts.push(`${r.said_unfinished} said they could not finish (check them)`);
  if (r.reattempt_cost > 0)
    parts.push(`re-attempts after blocks: ${usd(r.reattempt_cost)} (included above)`);
  const cheapest = r.tasks > 0 && Object.keys(r.by_model).every((m) => modelFamily(m) === "haiku");
  if (cheapest)
    parts.push("all subagent tasks already ran on the cheapest model");
  else if (r.mode === "shadow" && r.shadow_saving > 0) {
    const unproven = r.shadow_pick_saving - r.shadow_saving >= 1e-3 && r.shadow_pick_model ? ` (~${usd(r.shadow_pick_saving)} with ${cap(r.shadow_pick_model)}, unproven)` : "";
    parts.push(unproven ? `shadow: would save ~${usd(r.shadow_saving)} now${unproven}` : `shadow: would have saved ~${usd(r.shadow_saving)}`);
  } else if (opts.detail && r.mode === "shadow" && r.shadow_pick_saving >= 1e-3 && r.shadow_pick_model) {
    parts.push(`shadow: ${cap(r.shadow_pick_model)} would save ~${usd(r.shadow_pick_saving)} (unproven: active needs evidence first)`);
  }
  if (r.tasks > 0)
    parts.push("subagents only (main session not included)");
  return parts.join(" \xB7 ");
}
function worthReading(r, last) {
  const fresh = /* @__PURE__ */ __name((now, was) => now > (was ?? 0), "fresh");
  return fresh(r.checks_failed, last?.failed) || fresh(r.blocked, last?.blocked) || fresh(r.unverified, last?.unverified) || fresh(r.no_result, last?.no_result) || fresh(r.said_unfinished, last?.said_unfinished) || r.mode === "active" && fresh(r.no_signal_edit, last?.no_signal_edit) || netVerdict(r.net) !== "same cost" || r.shadow_saving >= 1e-3 || r.pinned_saving >= 1e-3;
}
function pendingReceiptLine(session_id) {
  try {
    const events = readEvents();
    const mine = events.filter((e) => e.session_id === session_id);
    const r = buildReceipt(events, session_id, { session_model: readSessionModel(session_id), mode: loadConfig().mode });
    const seen = {
      attempts: mine.filter((e) => e.type === "attempt").length,
      verifications: mine.filter((e) => e.type === "verification").length,
      failed: r.checks_failed,
      blocked: r.blocked,
      unverified: r.unverified,
      no_signal_edit: r.no_signal_edit,
      no_result: r.no_result,
      said_unfinished: r.said_unfinished
    };
    const file = join13(horsyHome(), "receipt-state.json");
    let state = {};
    try {
      const v2 = JSON.parse(readFileSync11(file, "utf8"));
      if (typeof v2 === "object" && v2 !== null && !Array.isArray(v2))
        state = v2;
    } catch {
    }
    const last = state[session_id];
    if (last && last.attempts >= seen.attempts && last.verifications >= seen.verifications)
      return null;
    if (seen.attempts + seen.verifications === 0)
      return null;
    ensureHome();
    delete state[session_id];
    state[session_id] = seen;
    const kept = Object.fromEntries(Object.entries(state).slice(-MAX_SESSIONS));
    const tmp = `${file}.${process.pid}.${randomUUID6()}.tmp`;
    try {
      writeFileSync7(tmp, JSON.stringify(kept), PRIVATE_FILE);
      renameSync7(tmp, file);
    } finally {
      rmSync8(tmp, { force: true });
    }
    return worthReading(r, last) ? receiptLine(r) : null;
  } catch {
    return null;
  }
}
var hasLongContext, failedAndRan, UNVERIFIED_REASONS, usd, cap, plural, MAX_SESSIONS;
var init_receipt = __esm({
  "packages/core/build/receipt.js"() {
    "use strict";
    init_models();
    init_pricing2();
    init_config();
    init_session_model();
    init_store();
    hasLongContext = /* @__PURE__ */ __name((model) => {
      const fam = modelFamily(model);
      return fam !== null && priceFor(CURRENT[fam === "mythos" ? "fable" : fam])?.long_context !== void 0;
    }, "hasLongContext");
    failedAndRan = /* @__PURE__ */ __name((c) => c.timed_out ? c.duration_ms > 0 : c.exit_code !== null && c.exit_code !== 0, "failedAndRan");
    __name(repriced, "repriced");
    UNVERIFIED_REASONS = /* @__PURE__ */ new Set([
      "scripts_modified",
      "scripts_unreviewed",
      "baseline_unavailable",
      "baseline_uncertain",
      "busy",
      "session_ended",
      "timeout",
      "repo_too_large",
      "isolation_failed"
    ]);
    __name(buildReceipt, "buildReceipt");
    __name(formatUsd, "formatUsd");
    usd = formatUsd;
    __name(netVerdict, "netVerdict");
    cap = /* @__PURE__ */ __name((w) => w.charAt(0).toUpperCase() + w.slice(1), "cap");
    plural = /* @__PURE__ */ __name((n, w) => `${n} ${w}${n === 1 ? "" : "s"}`, "plural");
    __name(receiptLine, "receiptLine");
    __name(worthReading, "worthReading");
    MAX_SESSIONS = 200;
    __name(pendingReceiptLine, "pendingReceiptLine");
  }
});

// packages/core/build/verify/lock.js
import { closeSync as closeSync5, linkSync as linkSync2, mkdirSync as mkdirSync4, openSync as openSync5, readFileSync as readFileSync12, renameSync as renameSync8, rmSync as rmSync9, statSync as statSync5, writeSync } from "node:fs";
import { dirname as dirname5 } from "node:path";
import { randomUUID as randomUUID7 } from "node:crypto";
function readHeld(file) {
  try {
    const v2 = JSON.parse(readFileSync12(file, "utf8"));
    return typeof v2.pid === "number" && typeof v2.ts === "number" && typeof v2.token === "string" ? v2 : null;
  } catch {
    return null;
  }
}
function isStale(file, held, staleMs, now) {
  if (held)
    return now - held.ts > staleMs || !alive(held.pid);
  try {
    return now - statSync5(file).mtimeMs > 5e3;
  } catch {
    return false;
  }
}
function tryCreate(file, token) {
  try {
    const fd = openSync5(file, "wx", 384);
    try {
      writeSync(fd, JSON.stringify({ pid: process.pid, ts: Date.now(), token }));
    } finally {
      closeSync5(fd);
    }
    return true;
  } catch (err) {
    if (err.code === "EEXIST")
      return false;
    throw err;
  }
}
function takeOver(file, staleToken) {
  const moved = `${file}.stale-${randomUUID7()}`;
  try {
    renameSync8(file, moved);
  } catch {
    return;
  }
  if (readHeld(moved)?.token !== staleToken) {
    try {
      linkSync2(moved, file);
    } catch {
    }
  }
  rmSync9(moved, { force: true });
}
async function acquireLock(file, opts) {
  mkdirSync4(dirname5(file), { recursive: true, mode: 448 });
  const token = randomUUID7();
  const end = Date.now() + opts.waitMs;
  for (; ; ) {
    if (tryCreate(file, token))
      break;
    const held = readHeld(file);
    if (isStale(file, held, opts.staleMs, Date.now())) {
      takeOver(file, held?.token);
      continue;
    }
    if (Date.now() >= end)
      return null;
    await new Promise((r) => setTimeout(r, 50));
  }
  const release = /* @__PURE__ */ __name(() => {
    process.off("exit", release);
    if (readHeld(file)?.token === token)
      rmSync9(file, { force: true });
  }, "release");
  process.on("exit", release);
  return { release };
}
var alive;
var init_lock = __esm({
  "packages/core/build/verify/lock.js"() {
    "use strict";
    __name(readHeld, "readHeld");
    alive = /* @__PURE__ */ __name((pid) => {
      try {
        process.kill(pid, 0);
        return true;
      } catch (err) {
        return err.code === "EPERM";
      }
    }, "alive");
    __name(isStale, "isStale");
    __name(tryCreate, "tryCreate");
    __name(takeOver, "takeOver");
    __name(acquireLock, "acquireLock");
  }
});

// packages/core/build/verify/verify.js
import { createHash as createHash3, randomUUID as randomUUID8 } from "node:crypto";
import { execFileSync as execFileSync5 } from "node:child_process";
import { readFileSync as readFileSync13, readdirSync as readdirSync5, realpathSync as realpathSync4, renameSync as renameSync9, rmSync as rmSync10, statSync as statSync6, writeFileSync as writeFileSync8 } from "node:fs";
import { basename, dirname as dirname6, isAbsolute as isAbsolute3, join as join14, relative as relative3, resolve, sep as sep5 } from "node:path";
function usageOf(raw) {
  const u = isObj4(raw) ? raw : {};
  const cc = isObj4(u["cache_creation"]) ? u["cache_creation"] : {};
  return {
    input_tokens: count(u["input_tokens"]),
    output_tokens: count(u["output_tokens"]),
    cache_creation_input_tokens: count(u["cache_creation_input_tokens"]),
    cache_read_input_tokens: count(u["cache_read_input_tokens"]),
    cache_creation_1h_input_tokens: count(cc["ephemeral_1h_input_tokens"])
  };
}
function firstPromptOf(content) {
  if (typeof content === "string")
    return content;
  if (!Array.isArray(content))
    return "";
  return content.map((b) => isObj4(b) && b["type"] === "text" && typeof b["text"] === "string" ? b["text"] : "").join("");
}
function hunksOf(v2) {
  if (!Array.isArray(v2))
    return null;
  const ok = v2.every((h) => isObj4(h) && ["oldStart", "oldLines", "newStart", "newLines"].every((k) => Number.isInteger(h[k])) && Array.isArray(h["lines"]) && h["lines"].every((l) => typeof l === "string"));
  return ok ? v2 : null;
}
function changeOf(tool, path, input, result) {
  const r = result ?? {};
  const original = str2(r["originalFile"]);
  if (tool === "Write" && r["type"] === "create")
    return { path, before: null, edits: null, patch: null };
  if (original !== null)
    return { path, before: original, edits: null, patch: null };
  const patch = hunksOf(r["structuredPatch"]);
  let edits = null;
  if (tool === "Edit") {
    const o = str2(r["oldString"]) ?? str2(input["old_string"]);
    const n = str2(r["newString"]) ?? str2(input["new_string"]);
    if (o === "" && n !== null)
      return { path, before: null, edits: null, patch: null };
    if (o !== null && n !== null)
      edits = [{ old: o, new: n, all: typeof r["replaceAll"] === "boolean" ? r["replaceAll"] : input["replace_all"] === true }];
  } else if (tool === "MultiEdit" && Array.isArray(input["edits"])) {
    const list2 = input["edits"].map((e) => isObj4(e) && str2(e["old_string"]) !== null && str2(e["new_string"]) !== null ? { old: e["old_string"], new: e["new_string"], all: e["replace_all"] === true } : null);
    if (list2.length > 0 && list2.every((e) => e !== null))
      edits = list2;
  }
  return { path, before: void 0, edits, patch };
}
function changesByFile(changes, root2, cwd) {
  const relOf = /* @__PURE__ */ new Map();
  const out = /* @__PURE__ */ new Map();
  for (const c of changes) {
    if (!relOf.has(c.path))
      relOf.set(c.path, repoRelative([c.path], root2, cwd)[0]);
    const rel = relOf.get(c.path);
    if (rel === void 0)
      continue;
    out.set(rel, [...out.get(rel) ?? [], c]);
  }
  return out;
}
function summarizeAgentTranscript(path) {
  const out = { first_prompt: "", model: null, effort: null, usage: { ...ZERO }, cost: null, files: [], turn_files: [], changes: [], turn_changes: [], turn_tool_use: false, duration_ms: null, ended_normally: false, last_tool_error: false, last_line_tool: false, max_tokens: false, last_text: "" };
  const usages = /* @__PURE__ */ new Map();
  const edits = [];
  let turn = [];
  let turnActing = [];
  const errors = /* @__PURE__ */ new Set();
  const results = /* @__PURE__ */ new Map();
  const editIds = /* @__PURE__ */ new Set();
  let lastToolId = null;
  let lastMsgId = "";
  let lastText = "";
  let firstSeen = false;
  let t0 = Infinity;
  let t1 = -Infinity;
  let seq = 0;
  for (const raw of readFileSync13(path, "utf8").split("\n")) {
    if (!raw)
      continue;
    let o;
    try {
      o = JSON.parse(raw);
    } catch {
      continue;
    }
    if (!isObj4(o))
      continue;
    const ts = typeof o["timestamp"] === "string" ? Date.parse(o["timestamp"]) : NaN;
    if (Number.isFinite(ts)) {
      t0 = Math.min(t0, ts);
      t1 = Math.max(t1, ts);
    }
    const msg = isObj4(o["message"]) ? o["message"] : null;
    if (!msg)
      continue;
    if (o["type"] === "user") {
      if (!firstSeen) {
        out.first_prompt = firstPromptOf(msg["content"]);
        firstSeen = true;
      }
      if (Array.isArray(msg["content"])) {
        for (const b of msg["content"]) {
          if (isObj4(b) && b["type"] === "tool_result" && b["is_error"] === true && typeof b["tool_use_id"] === "string")
            errors.add(b["tool_use_id"]);
        }
        const resultIds = msg["content"].flatMap((b) => isObj4(b) && b["type"] === "tool_result" && typeof b["tool_use_id"] === "string" ? [b["tool_use_id"]] : []);
        const tur = o["toolUseResult"];
        if (resultIds.length === 1 && editIds.has(resultIds[0]) && isObj4(tur))
          results.set(resultIds[0], Object.fromEntries(KEPT.filter((k) => k in tur).map((k) => [k, tur[k]])));
        if (msg["content"].some((b) => isObj4(b) && b["type"] === "tool_result"))
          out.last_line_tool = true;
      }
      if (isPrompt(msg["content"])) {
        turn = [];
        turnActing = [];
      }
      continue;
    }
    if (o["type"] !== "assistant")
      continue;
    const model = msg["model"];
    if (typeof model === "string" && model && model !== "<synthetic>") {
      out.model = model;
      const effort = o["effort"];
      out.effort = typeof effort === "string" && EFFORTS2.includes(effort) ? effort : null;
    }
    const id = typeof msg["id"] === "string" && msg["id"] ? msg["id"] : `line-${seq++}`;
    if (isObj4(msg["usage"]))
      usages.set(id, { usage: usageOf(msg["usage"]), model: out.model });
    out.ended_normally = msg["stop_reason"] === "end_turn";
    out.max_tokens = msg["stop_reason"] === "max_tokens";
    out.last_line_tool = Array.isArray(msg["content"]) && msg["content"].some((b) => isObj4(b) && b["type"] === "tool_use");
    if (!Array.isArray(msg["content"]))
      continue;
    if (id !== lastMsgId) {
      lastMsgId = id;
      lastText = "";
    }
    for (const b of msg["content"]) {
      if (isObj4(b) && b["type"] === "text" && typeof b["text"] === "string")
        lastText += b["text"];
      if (!isObj4(b) || b["type"] !== "tool_use" || typeof b["name"] !== "string")
        continue;
      const use = { id: typeof b["id"] === "string" ? b["id"] : "", name: b["name"], path: null, input: isObj4(b["input"]) ? b["input"] : {} };
      lastToolId = use.id || null;
      if (use.id && EDIT_TOOLS.has(use.name))
        editIds.add(use.id);
      if (ACTING_TOOLS.has(use.name))
        turnActing.push(use);
      if (!EDIT_TOOLS.has(use.name) || !isObj4(b["input"]))
        continue;
      const p = b["input"]["file_path"] ?? b["input"]["notebook_path"];
      if (typeof p === "string" && p) {
        use.path = p;
        edits.push(use);
        turn.push(use);
      }
    }
  }
  const landed = /* @__PURE__ */ __name((u) => !(u.id && errors.has(u.id)), "landed");
  const paths = /* @__PURE__ */ __name((us) => [...new Set(us.filter(landed).map((u) => u.path ?? ""))], "paths");
  for (const m of usages.values()) {
    out.usage = addUsage(out.usage, m.usage);
    const c = m.model ? costOf(m.model, m.usage) : null;
    if (c !== null)
      out.cost = (out.cost ?? 0) + c;
  }
  const changesOf = /* @__PURE__ */ __name((us) => us.filter(landed).map((u) => changeOf(u.name, u.path ?? "", u.input, results.get(u.id))), "changesOf");
  out.files = paths(edits);
  out.turn_files = paths(turn);
  out.changes = changesOf(edits);
  out.turn_changes = changesOf(turn);
  out.turn_tool_use = turnActing.some((u) => !EDIT_TOOLS.has(u.name) || landed(u));
  out.last_tool_error = lastToolId !== null && errors.has(lastToolId);
  out.last_text = lastText;
  out.duration_ms = t1 > t0 ? t1 - t0 : null;
  return out;
}
function readJson(file) {
  try {
    const v2 = JSON.parse(readFileSync13(file, "utf8"));
    return isObj4(v2) ? v2 : null;
  } catch {
    return null;
  }
}
function writeJson(file, value) {
  const tmp = `${file}.${process.pid}.${randomUUID8()}.tmp`;
  try {
    writeFileSync8(tmp, JSON.stringify(value), PRIVATE_FILE);
    renameSync9(tmp, file);
  } finally {
    rmSync10(tmp, { force: true });
  }
}
function stateFile(kind, id) {
  const dir2 = join14(horsyHome(), kind);
  ensureDir(dir2);
  if (Math.random() < 1 / 20) {
    try {
      for (const f of readdirSync5(dir2)) {
        if (statSync6(join14(dir2, f)).mtimeMs < Date.now() - WEEK_MS2)
          rmSync10(join14(dir2, f), { force: true });
      }
    } catch {
    }
  }
  return join14(dir2, `${safeName(id)}.json`);
}
function emit(e) {
  try {
    appendEvent(e);
  } catch {
  }
}
function repoRootOf(cwd) {
  const top = repoRoot(cwd);
  if (top)
    return top;
  try {
    return realpathSync4(cwd);
  } catch {
    return cwd;
  }
}
function repoRelative(paths, root2, cwd) {
  const out = /* @__PURE__ */ new Set();
  for (const p of paths) {
    const abs = isAbsolute3(p) ? p : resolve(cwd, p);
    let real = abs;
    try {
      real = realpathSync4(abs);
    } catch {
      try {
        real = join14(realpathSync4(dirname6(abs)), basename(abs));
      } catch {
      }
    }
    const rel = relative3(root2, real);
    if (rel === "" || rel === ".." || rel.startsWith(`..${sep5}`) || isAbsolute3(rel))
      continue;
    out.add(rel);
  }
  return [...out];
}
function fingerprint(root2) {
  const git2 = /* @__PURE__ */ __name((...args) => execFileSync5("git", args, { cwd: root2, encoding: "utf8", timeout: 5e3, maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "ignore"] }), "git");
  try {
    const h = createHash3("sha256");
    h.update(git2("status", "--porcelain=v1", "-uall"));
    h.update(git2("diff", "--no-ext-diff", "HEAD"));
    for (const f of git2("ls-files", "-o", "--exclude-standard", "-z").split("\0").filter(Boolean).slice(0, 500)) {
      try {
        const s = statSync6(join14(root2, f));
        h.update(`${f}\0${s.size}\0${s.mtimeMs}
`);
      } catch {
      }
    }
    return h.digest("hex");
  } catch {
    return null;
  }
}
function changedFiles(top) {
  const out = /* @__PURE__ */ new Set();
  for (const args of [["diff", "--name-only", "-z", "HEAD"], ["ls-files", "-o", "--exclude-standard", "-z"]]) {
    try {
      const raw = execFileSync5("git", args, { cwd: top, encoding: "utf8", timeout: 5e3, maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "ignore"] });
      for (const f of raw.split("\0"))
        if (f)
          out.add(f);
    } catch {
    }
  }
  return [...out].slice(0, MAX_CHANGED);
}
async function verifyRepo(root2, files, o) {
  const deadline = (o.now ?? Date.now)() + o.timeoutMs;
  if ((o.platform ?? process.platform) === "win32")
    return noSignal("unsupported_platform");
  const top = repoRoot(root2);
  if (!top)
    return noSignal("no_git");
  const { checks, modified, unreviewed } = detectChecksForFiles(top, files.length > 0 ? files : changedFiles(top));
  if (modified.length > 0 || checks.length === 0)
    return { ...await judge([], deadline, () => Promise.resolve(null), modified, unreviewed), labels: {} };
  let lock = null;
  try {
    ensureDir(join14(horsyHome(), "locks"));
    lock = await acquireLock(join14(horsyHome(), "locks", `${hashPath(top)}.lock`), {
      staleMs: o.timeoutMs + 6e4,
      waitMs: o.wait ? o.lockWaitMs ?? LOCK_WAIT_MS : 0
    });
    if (!lock)
      return noSignal("busy");
    sweepOldCopies();
    const rel = /* @__PURE__ */ __name((c) => relative3(top, c.cwd), "rel");
    const copy = makeCopy(top, checks.map(rel), o.limits);
    if ("skip" in copy)
      return noSignal(copy.skip);
    try {
      const inCopy = checks.map((c) => ({ ...c, cwd: join14(copy.dir, rel(c)), ...c.binRoot ? { binRoot: copy.dir } : {} }));
      const run2 = { passthrough: o.passthrough ?? [], ...o.signal ? { signal: o.signal } : {} };
      const base = /* @__PURE__ */ __name((failing) => headBaseline(top, checks.filter((c) => failing.includes(c.name)), deadline, { ...run2, ...files.length > 0 || o.changes ? { files } : {}, ...o.changes ? { changes: o.changes } : {}, ...o.limits ? { limits: o.limits } : {} }), "base");
      const j = await judge(inCopy, deadline, base, [], false, run2);
      const labels = Object.fromEntries(checks.filter((c) => c.where).map((c) => [c.name, `${c.name.split("@")[0] ?? c.name} (${c.where ?? ""})`]));
      return { ...j, labels };
    } finally {
      copy.cleanup();
    }
  } catch {
    return noSignal("isolation_failed");
  } finally {
    lock?.release();
  }
}
function failingTestNames(tail) {
  const names = [];
  for (const raw of tail.replace(ANSI, "").split("\n")) {
    const line = raw.trim();
    for (const re2 of NAME_PATTERNS) {
      const m = re2.exec(line);
      if (!m?.[1])
        continue;
      const name = m[1].replace(/(?:\s+|\s*\()\d+(\.\d+)?\s*m?s\)?$/, "").slice(0, NAME_MAX);
      if (name && !NOT_A_NAME.test(name) && !names.includes(name))
        names.push(name);
      break;
    }
    if (names.length >= NAMES_MAX)
      break;
  }
  return names;
}
function blockReason(j) {
  const failing = failingNames(j);
  const bad = j.runs.filter((r) => r.exit_code !== 0 && (!failing || failing.includes(r.name)));
  const label = /* @__PURE__ */ __name((n) => j.labels[n] ?? n, "label");
  const names = (bad.map((r) => r.timed_out ? `${label(r.name)} (timed out)` : label(r.name)).join(", ") || j.reason || "checks").slice(0, 200);
  const tests = [...new Set(bad.flatMap((r) => failingTestNames(r.tail)))].slice(0, NAMES_MAX).join(", ").slice(0, TESTS_MAX);
  const head = `Horsy verification failed: ${names}${tests ? ` (failing: ${tests})` : ""}. Last output:
`;
  const foot = "\nFix the failures before reporting done.";
  const room = REASON_MAX - head.length - foot.length;
  const tail = (bad[0]?.tail ?? "").trim();
  return head + (room > 0 ? tail.slice(-room) : "") + foot;
}
function unverified(j, max_blocks) {
  const names = (failingNames(j) ?? []).map((n) => j.labels[n] ?? n).join(", ");
  return {
    j: { ...j, reason: "unverified_block_limit" },
    message: `Horsy: checks still failing after ${max_blocks} retries; this "done" is UNVERIFIED (${names}).`
  };
}
function verificationEvent(ctx, f) {
  return makeEvent({
    type: "verification",
    task_id: f.task_id,
    agent_id: f.agent_id,
    task_class: f.task_class,
    arm: f.arm,
    scope: f.scope,
    checks: checkSummary(f.j.runs),
    result: f.j.result,
    strength: f.j.result === "no_signal" ? 0 : 0.5,
    reason: f.j.reason,
    blocked: f.blocked,
    continuation: f.continuation
  }, ctx);
}
function markVerified(session_id, root2) {
  const fp = fingerprint(root2);
  if (fp === null)
    return;
  const file = stateFile("stops", session_id);
  writeJson(file, { ...readJson(file) ?? { blocks: 0, blocked_fp: null }, fingerprint: fp });
}
async function readSettled(path, last) {
  let sum2 = summarizeAgentTranscript(path);
  const end = Date.now() + 500;
  while (!flushed(sum2, last) && Date.now() < end) {
    await new Promise((r) => setTimeout(r, 50));
    sum2 = summarizeAgentTranscript(path);
  }
  return sum2;
}
function readOnlyVerdict(sum2, last) {
  const missing = finalNotOnDisk(sum2, last);
  if (sum2.max_tokens || sum2.last_line_tool && !missing)
    return { result: "fail", reason: "incomplete", runs: [] };
  if (!flushed(sum2, last))
    return { result: "no_signal", reason: "not_flushed", runs: [] };
  if (!missing && FAILURE_MARKERS.test(sum2.last_text) || FAILURE_MARKERS.test(last ?? ""))
    return { result: "no_signal", reason: "incomplete_text", runs: [] };
  return { result: "pass", reason: "completed", runs: [] };
}
async function onSubagentStop(input, deps) {
  const { config } = deps;
  if (config.mode === "off" || !input.agent_type)
    return null;
  const sum2 = await readSettled(input.agent_transcript_path, input.last_assistant_message);
  const root2 = repoRootOf(input.cwd);
  const ctx = { session_id: input.session_id, repo_hash: hashPath(root2) };
  const rels = repoRelative(sum2.files, root2, input.cwd);
  const hashes = rels.map(hashPath);
  const agentFile = stateFile("agents", input.agent_id);
  let st = readJson(agentFile);
  if (!st) {
    const p = takePending(input.session_id, promptHash(sum2.first_prompt), input.agent_type);
    st = { task_id: p?.task_id ?? null, task_class: p?.task_class ?? null, usage: { ...ZERO }, duration_ms: 0, file_hashes: [] };
  }
  const arm = armOf(sum2);
  const delta = subUsage(sum2.usage, st.usage);
  const logged = st.cost ?? (sum2.model ? costOf(sum2.model, st.usage) : null) ?? 0;
  const fresh = hashes.filter((h) => !st.file_hashes.includes(h));
  emit(makeEvent({
    type: "attempt",
    task_id: st.task_id,
    agent_id: input.agent_id,
    task_class: st.task_class,
    arm,
    usage: delta,
    cost_usd: sum2.cost === null ? null : Math.max(sum2.cost - logged, 0),
    duration_ms: sum2.duration_ms === null ? null : Math.max(sum2.duration_ms - st.duration_ms, 0),
    files_changed: fresh.length,
    file_hashes: fresh
  }, ctx));
  writeJson(agentFile, { ...st, usage: sum2.usage, cost: sum2.cost ?? st.cost ?? null, duration_ms: sum2.duration_ms ?? st.duration_ms, file_hashes: [...st.file_hashes, ...fresh] });
  const base = { scope: "subagent", task_id: st.task_id, agent_id: input.agent_id, task_class: st.task_class, arm };
  if (hashes.length === 0) {
    const j = readOnlyVerdict(sum2, input.last_assistant_message);
    emit(verificationEvent(ctx, { ...base, j, blocked: false, continuation: 0 }));
    return null;
  }
  if (checksOff(config, ctx.repo_hash)) {
    emit(verificationEvent(ctx, { ...base, j: CHECKS_OFF, blocked: false, continuation: 0 }));
    markVerified(input.session_id, root2);
    return null;
  }
  if (config.mode === "shadow") {
    try {
      deps.spawnBackground({ scope: "subagent", session_id: input.session_id, cwd: input.cwd, agent_id: input.agent_id, task_id: st.task_id, task_class: st.task_class, arm, files: rels.slice(0, MAX_BG_FILES), transcript_path: input.agent_transcript_path, parent_pid: sessionPid(process.ppid, deps.lookup ?? psLookup) });
    } catch {
    }
    markVerified(input.session_id, root2);
    return null;
  }
  const blockFile = stateFile("blocks", input.agent_id);
  const blocks = readJson(blockFile)?.count ?? 0;
  const v2 = await verifyRepo(root2, rels, { timeoutMs: config.verify.timeout_s * 1e3, wait: true, now: deps.now, passthrough: config.verify.env_passthrough, changes: changesByFile(sum2.changes, root2, input.cwd) });
  const block = blocksOn(v2) && blocks < config.verify.max_blocks;
  const spent = blocksOn(v2) && !block ? unverified(v2, config.verify.max_blocks) : null;
  if (block)
    writeJson(blockFile, { count: blocks + 1 });
  emit(verificationEvent(ctx, { ...base, j: spent?.j ?? v2, blocked: block, continuation: blocks }));
  if (block)
    return { decision: "block", reason: blockReason(v2) };
  markVerified(input.session_id, root2);
  return spent ? { systemMessage: spent.message } : null;
}
async function stopInner(input, deps) {
  const none = { output: null, unverified: null };
  const { config } = deps;
  const root2 = repoRootOf(input.cwd);
  const fp = fingerprint(root2);
  if (fp === null)
    return none;
  const file = stateFile("stops", input.session_id);
  const st = readJson(file);
  let acted = input.stop_hook_active;
  let files = [];
  let sum2 = null;
  try {
    sum2 = summarizeAgentTranscript(input.transcript_path);
    acted ||= sum2.turn_tool_use;
    files = repoRelative(sum2.turn_files, root2, input.cwd);
  } catch {
  }
  const changed = st ? st.fingerprint !== fp : files.length > 0;
  if (!changed || !acted) {
    if (!st)
      writeJson(file, { fingerprint: fp, blocks: 0, blocked_fp: null });
    else if (changed)
      writeJson(file, { ...st, fingerprint: fp });
    return none;
  }
  const ctx = { session_id: input.session_id, repo_hash: hashPath(root2) };
  const base = { scope: "session", task_id: null, agent_id: null, task_class: null, arm: null };
  if (checksOff(config, ctx.repo_hash)) {
    emit(verificationEvent(ctx, { ...base, j: CHECKS_OFF, blocked: false, continuation: 0 }));
    writeJson(file, { fingerprint: fp, blocks: 0, blocked_fp: null });
    return none;
  }
  if (config.mode === "shadow") {
    try {
      deps.spawnBackground({ scope: "session", session_id: input.session_id, cwd: input.cwd, agent_id: null, task_id: null, task_class: null, arm: null, files: files.slice(0, MAX_BG_FILES), transcript_path: input.transcript_path, parent_pid: sessionPid(process.ppid, deps.lookup ?? psLookup) });
    } catch {
    }
    writeJson(file, { fingerprint: fp, blocks: 0, blocked_fp: null });
    return none;
  }
  const continuing = input.stop_hook_active || st?.blocked_fp === fp;
  const blocks = continuing ? st?.blocks ?? 0 : 0;
  const from = continuing && typeof st?.chain_from === "number" ? st.chain_from : sum2 ? sum2.changes.length - sum2.turn_changes.length : 0;
  const chain = sum2 ? sum2.changes.slice(from) : [];
  const chainFiles = repoRelative([...new Set(chain.map((c) => c.path))], root2, input.cwd);
  const v2 = await verifyRepo(root2, chainFiles, { timeoutMs: config.verify.timeout_s * 1e3, wait: true, now: deps.now, passthrough: config.verify.env_passthrough, changes: changesByFile(chain, root2, input.cwd) });
  const block = blocksOn(v2) && blocks < config.verify.max_blocks;
  const spent = blocksOn(v2) && !block ? unverified(v2, config.verify.max_blocks) : null;
  if (block)
    writeJson(file, { fingerprint: st?.fingerprint ?? null, blocks: blocks + 1, blocked_fp: fp, chain_from: from });
  emit(verificationEvent(ctx, { ...base, j: spent?.j ?? v2, blocked: block, continuation: blocks }));
  if (block)
    return { output: { decision: "block", reason: blockReason(v2) }, unverified: null };
  writeJson(file, { fingerprint: fp, blocks, blocked_fp: st?.blocked_fp ?? null, chain_from: null });
  return { output: null, unverified: spent?.message ?? null };
}
async function onStop(input, deps) {
  if (deps.config.mode === "off")
    return null;
  const { output, unverified: warning } = await stopInner(input, deps);
  if (output)
    return output;
  const line = pendingReceiptLine(input.session_id);
  const message = [warning, line].filter(Boolean).join("\n");
  return message ? { systemMessage: message } : null;
}
function isBgArgs(v2) {
  if (!isObj4(v2))
    return false;
  const nstr = /* @__PURE__ */ __name((x) => x === null || typeof x === "string", "nstr");
  return (v2["scope"] === "subagent" || v2["scope"] === "session") && typeof v2["session_id"] === "string" && typeof v2["cwd"] === "string" && nstr(v2["agent_id"]) && nstr(v2["task_id"]) && nstr(v2["task_class"]) && Array.isArray(v2["files"]) && v2["files"].every((f) => typeof f === "string") && nstr(v2["transcript_path"] ?? null) && typeof v2["parent_pid"] === "number" && Number.isInteger(v2["parent_pid"]) && v2["parent_pid"] > 0 && (v2["arm"] === null || isObj4(v2["arm"]) && typeof v2["arm"]["model"] === "string");
}
function sessionPid(pid, lookup) {
  let cur = pid;
  for (let i = 0; i < 3; i++) {
    const info = lookup(cur);
    if (!info || !SHELLS.has(basename(info.comm).replace(/^-/, "")) || info.ppid <= 1)
      break;
    cur = info.ppid;
  }
  return cur;
}
function bgChanges(args, root2) {
  try {
    const sum2 = summarizeAgentTranscript(args.transcript_path);
    return changesByFile(args.scope === "session" ? sum2.turn_changes : sum2.changes, root2, args.cwd);
  } catch {
    return /* @__PURE__ */ new Map();
  }
}
async function verifyInBackground(args) {
  if (!isBgArgs(args))
    return;
  const config = loadConfig();
  if (config.mode === "off")
    return;
  const root2 = repoRootOf(args.cwd);
  const abort = new AbortController();
  const watch = setInterval(() => {
    if (!alive(args.parent_pid))
      abort.abort();
  }, PARENT_POLL_MS);
  watch.unref();
  try {
    const j = await verifyRepo(root2, args.files, { timeoutMs: config.verify.timeout_s * 1e3, wait: false, passthrough: config.verify.env_passthrough, signal: abort.signal, ...args.transcript_path ? { changes: bgChanges(args, root2) } : {} });
    if (loadConfig().mode === "off")
      return;
    emit(verificationEvent({ session_id: args.session_id, repo_hash: hashPath(root2) }, { scope: args.scope, task_id: args.task_id, agent_id: args.agent_id, task_class: args.task_class, arm: args.arm, j, blocked: false, continuation: 0 }));
  } finally {
    clearInterval(watch);
  }
}
var ZERO, EDIT_TOOLS, ACTING_TOOLS, EFFORTS2, REASON_MAX, MAX_BG_FILES, FAILURE_MARKERS, WEEK_MS2, isObj4, count, addUsage, subUsage, isPrompt, str2, KEPT, LOCK_WAIT_MS, MAX_CHANGED, noSignal, blocksOn, failingNames, NAME_MAX, NAMES_MAX, TESTS_MAX, ANSI, NAME_PATTERNS, NOT_A_NAME, checkSummary, checksOff, CHECKS_OFF, armOf, norm, finalNotOnDisk, flushed, PARENT_POLL_MS, SHELLS, psLookup;
var init_verify = __esm({
  "packages/core/build/verify/verify.js"() {
    "use strict";
    init_config();
    init_events();
    init_models();
    init_pricing2();
    init_pending();
    init_receipt();
    init_repo();
    init_session_model();
    init_store();
    init_baseline();
    init_detect();
    init_isolate();
    init_lock();
    init_run();
    ZERO = { input_tokens: 0, output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, cache_creation_1h_input_tokens: 0 };
    EDIT_TOOLS = /* @__PURE__ */ new Set(["Edit", "Write", "MultiEdit", "NotebookEdit"]);
    ACTING_TOOLS = /* @__PURE__ */ new Set([...EDIT_TOOLS, "Bash", "Agent", "Task"]);
    EFFORTS2 = ["low", "medium", "high", "xhigh", "max"];
    REASON_MAX = 600;
    MAX_BG_FILES = 200;
    FAILURE_MARKERS = /unable to|could not|couldn['’]t|can['’]t|cannot|not able to|failed|permission|denied|not completed|did not complete|no access/i;
    WEEK_MS2 = 7 * 24 * 3600 * 1e3;
    isObj4 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    count = /* @__PURE__ */ __name((v2) => typeof v2 === "number" && Number.isFinite(v2) && v2 > 0 ? v2 : 0, "count");
    __name(usageOf, "usageOf");
    addUsage = /* @__PURE__ */ __name((a, b) => ({
      input_tokens: a.input_tokens + b.input_tokens,
      output_tokens: a.output_tokens + b.output_tokens,
      cache_creation_input_tokens: a.cache_creation_input_tokens + b.cache_creation_input_tokens,
      cache_read_input_tokens: a.cache_read_input_tokens + b.cache_read_input_tokens,
      cache_creation_1h_input_tokens: a.cache_creation_1h_input_tokens + b.cache_creation_1h_input_tokens
    }), "addUsage");
    subUsage = /* @__PURE__ */ __name((a, b) => ({
      input_tokens: Math.max(a.input_tokens - b.input_tokens, 0),
      output_tokens: Math.max(a.output_tokens - b.output_tokens, 0),
      cache_creation_input_tokens: Math.max(a.cache_creation_input_tokens - b.cache_creation_input_tokens, 0),
      cache_read_input_tokens: Math.max(a.cache_read_input_tokens - b.cache_read_input_tokens, 0),
      cache_creation_1h_input_tokens: Math.max(a.cache_creation_1h_input_tokens - b.cache_creation_1h_input_tokens, 0)
    }), "subUsage");
    __name(firstPromptOf, "firstPromptOf");
    isPrompt = /* @__PURE__ */ __name((content) => typeof content === "string" || Array.isArray(content) && content.some((b) => isObj4(b) && b["type"] === "text"), "isPrompt");
    str2 = /* @__PURE__ */ __name((v2) => typeof v2 === "string" ? v2 : null, "str");
    __name(hunksOf, "hunksOf");
    KEPT = ["originalFile", "type", "oldString", "newString", "replaceAll", "structuredPatch"];
    __name(changeOf, "changeOf");
    __name(changesByFile, "changesByFile");
    __name(summarizeAgentTranscript, "summarizeAgentTranscript");
    __name(readJson, "readJson");
    __name(writeJson, "writeJson");
    __name(stateFile, "stateFile");
    __name(emit, "emit");
    __name(repoRootOf, "repoRootOf");
    __name(repoRelative, "repoRelative");
    __name(fingerprint, "fingerprint");
    LOCK_WAIT_MS = 3e4;
    MAX_CHANGED = 500;
    noSignal = /* @__PURE__ */ __name((reason) => ({ result: "no_signal", reason, runs: [], labels: {} }), "noSignal");
    __name(changedFiles, "changedFiles");
    __name(verifyRepo, "verifyRepo");
    blocksOn = /* @__PURE__ */ __name((j) => j.result === "fail" && (j.reason?.startsWith("failed: ") ?? false), "blocksOn");
    failingNames = /* @__PURE__ */ __name((j) => j.reason?.startsWith("failed: ") ? j.reason.slice("failed: ".length).split(", ") : null, "failingNames");
    NAME_MAX = 60;
    NAMES_MAX = 5;
    TESTS_MAX = 150;
    ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*[A-Za-z]`, "g");
    NAME_PATTERNS = [/^not ok \d+\s*-?\s*(.+?)(?:\s+#.*)?$/, /^--- FAIL:\s+(\S+)/, /^FAILED\s+(\S+(?:::\S+|\.py))(?=\s|$)/, /^FAIL {1,}(\S.*)$/, /^[✗✕✖×●]\s*([^\s\d]\S*(?:\s.*)?)$/];
    NOT_A_NAME = /^(?:Console|failing tests:)$/;
    __name(failingTestNames, "failingTestNames");
    __name(blockReason, "blockReason");
    __name(unverified, "unverified");
    checkSummary = /* @__PURE__ */ __name((runs) => runs.map(({ name, exit_code, duration_ms, timed_out }) => ({ name, exit_code, duration_ms, timed_out })), "checkSummary");
    __name(verificationEvent, "verificationEvent");
    checksOff = /* @__PURE__ */ __name((config, repo_hash) => !config.verify.repos.includes(repo_hash), "checksOff");
    CHECKS_OFF = { result: "no_signal", reason: "checks_off", runs: [] };
    armOf = /* @__PURE__ */ __name((s) => ({ model: s.model && modelFamily(s.model) || "inherit", effort: s.effort }), "armOf");
    __name(markVerified, "markVerified");
    norm = /* @__PURE__ */ __name((t) => t.replace(/\s+/g, " ").trim(), "norm");
    finalNotOnDisk = /* @__PURE__ */ __name((x, last) => x.last_line_tool && !!last && norm(last) !== "" && norm(x.last_text) !== norm(last), "finalNotOnDisk");
    flushed = /* @__PURE__ */ __name((x, last) => x.ended_normally || !!last && norm(last) !== "" && norm(x.last_text) === norm(last) || finalNotOnDisk(x, last), "flushed");
    __name(readSettled, "readSettled");
    __name(readOnlyVerdict, "readOnlyVerdict");
    __name(onSubagentStop, "onSubagentStop");
    __name(stopInner, "stopInner");
    __name(onStop, "onStop");
    __name(isBgArgs, "isBgArgs");
    PARENT_POLL_MS = 1e3;
    SHELLS = /* @__PURE__ */ new Set(["sh", "bash", "zsh", "dash", "fish"]);
    psLookup = /* @__PURE__ */ __name((pid) => {
      try {
        const out = execFileSync5("ps", ["-o", "ppid=,comm=", "-p", String(pid)], { encoding: "utf8", timeout: 1e3, stdio: ["ignore", "pipe", "ignore"] });
        const m = /^\s*(\d+)\s+(.+?)\s*$/.exec(out);
        return m ? { ppid: Number(m[1]), comm: m[2] } : null;
      } catch {
        return null;
      }
    }, "psLookup");
    __name(sessionPid, "sessionPid");
    __name(bgChanges, "bgChanges");
    __name(verifyInBackground, "verifyInBackground");
  }
});

// packages/core/build/verify/warn.js
function unverifiedWarning(session_id, agent_id, tool_use_id) {
  const events = readRecentEvents(TAIL_BYTES).filter((e) => e.session_id === session_id);
  const start = events.find((e) => e.type === "task_created" && e.task_id === tool_use_id)?.ts;
  if (!start)
    return null;
  const last = events.filter((e) => e.type === "verification" && e.agent_id === agent_id && !e.blocked && e.ts >= start).at(-1);
  if (last?.type !== "verification")
    return null;
  if (last.reason !== "unverified_block_limit" && !(last.result === "fail" && last.checks.length > 0))
    return null;
  const failed = last.checks.filter((c) => c.timed_out || c.exit_code !== 0);
  const names = (failed.length > 0 ? failed : last.checks).map((c) => c.name);
  return `Horsy: this subagent's "done" is UNVERIFIED: checks still failing (${names.join(", ") || "checks"}).`;
}
var TAIL_BYTES;
var init_warn = __esm({
  "packages/core/build/verify/warn.js"() {
    "use strict";
    init_store();
    TAIL_BYTES = 256 * 1024;
    __name(unverifiedWarning, "unverifiedWarning");
  }
});

// packages/core/build/verify/index.js
var init_verify2 = __esm({
  "packages/core/build/verify/index.js"() {
    "use strict";
    init_detect();
    init_run();
    init_baseline();
    init_verify();
    init_warn();
  }
});

// packages/core/build/reconcile.js
import { execFileSync as execFileSync6 } from "node:child_process";
import { realpathSync as realpathSync5 } from "node:fs";
function git(root2, args) {
  return execFileSync6("git", args, {
    cwd: root2,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
    timeout: 1e4,
    maxBuffer: 32 * 1024 * 1024
  });
}
function readCommits(root2, sinceMs, untilMs) {
  let email = "";
  try {
    email = git(root2, ["config", "user.email"]).trim();
  } catch {
  }
  let out;
  try {
    out = git(root2, [
      "-c",
      "core.quotepath=off",
      "log",
      "-z",
      "--name-only",
      `--format=${SEP}%H%x1f%ct%x1f%s%x1f%b%x02`,
      `--since=${Math.floor(sinceMs / 1e3)}`,
      `--until=${Math.ceil(untilMs / 1e3)}`,
      ...email ? ["--fixed-strings", `--author=<${email}>`] : []
    ]);
  } catch {
    return null;
  }
  const commits = [];
  for (const rec of out.split(SEP)) {
    if (!rec)
      continue;
    const end = rec.indexOf("");
    const [, ct = "", subject = "", body = ""] = rec.slice(0, end).split("");
    const names = rec.slice(end + 1).replace(/^\0\n?/, "").split("\0").filter(Boolean);
    commits.push({
      at: Number(ct) * 1e3,
      revert: subject.startsWith("Revert") || body.includes("This reverts commit"),
      files: new Set(names.map(hashPath))
    });
  }
  return commits.reverse().sort((a, b) => a.at - b.at);
}
function classify2(files, startMs, commits, index) {
  const start = Math.floor(startMs / 1e3) * 1e3;
  const end = start + 24 * HOUR_MS;
  const hit = /* @__PURE__ */ new Set();
  for (const f of files)
    for (const i of index.get(f) ?? [])
      hit.add(i);
  const touching = [...hit].sort((a, b) => a - b).map((i) => commits[i]).filter((c) => c.at >= start && c.at <= end);
  const [, ...later] = touching;
  const reverted = later.some((c) => c.revert);
  const reworked = !reverted && later.length > 0;
  const evidence = { commits: touching.length, reverted, reworked };
  if (touching.length === 0)
    return { result: "no_signal", success: null, weight: 1, evidence };
  if (reverted)
    return { result: "reverted", success: false, weight: 1, evidence };
  if (reworked)
    return { result: "reworked", success: false, weight: 0.5, evidence };
  return { result: "kept", success: true, weight: 1, evidence };
}
function reconcile(root2, events, opts) {
  let real = root2;
  try {
    real = realpathSync5(root2);
  } catch {
  }
  const repo = repoHash(real);
  const mine = events.filter((e) => e.repo_hash === repo);
  const done = /* @__PURE__ */ new Set();
  const attempts = /* @__PURE__ */ new Map();
  const createdAt = /* @__PURE__ */ new Map();
  const created = /* @__PURE__ */ new Map();
  const verdict = /* @__PURE__ */ new Map();
  for (const e of mine) {
    if (e.type === "outcome_delayed")
      done.add(e.task_id);
    else if (e.type === "attempt" && e.task_id !== null)
      attempts.set(e.task_id, [...attempts.get(e.task_id) ?? [], e]);
    else if (e.type === "task_created") {
      created.set(e.task_id, e);
      createdAt.set(e.task_id, Date.parse(e.ts));
    } else if (e.type === "verification" && e.task_id !== null && e.scope === "subagent" && !e.blocked) {
      if (e.result === "no_signal")
        verdict.delete(e.task_id);
      else
        verdict.set(e.task_id, e.result);
    }
  }
  const out = [];
  const emit2 = /* @__PURE__ */ __name((taskId, sessionId, taskClass, arm, r) => {
    done.add(taskId);
    const e = makeEvent({ type: "outcome_delayed", task_id: taskId, task_class: taskClass, arm, supersedes: verdict.get(taskId) ?? null, ...r }, { session_id: sessionId, repo_hash: repo });
    out.push({ ...e, ts: opts.now.toISOString() });
  }, "emit");
  for (const child of created.values()) {
    const p = child.parent_task_id === null ? void 0 : created.get(child.parent_task_id);
    if (!p || p.session_id !== child.session_id || done.has(p.task_id))
      continue;
    const a = attempts.get(p.task_id)?.at(-1);
    emit2(p.task_id, p.session_id, a?.task_class ?? p.task_class, a?.arm ?? null, {
      result: "reworked",
      success: false,
      weight: 1,
      evidence: { commits: 0, reverted: false, reworked: true }
    });
  }
  const due = [];
  for (const [id, list2] of attempts) {
    const files = new Set(list2.flatMap((a) => a.file_hashes));
    const first = list2[0];
    const start = createdAt.get(id) ?? Date.parse(first.ts) - (first.duration_ms ?? 0);
    if (files.size === 0 || done.has(id) || !Number.isFinite(start))
      continue;
    if (opts.all === true || start + opts.windowHours * HOUR_MS <= opts.now.getTime())
      due.push({ id, last: list2.at(-1), files, start });
  }
  if (due.length > 0) {
    const commits = readCommits(real, Math.min(...due.map((d) => d.start)), Math.max(...due.map((d) => d.start)) + opts.windowHours * HOUR_MS);
    if (commits !== null) {
      const index = /* @__PURE__ */ new Map();
      commits.forEach((c, i) => {
        for (const f of c.files)
          index.set(f, [...index.get(f) ?? [], i]);
      });
      for (const d of due)
        emit2(d.id, d.last.session_id, d.last.task_class, d.last.arm, classify2(d.files, d.start, commits, index));
    }
  }
  return out;
}
var HOUR_MS, SEP;
var init_reconcile = __esm({
  "packages/core/build/reconcile.js"() {
    "use strict";
    init_events();
    init_repo();
    HOUR_MS = 36e5;
    SEP = "";
    __name(git, "git");
    __name(readCommits, "readCommits");
    __name(classify2, "classify");
    __name(reconcile, "reconcile");
  }
});

// packages/core/build/index.js
var init_build = __esm({
  "packages/core/build/index.js"() {
    "use strict";
    init_events();
    init_store();
    init_repo();
    init_classify();
    init_pricing2();
    init_models();
    init_config();
    init_bandit();
    init_deciders();
    init_router();
    init_pending();
    init_transcript_tail();
    init_session_model();
    init_verify2();
    init_reconcile();
    init_receipt();
    init_agent_perms();
  }
});

// packages/audit/build/analyze.js
function rows(m) {
  return [...m].map(([key, v2]) => ({ key, ...v2 })).sort((a, b) => b.cost - a.cost || a.key.localeCompare(b.key));
}
function money(x) {
  if (!Number.isFinite(x) || x <= 0)
    return "$0.00";
  if (x < 5e-3)
    return "<$0.01";
  if (x < 99.995)
    return `$${x.toFixed(2)}`;
  return `$${Math.round(x).toLocaleString("en-US")}`;
}
function tokenCount(n) {
  const v2 = Math.round(n);
  if (v2 < 1e3)
    return String(v2);
  if (v2 < 9950)
    return `${(v2 / 1e3).toFixed(1)}k`;
  if (v2 < 999500)
    return `${Math.round(v2 / 1e3)}k`;
  if (v2 < 99995e4)
    return `${(v2 / 1e6).toFixed(1)}M`;
  return `${(v2 / 1e9).toFixed(1)}B`;
}
function shareLine(days, total, lo, hi, unpricedShare, fresh) {
  const tokens = fresh;
  const head = fresh === void 0 ? `Last ${days} days: ~${money(total)} of Claude Code usage at API list prices` : `Last ${days} days: ${tokenCount(fresh)} fresh tokens of Claude Code usage (\u2248 ${money(total)} at API list prices)`;
  const partial = unpricedShare > PARTIAL_UNPRICED_SHARE ? ` (partial: ${Math.round(unpricedShare * 100)}% of tokens unpriced)` : "";
  if (!(hi >= 5e-3))
    return `${head}, no avoidable spend found (estimates)${partial}`;
  const a = money(lo), b = money(hi);
  const haiku = a === b ? "" : ` (Haiku: unproven in your repo, up to ~${b})`;
  return `${head}, ~${a} could cost less${tokens === void 0 ? "" : " at API prices"}${haiku} (estimates)${partial}`;
}
function median(xs) {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? 0 : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function dominantModel(turns) {
  const n = /* @__PURE__ */ new Map();
  for (const t of turns)
    n.set(t.model, (n.get(t.model) ?? 0) + 1);
  let best = "unknown", max = 0;
  for (const [m, c] of n)
    if (c > max) {
      best = m;
      max = c;
    }
  return best;
}
function analyze(p, opts) {
  const { days } = opts;
  if (!Number.isFinite(days) || days <= 0)
    throw new RangeError(`days must be a positive number, got ${days}`);
  const hi = opts.now.getTime();
  const lo = hi - days * DAY_MS3;
  const inWindow = /* @__PURE__ */ __name((ts) => {
    const t = Date.parse(ts);
    return t >= lo && t <= hi;
  }, "inWindow");
  const real = p.turns.filter((t) => t.model !== "<synthetic>");
  const turns = real.filter((t) => inWindow(t.ts));
  const prevTurns = real.filter((t) => {
    const x = Date.parse(t.ts);
    return x >= lo - days * DAY_MS3 && x < lo;
  });
  const calls = p.calls.filter((c) => inWindow(c.ts));
  let total = 0, subagent = 0, subagentTokens = 0, outTok = 0, unknownTokens = 0, approxTokens = 0, allTokens = 0, inTok = 0, ccTok = 0, crTok = 0, inputCost = 0;
  const subKind = { input: 0, cache_writes: 0, cache_reads: 0, output: 0 };
  const byModel = /* @__PURE__ */ new Map();
  const byProject = /* @__PURE__ */ new Map();
  const byDay = /* @__PURE__ */ new Map();
  const mainModels = /* @__PURE__ */ new Map();
  const add = /* @__PURE__ */ __name((m, key, cost, tokens, fresh) => {
    const cur = m.get(key) ?? { cost: 0, tokens: 0, fresh: 0 };
    cur.cost += cost;
    cur.tokens += tokens;
    cur.fresh += fresh;
    m.set(key, cur);
  }, "add");
  for (const t of turns) {
    const { cost: c, approx } = costInfo(t.model, t.usage);
    const tokens = tokensOf(t.usage);
    const fresh = freshOf(t.usage);
    const cost = c ?? 0;
    if (c === null)
      unknownTokens += tokens;
    else if (approx)
      approxTokens += tokens;
    total += cost;
    allTokens += tokens;
    if (t.sidechain) {
      subagent += cost;
      subagentTokens += tokens;
      subKind.input += t.usage.input_tokens;
      subKind.cache_writes += t.usage.cache_creation_input_tokens;
      subKind.cache_reads += t.usage.cache_read_input_tokens;
      subKind.output += t.usage.output_tokens;
    } else
      add(mainModels, t.model, cost, tokens, fresh);
    outTok += t.usage.output_tokens;
    inTok += t.usage.input_tokens;
    ccTok += t.usage.cache_creation_input_tokens;
    crTok += t.usage.cache_read_input_tokens;
    inputCost += costOf(t.model, inputSide(t.usage)) ?? 0;
    add(byModel, t.model, cost, tokens, fresh);
    add(byProject, t.project, cost, tokens, fresh);
    add(byDay, new Date(t.ts).toISOString().slice(0, 10), cost, tokens, fresh);
  }
  const inputDenom = inTok + ccTok + crTok;
  const cacheEff = inputDenom === 0 ? null : crTok / inputDenom;
  const turnsByAgent = /* @__PURE__ */ new Map();
  for (const t of turns) {
    if (t.agent_id === null)
      continue;
    const l = turnsByAgent.get(t.agent_id);
    if (l)
      l.push(t);
    else
      turnsByAgent.set(t.agent_id, [t]);
  }
  const firstCall = /* @__PURE__ */ new Map();
  for (const c of calls) {
    if (c.agent_id === null)
      continue;
    const cur = firstCall.get(c.agent_id);
    if (!cur || Date.parse(c.ts) < Date.parse(cur.ts))
      firstCall.set(c.agent_id, c);
  }
  const tasks = calls.map((call2) => {
    const ts = call2.agent_id === null || firstCall.get(call2.agent_id) !== call2 ? [] : turnsByAgent.get(call2.agent_id) ?? [];
    const cls = classify(call2, { has_tests: false, hash_description: false }).task_class;
    return { call: call2, cls, model: call2.resolved_model ?? (ts.length > 0 ? dominantModel(ts) : "unknown"), cost: costSum(ts), tokens: ts.reduce((n, x) => n + tokensOf(x.usage), 0), fresh: ts.reduce((n, x) => n + freshOf(x.usage), 0), turns: ts };
  });
  const taskRows = /* @__PURE__ */ new Map();
  for (const t of tasks) {
    const k = `${t.cls}\0${t.model}`;
    const r = taskRows.get(k) ?? { task_class: t.cls, model: t.model, count: 0, cost: 0, tokens: 0, fresh: 0 };
    r.count++;
    r.cost += t.cost;
    r.tokens += t.tokens;
    r.fresh += t.fresh;
    taskRows.set(k, r);
  }
  const attributed = { cost: 0, tokens: 0, fresh: 0 };
  for (const t of tasks) {
    attributed.cost += t.cost;
    attributed.tokens += t.tokens;
    attributed.fresh += t.fresh;
  }
  const left = /* @__PURE__ */ __name((whole, part) => whole - part > 1e-9 ? whole - part : 0, "left");
  const unattributed = {
    cost: left(subagent, attributed.cost),
    tokens: Math.round(left(subagentTokens, attributed.tokens)),
    fresh: Math.round(left(subKind.input + subKind.cache_writes + subKind.output, attributed.fresh))
  };
  const groups2 = /* @__PURE__ */ new Map();
  for (const t of tasks) {
    const d = normalizeDescription(t.call.description);
    if (d === "")
      continue;
    const k = `${t.call.session_id}\0${d}`;
    const g = groups2.get(k);
    if (g)
      g.push(t);
    else
      groups2.set(k, [t]);
  }
  const reworked = new Set(tasks.filter((t) => t.call.failed));
  let repeated = 0, retries = 0;
  for (const g of groups2.values()) {
    if (g.length < 2)
      continue;
    g.sort((a, b) => Date.parse(a.call.ts) - Date.parse(b.call.ts));
    repeated++;
    g.slice(0, -1).forEach((t) => reworked.add(t));
    retries += g.slice(0, -1).filter((t) => t.call.failed).length;
  }
  const rework = {
    repeated_tasks: repeated,
    retries_after_failure: retries,
    cost: [...reworked].reduce((n, t) => n + t.cost, 0),
    tasks: reworked.size,
    tokens: [...reworked].reduce((n, t) => n + t.tokens, 0),
    fresh: [...reworked].reduce((n, t) => n + t.fresh, 0)
  };
  const waste = { min: 0, max: 0, tasks: 0, tokens: 0, fresh: 0 };
  for (const t of tasks) {
    if (reworked.has(t))
      continue;
    const fam = modelFamily(t.model);
    if (!SAFE_CLASSES.includes(t.cls) || fam !== "opus" && fam !== "fable")
      continue;
    waste.tasks++;
    waste.tokens += t.tokens;
    waste.fresh += t.fresh;
    waste.min += Math.max(0, t.cost - reprice(t.turns, CURRENT.sonnet));
    waste.max += Math.max(0, t.cost - reprice(t.turns, CURRENT.haiku));
  }
  const firstTurn = /* @__PURE__ */ new Map();
  for (const t of turns) {
    if (t.sidechain)
      continue;
    const cur = firstTurn.get(t.session_id);
    if (!cur || Date.parse(t.ts) < Date.parse(cur.ts))
      firstTurn.set(t.session_id, t);
  }
  const startedInWindow = [];
  for (const [sid, cc] of p.session_first_cache_creation) {
    const t = firstTurn.get(sid);
    if (t && t.usage.cache_creation_input_tokens === cc)
      startedInWindow.push([t, cc]);
  }
  const relative4 = startedInWindow.length >= BLOAT_MIN_SESSIONS ? BLOAT_MEDIAN_FACTOR * median(startedInWindow.map(([, cc]) => cc)) : 0;
  const bloatThreshold = Math.max(BLOAT_MIN_TOKENS, relative4);
  let bloatCount = 0, bloatCost = 0, bloatTokens = 0;
  for (const [t, cc] of startedInWindow) {
    if (cc <= bloatThreshold)
      continue;
    bloatCount++;
    bloatTokens += cc - bloatThreshold;
    const write = costOf(t.model, { ...t.usage, input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0 }) ?? 0;
    bloatCost += write * ((cc - bloatThreshold) / cc);
  }
  const ctxBySession = /* @__PURE__ */ new Map();
  for (const t of turns) {
    if (t.sidechain)
      continue;
    const ctx = t.usage.input_tokens + t.usage.cache_read_input_tokens + t.usage.cache_creation_input_tokens;
    const l = ctxBySession.get(t.session_id);
    if (l)
      l.push(ctx);
    else
      ctxBySession.set(t.session_id, [ctx]);
  }
  const sessionMedians = [...ctxBySession.values()].filter((l) => l.length >= LONG_SESSION_MIN_TURNS).map(median);
  const ownMedian = median(sessionMedians);
  const longMedians = sessionMedians.length < LONG_SESSION_MIN_SESSIONS ? [] : sessionMedians.filter((m) => m > LONG_SESSION_MIN_TOKENS && m > LONG_SESSION_FACTOR * ownMedian);
  const longSessions = { count: longMedians.length, context_tokens: Math.round(median(longMedians)) };
  const scale = MONTH_DAYS / Math.max(days, MIN_SCALE_DAYS);
  const cents = /* @__PURE__ */ __name((x) => Math.round(x * scale * 100) / 100, "cents");
  const cand = [];
  const tips = [];
  if (waste.tasks >= MIN_FINDING_TASKS && waste.min * scale >= MIN_FINDING_SAVING) {
    const fams = new Set(tasks.filter((t) => SAFE_CLASSES.includes(t.cls)).map((t) => modelFamily(t.model)));
    const label = fams.has("opus") && fams.has("fable") ? "Opus or Fable" : fams.has("fable") && !fams.has("opus") ? "Fable" : "Opus";
    cand.push({
      title: `Send search/read/explain/rename subagents to Sonnet \u2014 ${waste.tasks} task${waste.tasks === 1 ? "" : "s"} ran on ${label}.`,
      action: 'Pass `model: "sonnet"` (or set `model: sonnet` in the agent\'s definition) for search/read/explain subagents. Haiku: unproven in your repo. The Horsy plugin can do this per task, in shadow mode first.',
      est_monthly_saving: cents(waste.min)
    });
  }
  if (bloatCount > 0) {
    cand.push({
      title: `${bloatCount} session${bloatCount === 1 ? "" : "s"} started with > ${tokenCount(bloatThreshold)} tokens of context.`,
      action: "Trim CLAUDE.md, unused MCP servers and skills loaded at startup.",
      est_monthly_saving: cents(bloatCost)
    });
  }
  const mainTurns = turns.filter((t) => !t.sidechain);
  const mainSessions = new Set(mainTurns.map((t) => t.session_id)).size;
  if (cacheEff !== null && cacheEff < CACHE_TARGET && inputCost > 0 && mainTurns.length >= MIN_CACHE_TURNS && mainSessions >= MIN_CACHE_SESSIONS) {
    cand.push({
      title: `Only ${Math.round(cacheEff * 100)} % of input came from cache.`,
      action: "Avoid editing CLAUDE.md or switching models mid-session; keep long sessions alive instead of restarting.",
      est_monthly_saving: cents((CACHE_TARGET - cacheEff) * inputCost * CACHE_SAVING_SHARE)
    });
  }
  if (reworked.size >= MIN_FINDING_TASKS && rework.cost * scale >= MIN_FINDING_SAVING) {
    cand.push({
      title: `${reworked.size} subagent tasks failed or were dispatched more than once`,
      action: "Give subagents the acceptance check up front (e.g. 'run the tests before reporting').",
      est_monthly_saving: cents(rework.cost)
    });
  }
  if (total > 20 && subagent / total < 0.1) {
    tips.push({
      title: `Almost no work is delegated (${Math.round(subagent / total * 100)} % of spend).`,
      action: "Delegating search and reading to subagents keeps the main context small.",
      est_monthly_saving: null
    });
  }
  if (longSessions.count > 0) {
    const n = longSessions.count;
    tips.push({
      title: `Your ${n === 1 ? "longest session re-reads" : `${n} longest sessions re-read`} ~${tokenCount(longSessions.context_tokens)} tokens of context every turn`,
      action: "/clear between unrelated tasks, /compact when switching topic.",
      est_monthly_saving: null
    });
  }
  const top3 = cand.filter((f) => f.est_monthly_saving !== null && f.est_monthly_saving > 0).sort((a, b) => (b.est_monthly_saving ?? 0) - (a.est_monthly_saving ?? 0)).slice(0, 3);
  const mainCost = total - subagent;
  const mainFam = { opus: 0, fable: 0 };
  for (const t of mainTurns) {
    const f = modelFamily(t.model);
    if (f === "opus" || f === "fable")
      mainFam[f] += costOf(t.model, t.usage) ?? 0;
  }
  if (total > 0 && mainCost / total >= MAIN_SHARE_MIN && (mainFam.opus + mainFam.fable) / mainCost >= TOP_FAMILY_SHARE_MIN) {
    tips.unshift({
      title: `${Math.round(mainCost / total * 100)}% of your spend is in main sessions, mostly on ${mainFam.fable > mainFam.opus ? "Fable" : "Opus"} \u2014 subagent routing can't change that`,
      action: "For routine sessions, switch with /model sonnet; keep Opus for planning and hard debugging.",
      est_monthly_saving: null
    });
  }
  const avoidable = { min: waste.min + rework.cost + bloatCost, max: waste.max + rework.cost + bloatCost };
  const days3 = rows(byDay).sort((a, b) => b.tokens - a.tokens || a.key.localeCompare(b.key)).slice(0, 3).map((d) => ({ day: d.key, tokens: d.tokens, fresh: d.fresh, cost: d.cost }));
  const days3Fresh = rows(byDay).sort((a, b) => b.fresh - a.fresh || a.key.localeCompare(b.key)).slice(0, 3).map((d) => ({ day: d.key, tokens: d.tokens, fresh: d.fresh, cost: d.cost }));
  const windows = [];
  for (const t of [...turns].sort((a, b) => Date.parse(a.ts) - Date.parse(b.ts))) {
    const x = Date.parse(t.ts);
    let w = windows.at(-1);
    if (!w || x >= w.start + LIMIT_WINDOW_MS)
      windows.push(w = { start: Math.floor(x / HOUR_MS2) * HOUR_MS2, tokens: 0, fresh: 0, cost: 0 });
    w.tokens += tokensOf(t.usage);
    w.fresh += freshOf(t.usage);
    w.cost += costOf(t.model, t.usage) ?? 0;
  }
  const windows5h = windows.sort((a, b) => b.fresh - a.fresh || b.tokens - a.tokens || a.start - b.start).slice(0, 3).map((w) => ({ start: new Date(w.start).toISOString(), tokens: w.tokens, fresh: w.fresh, cost: w.cost }));
  const earliest = real.reduce((m, t) => Math.min(m, Date.parse(t.ts)), Infinity);
  const previous = earliest > lo - days * DAY_MS3 + PREVIOUS_SLACK * days * DAY_MS3 ? null : {
    total_cost: costSum(prevTurns),
    tokens_total: prevTurns.reduce((n, t) => n + tokensOf(t.usage), 0),
    fresh_tokens: prevTurns.reduce((n, t) => n + freshOf(t.usage), 0)
  };
  const unpricedShare = allTokens === 0 ? 0 : unknownTokens / allTokens;
  return {
    window_days: days,
    total_cost: total,
    unknown_cost_tokens: unknownTokens,
    tokens_total: allTokens,
    fresh_tokens: inTok + ccTok + outTok,
    unpriced_tokens: unknownTokens,
    approx_priced_tokens: approxTokens,
    by_model: rows(byModel),
    by_project: rows(byProject),
    by_day: rows(byDay),
    busiest_days: days3,
    busiest_days_fresh: days3Fresh,
    tokens_by_kind: { input: inTok, cache_writes: ccTok, cache_reads: crTok, output: outTok },
    subagent_tokens: subagentTokens,
    subagent_tokens_by_kind: subKind,
    main_tokens: allTokens - subagentTokens,
    windows_5h: windows5h,
    main_session_models: rows(mainModels),
    previous,
    pricing: { as_of: PRICING_AS_OF, source_url: PRICING_SOURCE_URL },
    subagent_cost: subagent,
    main_cost: mainCost,
    cache_efficiency: cacheEff,
    tasks: [...taskRows.values()].sort((a, b) => b.cost - a.cost || a.task_class.localeCompare(b.task_class)),
    unattributed,
    waste,
    long_sessions: longSessions,
    rework,
    bloated_sessions: { count: bloatCount, threshold_tokens: bloatThreshold, cost: bloatCost, tokens: bloatTokens },
    avoidable,
    skipped_lines: p.skipped,
    unknown_ratio: unpricedShare,
    top3,
    tips,
    share_line: shareLine(days, total, avoidable.min, avoidable.max, unpricedShare)
  };
}
var DAY_MS3, BLOAT_MIN_TOKENS, BLOAT_MEDIAN_FACTOR, BLOAT_MIN_SESSIONS, LONG_SESSION_MIN_TOKENS, LONG_SESSION_FACTOR, LONG_SESSION_MIN_TURNS, LONG_SESSION_MIN_SESSIONS, PREVIOUS_SLACK, PARTIAL_UNPRICED_SHARE, MAIN_SHARE_MIN, TOP_FAMILY_SHARE_MIN, CACHE_TARGET, MIN_FINDING_TASKS, MIN_FINDING_SAVING, MIN_CACHE_TURNS, MIN_CACHE_SESSIONS, MIN_SCALE_DAYS, CACHE_SAVING_SHARE, MONTH_DAYS, HOUR_MS2, LIMIT_WINDOW_MS, tokensOf, freshOf, inputSide, reprice, costSum;
var init_analyze = __esm({
  "packages/audit/build/analyze.js"() {
    "use strict";
    init_build();
    DAY_MS3 = 864e5;
    BLOAT_MIN_TOKENS = 5e4;
    BLOAT_MEDIAN_FACTOR = 2;
    BLOAT_MIN_SESSIONS = 5;
    LONG_SESSION_MIN_TOKENS = 8e4;
    LONG_SESSION_FACTOR = 2;
    LONG_SESSION_MIN_TURNS = 5;
    LONG_SESSION_MIN_SESSIONS = 3;
    PREVIOUS_SLACK = 0.1;
    PARTIAL_UNPRICED_SHARE = 0.05;
    MAIN_SHARE_MIN = 0.6;
    TOP_FAMILY_SHARE_MIN = 0.5;
    CACHE_TARGET = 0.6;
    MIN_FINDING_TASKS = 3;
    MIN_FINDING_SAVING = 1;
    MIN_CACHE_TURNS = 5;
    MIN_CACHE_SESSIONS = 2;
    MIN_SCALE_DAYS = 7;
    CACHE_SAVING_SHARE = 0.9;
    MONTH_DAYS = 30;
    HOUR_MS2 = 36e5;
    LIMIT_WINDOW_MS = 5 * HOUR_MS2;
    tokensOf = /* @__PURE__ */ __name((u) => u.input_tokens + u.output_tokens + u.cache_creation_input_tokens + u.cache_read_input_tokens, "tokensOf");
    freshOf = /* @__PURE__ */ __name((u) => u.input_tokens + u.output_tokens + u.cache_creation_input_tokens, "freshOf");
    inputSide = /* @__PURE__ */ __name((u) => ({ ...u, output_tokens: 0 }), "inputSide");
    __name(rows, "rows");
    __name(money, "money");
    __name(tokenCount, "tokenCount");
    __name(shareLine, "shareLine");
    __name(median, "median");
    reprice = /* @__PURE__ */ __name((turns, model) => turns.reduce((n, t) => n + (costOf(model, t.usage) ?? 0), 0), "reprice");
    costSum = /* @__PURE__ */ __name((turns) => turns.reduce((n, t) => n + (costOf(t.model, t.usage) ?? 0), 0), "costSum");
    __name(dominantModel, "dominantModel");
    __name(analyze, "analyze");
  }
});

// packages/audit/build/render.js
function shortProjectName(encoded) {
  let best = -1, markerLen = 0;
  for (const m of PATH_MARKERS) {
    const i = encoded.lastIndexOf(m);
    if (i > best && i + m.length < encoded.length) {
      best = i;
      markerLen = m.length;
    }
  }
  if (best >= 0)
    return encoded.slice(best + markerLen);
  const parts = encoded.split("-").filter((p) => p !== "");
  return parts.slice(-2).join("-") || encoded;
}
function anonymize(report, how = {}) {
  const by_project = report.by_project.map((r, i) => ({
    ...r,
    key: how.label ?? (how.decode ? shortProjectName(r.key) : `project-${i + 1}`)
  }));
  return { ...report, by_project };
}
function shares(rows2, total, n, label, plan, models = false) {
  const out = rows2.slice(0, n).map((r) => plan !== "api" ? `${label(r.key)} ${tokenCount(r.fresh)} fresh tok (${pct(r.fresh, total)})` : models && isUnpriced(r.key) ? `${label(r.key)} unpriced (${tokenCount(r.tokens)} tok)` : `${label(r.key)} ${money(r.cost)} (${pct(r.cost, total)})`);
  if (rows2.length > n)
    out.push(`+${rows2.length - n} more`);
  return out;
}
function wrap(text, indent, width = WIDTH) {
  const out = [];
  let line = indent;
  for (const word of text.split(" ")) {
    if (line.trim() !== "" && line.length + 1 + word.length > width) {
      out.push(line);
      line = indent;
    }
    line += (line === indent ? "" : " ") + word;
  }
  out.push(line);
  return out;
}
function formatNote(lines, files) {
  const what = [lines > 0 ? plural2(lines, "line") : "", files > 0 ? plural2(files, "file") : ""].filter((x) => x !== "").join(" and ");
  return `Note: ${what} could not be read or have an unknown format \u2014 Claude Code may have changed its log format; numbers may be incomplete.`;
}
function warnings(report, parse) {
  const out = [];
  const files = parse?.files_failed ?? 0;
  const bad = parse ? parse.skipped + (parse.suspicious_usage ?? 0) : report.skipped_lines;
  const ratio = parse && parse.lines > 0 ? bad / parse.lines : 0;
  if (bad > 0 && ratio > FORMAT_RATIO || files > 0)
    out.push(formatNote(bad, files));
  if (report.unknown_ratio > WARN_RATIO) {
    out.push(`Note: ${Math.round(report.unknown_ratio * 100)}% of tokens used models Horsy has no price for and are not costed; totals may be understated.`);
  }
  if (report.approx_priced_tokens > 0) {
    out.push(`Note: ${pct(report.approx_priced_tokens, report.tokens_total)} of tokens priced at the nearest known model (their exact id is not in the price table).`);
  }
  return out;
}
function planShareLine(r, plan) {
  return plan === "api" ? r.share_line : shareLine(r.window_days, r.total_cost, r.avoidable.min, r.avoidable.max, r.unknown_ratio, r.fresh_tokens);
}
function expensiveLabel(r) {
  const fams = new Set(r.tasks.filter((t) => SAFE_CLASSES.includes(t.task_class)).map((t) => modelFamily(t.model)));
  return fams.has("fable") ? fams.has("opus") ? "Opus/Fable" : "Fable" : "Opus";
}
function costLessLines(r, plan) {
  const out = [];
  const api = plan !== "api" ? " at API prices" : "";
  const lo = money(r.waste.min), hi = money(r.waste.max);
  const haiku = lo === hi ? "" : ` (Haiku: unproven in your repo, up to ~${hi})`;
  const label = expensiveLabel(r);
  out.push(r.waste.tasks <= 0 ? `No simple subagent tasks ran on ${label}: nothing obvious to downgrade.` : plan === "api" ? `~${lo} on ${plural2(r.waste.tasks, "simple subagent task")} that ran on ${label}, if rerun on Sonnet${haiku}; assumes equal quality` : `${plural2(r.waste.tasks, "simple subagent task")} (${tokenCount(r.waste.fresh)} fresh tokens) ran on ${label}. On Sonnet: ~${lo} less${api}${haiku}; assumes equal quality`);
  const amount = /* @__PURE__ */ __name((cost, tokens) => plan === "api" ? money(cost) : `${tokenCount(tokens)} fresh tokens (~${money(cost)}${api})`, "amount");
  if (r.rework.cost > 0) {
    const k = r.rework.tasks;
    out.push(`+ ${amount(r.rework.cost, r.rework.fresh)} rework: ${plural2(k, "task")} ${k === 1 ? "failed or was" : "failed or were"} dispatched more than once`);
  }
  const b = r.bloated_sessions;
  if (b.count > 0)
    out.push(`+ ${amount(b.cost, b.tokens)} context: ${plural2(b.count, "session")} started with > ${tokenCount(b.threshold_tokens)} tokens (cache writes above ${tokenCount(b.threshold_tokens)})`);
  return out;
}
function contextLine(r, opts) {
  const w = opts.window;
  const base = w ? `${plural2(w.sessions, "session")} \xB7 ${plural2(w.projects, "project")} \xB7 ${w.from} \u2192 ${w.to}` : `${plural2(r.by_project.length, "project")}`;
  const skipped2 = opts.parse?.skipped ?? r.skipped_lines;
  const invalid = opts.parse?.suspicious_usage ?? 0;
  return base + (skipped2 > 0 ? ` \xB7 ${plural2(skipped2, "line")} skipped` : "") + (invalid > 0 ? ` \xB7 ${plural2(invalid, "line")} with invalid usage numbers` : "");
}
function trend(r, plan) {
  if (!r.previous)
    return null;
  const span = `vs previous ${plural2(r.window_days, "day")}`;
  if (plan === "api")
    return r.previous.total_cost > 0 ? { text: `${signed(r.total_cost, r.previous.total_cost).text} ${span}` } : null;
  if (!(r.previous.fresh_tokens > 0))
    return null;
  const tok2 = signed(r.fresh_tokens, r.previous.fresh_tokens);
  if (!(r.previous.total_cost > 0))
    return { text: `fresh tokens ${tok2.text} ${span}` };
  const usd2 = signed(r.total_cost, r.previous.total_cost);
  const gap = tok2.d - usd2.d;
  return {
    text: `fresh tokens ${tok2.text} \xB7 $ ${usd2.text} ${span}`,
    ...gap > TREND_NOTE_POINTS ? { note: "Fresh tokens grew faster than dollars: a cheaper model mix, or fewer cache reads." } : gap < -TREND_NOTE_POINTS ? { note: "Dollars grew faster than fresh tokens: a pricier model mix, or more cache reads." } : {}
  };
}
function spendText(r, plan) {
  const lead = plan === "api" ? `~${money(r.total_cost)} API-equivalent \xB7 ${tokenCount(r.tokens_total)} tokens` : `${tokenCount(r.fresh_tokens)} fresh tokens (input + output + cache writes) \xB7 ${tokenCount(r.tokens_by_kind.cache_reads)} cache reads (\u2248 ${money(r.total_cost)} at API list prices)`;
  const t = trend(r, plan);
  return t ? `${lead} \xB7 ${t.text}` : lead;
}
function shareGap(r) {
  const sf = subFresh(r), mf = r.fresh_tokens - sf;
  if (!(r.total_cost > 0) || !(r.fresh_tokens > 0) || !(sf > 0) || !(mf > 0))
    return null;
  const d = r.subagent_cost / r.total_cost, t = sf / r.fresh_tokens;
  if (Math.abs(d - t) <= SHARE_GAP)
    return null;
  const all = r.tokens_by_kind, sub = r.subagent_tokens_by_kind;
  const readShare = /* @__PURE__ */ __name((reads2, tokens) => tokens > 0 ? reads2 / tokens : 0, "readShare");
  const reads = readShare(sub.cache_reads, r.subagent_tokens) - readShare(all.cache_reads - sub.cache_reads, r.main_tokens);
  const why = d > t && reads > CAUSE_GAP ? ": they re-read more cached context" : "";
  return `Subagents are ${pct(r.subagent_cost, r.total_cost)} of dollars but ${pct(sf, r.fresh_tokens)} of fresh tokens${why}. Both shares include reworked tasks and context re-reads.`;
}
function spendNotes(r, plan) {
  const [part, whole] = plan === "api" ? [r.subagent_cost, r.total_cost] : [subFresh(r), r.fresh_tokens];
  const out = [];
  if (whole > 0 && part / whole < SUBAGENT_SHARE_MAX)
    out.push(`Subagents are ${pct(part, whole)} of your spend: Horsy's routing can only change that part.`);
  out.push(plan === "api" ? API_NOTE : MAX_NOTE);
  if (plan !== "api")
    out.push(CACHE_READ_NOTE);
  const gap = plan === "api" ? null : shareGap(r);
  if (gap)
    out.push(gap);
  const note = trend(r, plan)?.note;
  if (note)
    out.push(note);
  return out;
}
function facts(r, plan) {
  const api = plan === "api";
  const total = api ? r.total_cost : r.fresh_tokens;
  const sf = subFresh(r);
  const out = [{ label: "Models", items: shares(r.by_model, total, MODEL_ROWS, modelName, plan, true) }];
  if (!api) {
    const k = r.tokens_by_kind;
    out.push({ label: "Tokens", items: [`input ${tokenCount(k.input)} \xB7 cache writes ${tokenCount(k.cache_writes)} \xB7 cache reads ${tokenCount(k.cache_reads)} \xB7 output ${tokenCount(k.output)}`] });
  }
  out.push({ label: "Subagents", items: [api ? `${pct(r.subagent_cost, r.total_cost)} of spend (${money(r.subagent_cost)})` : `${tokenCount(sf)} fresh tok (${pct(sf, r.fresh_tokens)}, \u2248 ${money(r.subagent_cost)} at API prices)`] });
  if (r.main_session_models.length > 0) {
    out.push({
      label: "Main sessions",
      lead: api ? `${pct(r.main_cost, r.total_cost)} of spend (${money(r.main_cost)})` : `${tokenCount(r.fresh_tokens - sf)} fresh tok (${pct(r.fresh_tokens - sf, r.fresh_tokens)}, \u2248 ${money(r.main_cost)} at API prices)`,
      items: shares(r.main_session_models, api ? r.main_cost : r.fresh_tokens - sf, MODEL_ROWS, modelName, plan, true)
    });
  }
  out.push({ label: "Cache", items: [cacheText(r)] });
  if (r.by_project.length > 0) {
    const labelled = r.by_project.length > 1 && r.by_project.some((p) => /^project-\d+$/.test(p.key));
    out.push({ label: "Projects", items: shares(r.by_project, total, PROJECT_ROWS, clean, plan), ...labelled ? { tail: SHOW_NAMES_HINT } : {} });
  }
  if (r.busiest_days.length > 0) {
    const days = api ? r.busiest_days : r.busiest_days_fresh;
    out.push({ label: "Busiest days", items: days.map((d) => `${clean(d.day)} ${api ? `${tok(d.tokens)} (${d.cost > 0 ? money(d.cost) : "unpriced"})` : freshReads(d)}`) });
  }
  if (!api && r.windows_5h.length > 0)
    out.push({ label: "Busiest 5-hour windows", items: r.windows_5h.map(windowText), tail: WINDOWS_HINT });
  return out;
}
function taskLines(r) {
  const out = r.tasks.slice(0, TASK_ROWS).map((t) => ({ cls: clean(t.task_class), model: modelName(t.model), count: String(t.count), fresh: t.fresh, cost: taskCost(t) }));
  if (r.unattributed.cost > UNATTRIBUTED_SHARE * r.subagent_cost && r.unattributed.cost > 0)
    out.push({ cls: NO_DESCRIPTION, model: "", count: "", fresh: r.unattributed.fresh, cost: money(r.unattributed.cost) });
  return out;
}
function renderTerminal(r, opts) {
  const plan = opts.plan ?? "api";
  const sty = /* @__PURE__ */ __name((code) => (s) => opts.color ? `\x1B[${code}m${s}\x1B[0m` : s, "sty");
  const bold = sty("1"), dim = sty("2"), yellow = sty("33");
  const L = [];
  L.push(bold(`Horsy audit \u2014 last ${r.window_days} days (ESTIMATE)`));
  L.push(dim(DISCLAIMER));
  L.push(dim(contextLine(r, opts)));
  if (opts.demo)
    L.push(yellow(DEMO_BANNER));
  L.push("");
  const spend = wrap(spendText(r, plan), " ".repeat("SPEND  ".length));
  spend.forEach((l, i) => L.push(i === 0 ? `${bold("SPEND")}  ${bold(l.trimStart())}` : bold(l)));
  for (const n of spendNotes(r, plan))
    L.push(...wrap(n, "  ").map(dim));
  for (const f of facts(r, plan)) {
    if (f.label.length >= LABEL_WIDTH) {
      L.push(`  ${f.label}${f.tail ? ` ${f.tail}` : ""}`, ...f.items.map((x) => `    ${x}`));
      continue;
    }
    const text = factText(f, `${NBSP}\xB7 `, nb);
    L.push(...wrap(text, " ".repeat(2 + LABEL_WIDTH)).map((l, i) => (i === 0 ? `  ${f.label.padEnd(LABEL_WIDTH)}${l.trimStart()}` : l).replace(/\u00a0/g, " ")));
  }
  L.push("");
  L.push(bold("DELEGATED TASKS"));
  const rows2 = taskLines(r);
  if (r.tasks.length === 0 && rows2.length === 0)
    L.push("  none: no subagent (Agent) calls in this window");
  else {
    const cw = Math.max("class (guessed)".length, ...rows2.map((t) => t.cls.length));
    const mw = Math.max("model".length, ...rows2.map((t) => t.model.length));
    const kw = Math.max(plan === "api" ? 6 : "at API prices".length, ...rows2.map((t) => t.cost.length));
    const tw = Math.max("fresh tok".length, ...rows2.map((t) => tokenCount(t.fresh).length));
    const cols = /* @__PURE__ */ __name((t) => (plan === "api" ? "" : `  ${tokenCount(t.fresh).padStart(tw)}`) + `  ${t.cost.padStart(kw)}`, "cols");
    L.push(dim(`  ${"class (guessed)".padEnd(cw)}  ${"model".padEnd(mw)}  tasks${plan === "api" ? `  ${"cost".padStart(kw)}` : `  ${"fresh tok".padStart(tw)}  ${"at API prices".padStart(kw)}`}`));
    rows2.forEach((t) => L.push(`  ${t.cls.padEnd(cw)}  ${t.model.padEnd(mw)}  ${t.count.padStart(5)}${cols(t)}`));
  }
  if (r.tasks.length > TASK_ROWS)
    L.push(dim(`  +${r.tasks.length - TASK_ROWS} more (use --json for all)`));
  if (r.tasks.length > 0)
    L.push(dim(`  ${LEGEND}`));
  L.push("");
  L.push(bold("COULD COST LESS"));
  for (const l of costLessLines(r, plan))
    L.push(...wrap(l, "  "));
  L.push("");
  L.push(bold("TOP THINGS TO CHANGE") + dim("  (savings are estimates, scaled to 30 days)"));
  if (r.top3.length === 0)
    L.push(`  ${NOTHING}`);
  r.top3.forEach((f, i) => {
    L.push(...bullet(` ${i + 1}. `, f.title));
    const s = saving(f, plan);
    L.push(...wrap(`\u2192 ${f.action}${s ? ` (est.\xA0saving\xA0${s.replace(/ /g, "\xA0")})` : ""}`, "    ").map((l) => l.replace(/\u00a0/g, " ")));
  });
  if (r.top3.length > 0 && r.top3.length < 3)
    L.push(`  Only ${plural2(r.top3.length, "finding")} stood out.`);
  if (r.tips.length > 0) {
    L.push("", bold("TIPS"));
    for (const f of r.tips) {
      L.push(...bullet("  - ", f.title));
      L.push(...wrap(`\u2192 ${f.action}`, "    "));
    }
  }
  L.push("");
  for (const wl of warnings(r, parseFor(opts)))
    L.push(...wrap(wl, "").map(yellow));
  L.push(...wrap(priceFooter(r), "").map(dim));
  L.push(`Share: ${r.share_line}`);
  return L.join("\n") + "\n";
}
function renderMarkdown(r, opts) {
  const plan = opts.plan ?? "api";
  const L = [];
  L.push(`# Horsy audit \u2014 last ${r.window_days} days (estimate)`, "", `_${DISCLAIMER}_`, "", contextLine(r, opts));
  if (opts.demo)
    L.push("", `> ${DEMO_BANNER}`);
  L.push("", `## Spend: ${spendText(r, plan)}`, "");
  for (const n of spendNotes(r, plan))
    L.push(`_${n}_`, "");
  for (const f of facts(r, plan))
    L.push(`- **${f.label}:** ${factText(f, " \xB7 ", (x) => x)}`);
  L.push("", "## Delegated tasks", "");
  const lines = taskLines(r);
  if (r.tasks.length === 0 && lines.length === 0)
    L.push("No subagent (Agent) calls in this window.");
  else {
    L.push(plan === "api" ? "| Class (guessed) | Model | Tasks | Cost |" : "| Class (guessed) | Model | Tasks | Fresh tokens | At API prices |", plan === "api" ? "| --- | --- | ---: | ---: |" : "| --- | --- | ---: | ---: | ---: |");
    for (const t of lines)
      L.push(`| ${t.cls} | ${t.model} | ${t.count} | ${plan === "api" ? "" : `${tokenCount(t.fresh)} | `}${t.cost} |`);
    if (r.tasks.length > TASK_ROWS)
      L.push("", `+${r.tasks.length - TASK_ROWS} more (use \`--json\` for all)`);
    L.push("", `_${LEGEND}_`);
  }
  L.push("", "## Could cost less", "");
  for (const l of costLessLines(r, plan))
    L.push(`- ${l}`);
  L.push("", "## Top things to change", "", "_Savings are estimates, scaled to 30 days._", "");
  if (r.top3.length === 0)
    L.push(NOTHING);
  r.top3.forEach((f, i) => {
    const s = saving(f, plan);
    L.push(`${i + 1}. **${f.title}**`, `   \u2192 ${f.action}${s ? ` (est. saving ${s})` : ""}`);
  });
  if (r.top3.length > 0 && r.top3.length < 3)
    L.push("", `Only ${plural2(r.top3.length, "finding")} stood out.`);
  if (r.tips.length > 0) {
    L.push("", "## Tips", "");
    for (const f of r.tips)
      L.push(`- **${f.title}**`, `  \u2192 ${f.action}`);
  }
  for (const wl of warnings(r, parseFor(opts)))
    L.push("", `> ${wl}`);
  L.push("", `_${priceFooter(r)}_`, "", `**Share:** ${r.share_line}`);
  return L.join("\n") + "\n";
}
function renderJson(r, meta = { demo: false, files_failed: 0 }) {
  const out = {
    estimate: true,
    demo: meta.demo,
    files_failed: meta.files_failed,
    window: meta.window ?? null,
    session_fingerprints: meta.session_fingerprints ?? [],
    ...r,
    total_cost: round(r.total_cost),
    subagent_cost: round(r.subagent_cost),
    main_cost: round(r.main_cost),
    by_model: roundRows(r.by_model),
    by_project: roundRows(r.by_project),
    by_day: roundRows(r.by_day).sort((a, b) => a.key.localeCompare(b.key)),
    tasks: r.tasks.map((t) => ({ ...t, cost: round(t.cost) })),
    waste: { ...r.waste, min: round(r.waste.min), max: round(r.waste.max) },
    rework: { ...r.rework, cost: round(r.rework.cost) },
    bloated_sessions: { ...r.bloated_sessions, cost: round(r.bloated_sessions.cost) }
  };
  return JSON.stringify(out, null, 2) + "\n";
}
var DEMO_BANNER, API_NOTE, MAX_NOTE, CACHE_READ_NOTE, SUBAGENT_SHARE_MAX, TREND_NOTE_POINTS, SHARE_GAP, CAUSE_GAP, DISCLAIMER, LEGEND, SHOW_NAMES_HINT, NOTHING, WIDTH, WARN_RATIO, FORMAT_RATIO, LABEL_WIDTH, TASK_ROWS, MODEL_ROWS, PROJECT_ROWS, JSON_DECIMALS, PATH_MARKERS, plural2, clean, modelName, NBSP, nb, pct, isUnpriced, cacheText, parseFor, priceFooter, saving, signed, subFresh, factText, tok, freshReads, windowText, WINDOWS_HINT, taskCost, UNATTRIBUTED_SHARE, NO_DESCRIPTION, bullet, round, roundRows;
var init_render = __esm({
  "packages/audit/build/render.js"() {
    "use strict";
    init_build();
    init_analyze();
    DEMO_BANNER = "Demo data (synthetic) \u2014 run without --demo to audit your own sessions";
    API_NOTE = "Paying per token? These are your dollars. On Max/Pro you pay a flat fee: rerun with --plan max to see tokens.";
    MAX_NOTE = "Max/Pro limits work on rolling 5-hour windows; these are estimated from this machine's transcripts. Dollars here are what the same usage would cost at API prices.";
    CACHE_READ_NOTE = "Cache reads are re-reads of context; how they count against plan limits is not published.";
    SUBAGENT_SHARE_MAX = 0.25;
    TREND_NOTE_POINTS = 20;
    SHARE_GAP = 0.1;
    CAUSE_GAP = 0.05;
    DISCLAIMER = "Estimate: your local Claude Code transcripts priced at API list rates. Nothing leaves this machine.";
    LEGEND = "class is guessed from the task description; -wide = touches many files";
    SHOW_NAMES_HINT = "(use --show-names to see which is which)";
    NOTHING = "Nothing stands out \u2014 your setup looks efficient for this period.";
    WIDTH = 100;
    WARN_RATIO = 0.05;
    FORMAT_RATIO = 0.01;
    LABEL_WIDTH = 15;
    TASK_ROWS = 5;
    MODEL_ROWS = 3;
    PROJECT_ROWS = 4;
    JSON_DECIMALS = 1e6;
    PATH_MARKERS = ["-dev-", "-src-", "-code-", "-projects-", "-repos-", "-Documents-"];
    __name(shortProjectName, "shortProjectName");
    __name(anonymize, "anonymize");
    plural2 = /* @__PURE__ */ __name((n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`, "plural");
    clean = /* @__PURE__ */ __name((s) => [...s].filter((ch) => {
      const c = ch.charCodeAt(0);
      return c > 31 && (c < 127 || c > 159);
    }).join(""), "clean");
    modelName = /* @__PURE__ */ __name((m) => clean(m).replace(/^claude-/, ""), "modelName");
    NBSP = "\xA0";
    nb = /* @__PURE__ */ __name((s) => s.replace(/ /g, NBSP), "nb");
    pct = /* @__PURE__ */ __name((part, whole) => {
      if (whole <= 0)
        return "0%";
      const p = part / whole * 100;
      return p > 0 && p < 1 ? "<1%" : `${Math.round(p)}%`;
    }, "pct");
    isUnpriced = /* @__PURE__ */ __name((model) => priceFor(model) === null, "isUnpriced");
    __name(shares, "shares");
    cacheText = /* @__PURE__ */ __name((r) => r.cache_efficiency === null ? "n/a (no input tokens)" : `${Math.round(r.cache_efficiency * 100)}% of input came from cache (higher is better)`, "cacheText");
    __name(wrap, "wrap");
    __name(formatNote, "formatNote");
    __name(warnings, "warnings");
    parseFor = /* @__PURE__ */ __name((o) => o.demo && o.parse ? { ...o.parse, skipped: 0, suspicious_usage: 0 } : o.parse, "parseFor");
    priceFooter = /* @__PURE__ */ __name((r) => `Prices: Anthropic API list prices as of ${r.pricing.as_of} (${r.pricing.source_url})`, "priceFooter");
    __name(planShareLine, "planShareLine");
    saving = /* @__PURE__ */ __name((f, plan) => f.est_monthly_saving === null ? null : `${f.saving_is_upper_bound ? "up to " : ""}~${money(f.est_monthly_saving)}/month${plan === "api" ? "" : " at API prices"}`, "saving");
    __name(expensiveLabel, "expensiveLabel");
    __name(costLessLines, "costLessLines");
    __name(contextLine, "contextLine");
    signed = /* @__PURE__ */ __name((cur, prev) => {
      const d = Math.round((cur / prev - 1) * 100);
      return { d, text: `${d > 0 ? "+" : d < 0 ? "-" : "\xB1"}${Math.abs(d)}%` };
    }, "signed");
    __name(trend, "trend");
    __name(spendText, "spendText");
    subFresh = /* @__PURE__ */ __name((r) => r.subagent_tokens_by_kind.input + r.subagent_tokens_by_kind.cache_writes + r.subagent_tokens_by_kind.output, "subFresh");
    __name(shareGap, "shareGap");
    __name(spendNotes, "spendNotes");
    factText = /* @__PURE__ */ __name((f, sep7, glue) => `${f.lead ? `${f.lead}: ` : ""}${f.items.map(glue).join(sep7)}${f.tail ? ` ${glue(f.tail)}` : ""}`, "factText");
    tok = /* @__PURE__ */ __name((n) => `${tokenCount(n)} tok`, "tok");
    freshReads = /* @__PURE__ */ __name((x) => `${tokenCount(x.fresh)} fresh (${tokenCount(x.tokens - x.fresh)} cache reads)`, "freshReads");
    windowText = /* @__PURE__ */ __name((w) => `${w.start.slice(0, 10)} ${w.start.slice(11, 13)}:00 +5h ${freshReads(w)}`, "windowText");
    WINDOWS_HINT = "(UTC, approximate: this machine only; your real reset may differ)";
    __name(facts, "facts");
    taskCost = /* @__PURE__ */ __name((t) => isUnpriced(t.model) ? "unpriced" : money(t.cost), "taskCost");
    UNATTRIBUTED_SHARE = 0.01;
    NO_DESCRIPTION = "(no description)";
    __name(taskLines, "taskLines");
    bullet = /* @__PURE__ */ __name((head, text) => wrap(text, "    ").map((l, j) => j === 0 ? `${head}${l.trim()}` : l), "bullet");
    __name(renderTerminal, "renderTerminal");
    __name(renderMarkdown, "renderMarkdown");
    round = /* @__PURE__ */ __name((x) => Math.round(x * JSON_DECIMALS) / JSON_DECIMALS, "round");
    roundRows = /* @__PURE__ */ __name((rows2) => rows2.map((x) => ({ ...x, cost: round(x.cost) })), "roundRows");
    __name(renderJson, "renderJson");
  }
});

// packages/audit/build/merge.js
import { createHash as createHash4 } from "node:crypto";
import { readFileSync as readFileSync14 } from "node:fs";
import { basename as basename2, resolve as resolve2 } from "node:path";
function toReport(v2, file) {
  if (!isObj6(v2) || v2["estimate"] !== true)
    return null;
  const { total_cost, tokens_total, subagent_cost, avoidable, rework, bloated_sessions, by_model } = v2;
  if (!isNum(total_cost) || !isNum(tokens_total) || !isNum(subagent_cost) || !Array.isArray(by_model))
    return null;
  if (!isObj6(avoidable) || !isNum(avoidable["min"]) || !isNum(avoidable["max"]))
    return null;
  if (!isObj6(rework) || !isNum(rework["tasks"]) || !isNum(rework["cost"]) || !isNum(rework["tokens"]))
    return null;
  if (!isObj6(bloated_sessions) || !isNum(bloated_sessions["count"]) || !isNum(bloated_sessions["cost"]) || !isNum(bloated_sessions["tokens"]))
    return null;
  const models = [];
  for (const m of by_model) {
    if (!isObj6(m) || typeof m["key"] !== "string" || !isNum(m["cost"]) || !isNum(m["tokens"]))
      return null;
    models.push({ key: m["key"], cost: m["cost"], tokens: m["tokens"] });
  }
  const fp = v2["session_fingerprints"];
  return {
    sessions: new Set(Array.isArray(fp) ? fp.filter((x) => typeof x === "string") : []),
    file,
    window_days: isNum(v2["window_days"]) ? v2["window_days"] : null,
    demo: v2["demo"] === true,
    total_cost,
    tokens_total,
    fresh_tokens: isNum(v2["fresh_tokens"]) ? v2["fresh_tokens"] : null,
    subagent_cost,
    avoidable: { min: avoidable["min"], max: avoidable["max"] },
    rework: { tasks: rework["tasks"], cost: rework["cost"], tokens: rework["tokens"] },
    context: { sessions: bloated_sessions["count"], cost: bloated_sessions["cost"], tokens: bloated_sessions["tokens"] },
    by_model: models
  };
}
function sumModels(rs) {
  const m = /* @__PURE__ */ new Map();
  for (const r of rs)
    for (const x of r.by_model) {
      const cur = m.get(x.key) ?? { cost: 0, tokens: 0 };
      cur.cost += x.cost;
      cur.tokens += x.tokens;
      m.set(x.key, cur);
    }
  return [...m].map(([key, v2]) => ({ key, ...v2 })).sort((a, b) => b.cost - a.cost || a.key.localeCompare(b.key));
}
async function mergeCommand(argv, io) {
  const files = [];
  let json = false, allowMixed = false;
  for (const a of argv) {
    if (a === "--help" || a === "-h") {
      io.stdout.write(USAGE);
      return 0;
    }
    if (a === "--json")
      json = true;
    else if (a === "--allow-mixed")
      allowMixed = true;
    else if (a.startsWith("--")) {
      io.stderr.write(`horsy merge: unknown option "${a}"

${USAGE}`);
      return 2;
    } else
      files.push(a);
  }
  if (files.length === 0) {
    io.stderr.write(`horsy merge: no report files given

${USAGE}`);
    return 2;
  }
  const reports = [];
  const seen = /* @__PURE__ */ new Set();
  const byContent = /* @__PURE__ */ new Map();
  for (const [i, f] of files.entries()) {
    if (seen.has(resolve2(f))) {
      io.stderr.write(`horsy merge: ${f} was passed twice; counting it once
`);
      continue;
    }
    seen.add(resolve2(f));
    let text;
    try {
      text = readFileSync14(f, "utf8");
    } catch {
      io.stderr.write(`horsy merge: cannot read ${f}
`);
      return 2;
    }
    const hash = createHash4("sha256").update(text).digest("hex");
    const first = byContent.get(hash);
    if (first !== void 0) {
      const [a, b] = first.given === f ? [`${basename2(first.given)} (#${first.pos})`, `${basename2(f)} (#${i + 1})`] : [first.given, f];
      io.stderr.write(`warning: ${a} and ${b} are identical; counted once
`);
      continue;
    }
    byContent.set(hash, { given: f, pos: i + 1 });
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      io.stderr.write(`horsy merge: ${f} is not valid JSON (${FILE_HELP})
`);
      return 2;
    }
    const r = toReport(parsed, basename2(f));
    if (!r) {
      io.stderr.write(`horsy merge: ${f} is not a horsy audit JSON report (${FILE_HELP})
`);
      return 2;
    }
    reports.push(r);
  }
  const windows = [...new Set(reports.flatMap((r) => r.window_days === null ? [] : [r.window_days]))].sort((a, b) => a - b);
  if (windows.length > 1 && !allowMixed) {
    io.stderr.write(`horsy merge: the reports cover different windows (${windows.length === 2 ? windows.join(" and ") : windows.join(", ")} days); adding them up would mislead. Rerun the audits with the same --days, or pass --allow-mixed.
`);
    return 2;
  }
  for (const [i, a] of reports.entries())
    for (const b of reports.slice(i + 1)) {
      const shared = [...a.sessions].filter((x) => b.sessions.has(x)).length;
      if (shared > 0)
        io.stderr.write(`warning: ${a.file} and ${b.file} share ${plural3(shared, "session")} (same install, overlapping time); merged, those sessions count twice
`);
    }
  const total = sum(reports, (r) => r.total_cost);
  const tokens = sum(reports, (r) => r.tokens_total);
  const fresh = reports.every((r) => r.fresh_tokens !== null) ? sum(reports, (r) => r.fresh_tokens ?? 0) : null;
  const subagent = sum(reports, (r) => r.subagent_cost);
  const avoidable = { min: sum(reports, (r) => r.avoidable.min), max: sum(reports, (r) => r.avoidable.max) };
  const rework = { tasks: sum(reports, (r) => r.rework.tasks), cost: sum(reports, (r) => r.rework.cost), tokens: sum(reports, (r) => r.rework.tokens) };
  const context = { sessions: sum(reports, (r) => r.context.sessions), cost: sum(reports, (r) => r.context.cost), tokens: sum(reports, (r) => r.context.tokens) };
  const models = sumModels(reports);
  if (json) {
    const out2 = {
      estimate: true,
      reports: reports.length,
      window_days: windows,
      total_cost: total,
      tokens_total: tokens,
      fresh_tokens: fresh,
      subagent_cost: subagent,
      subagent_share: total > 0 ? subagent / total : 0,
      could_cost_less: avoidable,
      rework,
      context,
      by_model: models,
      per_report: reports.map((r) => ({ file: r.file, total_cost: r.total_cost, tokens_total: r.tokens_total, subagent_cost: r.subagent_cost, could_cost_less: r.avoidable }))
    };
    io.stdout.write(`${JSON.stringify(out2, null, 2)}
`);
    return 0;
  }
  const row2 = /* @__PURE__ */ __name((label, text) => `  ${label.padEnd(LABEL_WIDTH2)}${text}
`, "row");
  const shown = models.slice(0, MODEL_ROWS2).map((m) => m.cost === 0 && m.tokens > 0 ? `${modelName(m.key)} unpriced (${tokenCount(m.tokens)} tok)` : `${modelName(m.key)} ${money(m.cost)} (${pct(m.cost, total)})`);
  if (models.length > MODEL_ROWS2)
    shown.push(`+${models.length - MODEL_ROWS2} more`);
  let out = `Horsy team audit \u2014 ${plural3(reports.length, "report")} (ESTIMATE)
`;
  out += "Merged on this machine from the files you named. Nothing was uploaded.\n";
  if (reports.some((r) => r.demo))
    out += "Includes demo data (synthetic).\n";
  if (windows.length > 1)
    out += `Note: the reports cover different windows (${windows.length === 2 ? windows.join(" and ") : windows.join(", ")} days); totals add them up as they are.
`;
  out += `
TEAM SPEND  ~${money(total)} API-equivalent \xB7 ${tokenCount(tokens)} tokens${fresh === null ? "" : ` (${tokenCount(fresh)} fresh)`}
`;
  out += row2("Models", shown.join(" \xB7 "));
  out += row2("Subagents", `${pct(subagent, total)} of spend (${money(subagent)})`);
  out += row2("Could cost less", range(avoidable.min, avoidable.max));
  out += row2("", "(each report's own estimate, added up)");
  out += row2("Rework", `${plural3(rework.tasks, "task")}, ${tokenCount(rework.tokens)} tokens (${money(rework.cost)})`);
  out += row2("Context", `${plural3(context.sessions, "session")}, ${tokenCount(context.tokens)} tokens (${money(context.cost)})`);
  const names = reports.map((r) => clean(r.file));
  const w = Math.max("report".length, ...names.map((n) => n.length));
  out += `
PER REPORT
  ${"report".padEnd(w)}  ${"cost".padStart(8)}  ${"tokens".padStart(7)}  subagents  could cost less
`;
  reports.forEach((r, i) => {
    out += `  ${names[i].padEnd(w)}  ${money(r.total_cost).padStart(8)}  ${tokenCount(r.tokens_total).padStart(7)}  ${pct(r.subagent_cost, r.total_cost).padStart(9)}  ${range(r.avoidable.min, r.avoidable.max)}
`;
  });
  io.stdout.write(out);
  return 0;
}
var USAGE, MODEL_ROWS2, LABEL_WIDTH2, FILE_HELP, isObj6, isNum, sum, range, plural3;
var init_merge = __esm({
  "packages/audit/build/merge.js"() {
    "use strict";
    init_analyze();
    init_render();
    USAGE = `Usage: horsy merge <report.json> [...] [--json] [--allow-mixed]

Combine "horsy audit --json" files (one per person) into a team view: total cost and tokens, by model,
subagent share, what could cost less, rework and context totals, and one row per file. Reads the files
you name and nothing else; nothing is uploaded. Rows are labelled by file name only.
Reports must cover the same number of days (add them up otherwise at your own risk: --allow-mixed).
Two reports from one machine that overlap in time are flagged: merged, they count the same sessions twice.

  --json         Machine-readable result
  --allow-mixed  Merge reports that cover different numbers of days
  --help         Show this help
`;
    MODEL_ROWS2 = 3;
    LABEL_WIDTH2 = 17;
    FILE_HELP = "make one with: horsy audit --json > <name>.json";
    isObj6 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    isNum = /* @__PURE__ */ __name((v2) => typeof v2 === "number" && Number.isFinite(v2), "isNum");
    __name(toReport, "toReport");
    sum = /* @__PURE__ */ __name((rs, f) => rs.reduce((n, r) => n + f(r), 0), "sum");
    range = /* @__PURE__ */ __name((lo, hi) => money(lo) === money(hi) ? `~${money(lo)}` : `~${money(lo)} (Haiku: unproven in your repo, up to ~${money(hi)})`, "range");
    plural3 = /* @__PURE__ */ __name((n, one) => `${n} ${n === 1 ? one : `${one}s`}`, "plural");
    __name(sumModels, "sumModels");
    __name(mergeCommand, "mergeCommand");
  }
});

// packages/audit/build/commands.js
import { homedir as homedir3 } from "node:os";
import { join as join15, sep as sep6 } from "node:path";
async function reconcileCommand(argv, io) {
  let all = false;
  let repo = process.cwd();
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--now")
      all = true;
    else if (a === "--repo" && argv[i + 1] !== void 0)
      repo = argv[++i];
    else if (a === "--help" || a === "-h") {
      io.stdout.write(USAGE2);
      return 0;
    } else {
      io.stderr.write(`horsy reconcile: unknown option "${a}"

${USAGE2}`);
      return 2;
    }
  }
  const root2 = repoRoot(repo);
  if (!root2) {
    io.stdout.write("Not a git repository: nothing to reconcile.\n");
    return 0;
  }
  const fresh = reconcile(root2, readEvents(), { now: /* @__PURE__ */ new Date(), windowHours: 24, all });
  const n = /* @__PURE__ */ __name((r) => fresh.filter((e) => e.type === "outcome_delayed" && e.result === r).length, "n");
  for (const e of fresh)
    appendEvent(e);
  io.stdout.write(`Reconciled ${fresh.length} outcomes: ${n("kept")} kept, ${n("reverted")} reverted, ${n("reworked")} reworked, ${n("no_signal")} no signal${all ? "" : " (git, last 24 h window)"}
`);
  return 0;
}
function tilde(p) {
  const home = homedir3();
  return p.startsWith(home + sep6) ? `~${p.slice(home.length)}` : p;
}
function decisionLine(cls, d) {
  const def = d.default_arm.model;
  const known = Number.isFinite(rank(def));
  const ap = d.apply_arm === void 0 ? d.arm : d.apply_arm;
  const cheaper = known && ap !== null && rank(ap.model) < rank(def);
  const left = /* @__PURE__ */ __name((what) => `${cls}: ${what}; Horsy left it${cheaper && ap ? ` (would pick ${ap.model})` : ""}`, "left");
  if (d.decider === "respect_explicit")
    return left(`agent chose ${def}`);
  if (d.decider === "force_env")
    return left(`forced to ${def} by env`);
  if (d.decider === "env_pin")
    return left(`pinned to ${def} by env`);
  if (known && Number.isFinite(rank(d.arm.model)) && rank(d.arm.model) > rank(def))
    return `${cls}: keeps ${def} (Horsy never routes up)`;
  if (d.mode === "shadow" && !d.applied && d.apply_arm !== void 0 && known && rank(d.arm.model) < rank(def)) {
    if (d.apply_arm)
      return `${cls}: would apply ${d.apply_arm.model} now${rank(d.arm.model) < rank(d.apply_arm.model) ? ` (${d.arm.model}: unproven)` : ""}; default ${def}`;
    const why2 = d.reason.split(";").find((p) => p.startsWith("active:"))?.slice("active:".length);
    const text = why2 === void 0 || why2 === "below_gate" ? `${d.arm.model}: unproven` : WHY[why2] ?? why2;
    return `${cls}: would apply nothing now (${text}); default ${def}`;
  }
  const pick = `${cls}: ${d.applied ? "picked" : "would pick"} ${d.arm.model}`;
  const vs = !known ? " (default not seen yet)" : cheaper ? ` (default ${def})` : `; default ${def}: no saving`;
  let why = "";
  if (d.mode === "active" && !d.applied) {
    const w = Object.entries(WHY).find(([k]) => d.reason.split(";").includes(k))?.[1];
    why = `, not applied${w ? `: ${w}` : ""}`;
  }
  return `${pick}${vs}${why}`;
}
async function statusCommand(argv, io) {
  if (argv.length > 0) {
    io.stderr.write("Usage: horsy status\n");
    return argv[0] === "--help" || argv[0] === "-h" ? 0 : 2;
  }
  const config = loadConfig();
  const events = readEvents();
  const classOf = /* @__PURE__ */ new Map();
  for (const e of events)
    if (e.type === "task_created")
      classOf.set(e.task_id, e.task_class);
  const decisions = events.filter((e) => e.type === "route_decision").slice(-LAST_DECISIONS);
  const env = process.env["HORSY_MODE"];
  const envNote = env !== void 0 && env === config.mode ? " (from HORSY_MODE)" : "";
  let out = "Horsy status\n";
  out += row("mode", config.mode + envNote);
  out += row("decider", config.decider);
  out += row("active classes", config.active_classes === "all" ? "all" : config.active_classes.join(", ") || "none");
  out += row("events", `${events.length}   ${tilde(join15(horsyHome(), "events.jsonl"))}`);
  if (config.mode === "off")
    out += "\nmode off \u2014 routing, verification and receipts are inactive\n";
  else {
    out += "\nLast decisions\n";
    if (decisions.length === 0)
      out += "  none yet\n";
    for (const d of decisions)
      out += `  ${decisionLine(classOf.get(d.task_id) ?? d.features.verb, d)}
`;
  }
  const repo = repoHashReadOnly(process.cwd());
  const stats = loadCellStats(config.ladder, { save: false })[repo] ?? {};
  const cells = Object.keys(stats).flatMap((cls) => posteriors({ [repo]: stats }, repo, cls, config.ladder).flatMap((c, i) => c.n > 0 ? [{ cls, rung: config.ladder[i]?.model ?? "?", n: c.n, mean: mean(c) }] : []));
  const shown = cells.filter((c) => c.n >= MIN_CELL_N).sort((a, b) => b.n - a.n || b.mean - a.mean);
  out += "\nTop cells (this repo)\n";
  if (shown.length === 0)
    out += `  none yet: cells appear after ${MIN_CELL_N} outcomes
`;
  for (const c of shown.slice(0, 5))
    out += `  ${c.cls.padEnd(14)} ${c.rung.padEnd(8)} n=${c.n}  mean ${c.mean.toFixed(2)}
`;
  out += config.mode === "off" ? "\nTurn on: /horsy:mode shadow (or HORSY_MODE=shadow)\n" : "\nTurn off: /horsy:mode off (or HORSY_MODE=off)\n";
  io.stdout.write(out);
  return 0;
}
function envOverride(io) {
  const env = process.env["HORSY_MODE"];
  if (env === void 0 || !["shadow", "active", "off"].includes(env))
    return false;
  io.stdout.write(`HORSY_MODE=${env} is set in this environment (for example by managed settings) and overrides the saved mode; nothing changed.
`);
  return true;
}
async function modeCommand(argv, io) {
  const [target, ...classes] = argv;
  if (target === "--help" || target === "-h") {
    io.stdout.write(MODE_USAGE);
    return 0;
  }
  if (target !== "shadow" && target !== "active" && target !== "off") {
    io.stderr.write(`horsy mode: ${target === void 0 ? "missing mode" : `unknown mode "${target}"`}

${MODE_USAGE}`);
    return 2;
  }
  if (target !== "active" && classes.length > 0) {
    io.stderr.write(`horsy mode: classes only apply to "active"

${MODE_USAGE}`);
    return 2;
  }
  const unknown = classes.filter((c) => !CLASSES.includes(c));
  if (unknown.length > 0) {
    io.stderr.write(`horsy mode: unknown class ${unknown.join(", ")}. Valid classes: ${CLASSES.join(", ")}
`);
    return 2;
  }
  if (envOverride(io))
    return 1;
  const before = loadSavedConfig();
  const from = before.mode;
  const back = from === "active" && before.active_classes !== "all" ? `active ${before.active_classes.join(" ")}` : from;
  const saved = saveConfig(target === "active" ? { mode: target, active_classes: classes.length > 0 ? classes : "all" } : { mode: target });
  appendEvent(makeEvent({ type: "override", task_id: null, kind: "mode_change", from, to: target }, { session_id: "cli", repo_hash: repoHash(process.cwd()) }));
  const scope = target === "active" ? ` (classes: ${saved.active_classes === "all" ? "all" : saved.active_classes.join(", ")})` : "";
  const undo = from === target ? "" : `Undo: /horsy:mode ${back} (or: horsy mode ${back})
`;
  io.stdout.write(`Horsy mode: ${from} \u2192 ${target}${scope}
${undo}`);
  if (target === "active") {
    const on = saved.verify.repos.includes(repoHash(process.cwd()));
    io.stdout.write(`Checks run only in repos where you ran "horsy checks on" (this repo: ${on ? "on" : "off"}). Without it Horsy routes only read-only tasks there and never blocks a "done".
`);
  }
  if (target === "active") {
    const targets = classes.length > 0 ? classes : VERBS.filter((v2) => v2 !== "other");
    const mine = loadCellStats(saved.ladder)[repoHash(process.cwd())] ?? {};
    const cheap = saved.ladder.slice(0, -1).map((a) => a.model);
    const missing = targets.filter((c) => !cheap.some((m) => (mine[c]?.[m]?.n ?? 0) > 0));
    if (missing.length > 0) {
      const names = classes.length === 0 && missing.length === targets.length ? "any class" : missing.join(", ");
      io.stdout.write(`Note: no cheap-model outcomes yet for ${names}: Horsy explores on ~${Math.round(saved.explore_rate * 100)}% of safe tasks; routing starts per class after 20 outcomes
`);
    }
  }
  const wide = classes.filter((c) => !c.endsWith("-wide") && !classes.includes(`${c}-wide`));
  if (wide.length > 0)
    io.stdout.write(`Note: ${wide.map((c) => `${c}-wide`).join(", ")} ${wide.length === 1 ? "is a separate class" : "are separate classes"}, not covered by ${wide.join(", ")}.
`);
  return 0;
}
async function receiptCommand(argv, io) {
  let session;
  let json = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--json")
      json = true;
    else if (a === "--session" && argv[i + 1] !== void 0)
      session = argv[++i];
    else if (a === "--help" || a === "-h") {
      io.stdout.write(RECEIPT_USAGE);
      return 0;
    } else {
      io.stderr.write(`horsy receipt: unknown option "${a}"

${RECEIPT_USAGE}`);
      return 2;
    }
  }
  const events = readEvents();
  if (session === void 0) {
    const seen = events.filter((e) => e.type !== "override");
    const repo = repoHashReadOnly(process.cwd());
    session = (seen.findLast((e) => e.repo_hash === repo) ?? seen.at(-1))?.session_id;
  }
  if (session === void 0) {
    io.stdout.write("No Horsy events yet: nothing to bill. Subagent tasks appear here after your next Claude Code session.\n");
    return 0;
  }
  const r = buildReceipt(events, session, { session_model: readSessionModel(session), mode: loadConfig().mode });
  if (json) {
    io.stdout.write(`${JSON.stringify(r, null, 2)}
`);
    return 0;
  }
  io.stdout.write(`${receiptLine(r, { detail: true })}
` + row("session", session) + row("cost", r.tasks > 0 && r.baseline_unknown_tasks >= r.tasks ? `${formatUsd(r.actual_cost)} actual, baseline unknown (default model not seen)` : `${formatUsd(r.actual_cost)} actual, ${formatUsd(r.default_cost)} on default, net ${netVerdict(r.net)}${r.baseline_unknown_tasks > 0 ? ` (baseline unknown for ${r.baseline_unknown_tasks} of ${r.tasks} tasks)` : ""}`) + row("time", `${secs(r.time_ms - r.check_ms)} in tasks, hook latency ${r.hook_ms} ms`) + row("check time", `${secs(r.check_ms)}${r.mode === "shadow" ? ", in the background" : ""}`) + row("checks", `${r.checks_passed} passed, ${r.checks_failed} failed, ${r.no_signal} no signal, ${r.blocked} blocked${r.unverified > 0 ? `, ${r.unverified} unverified` : ""}${r.failing_checks.length > 0 ? ` (failing: ${r.failing_checks.join(", ")})` : ""}`) + row("mode", r.mode));
  return 0;
}
async function checksCommand(argv, io) {
  const [sub, ...more] = argv;
  if (sub === "--help" || sub === "-h") {
    io.stdout.write(CHECKS_USAGE);
    return 0;
  }
  if (sub !== "on" && sub !== "off" && sub !== "status" || more.length > 0) {
    io.stderr.write(`horsy checks: ${sub === void 0 ? "missing argument" : `unknown argument "${argv.join(" ")}"`}

${CHECKS_USAGE}`);
    return 2;
  }
  const root2 = repoRoot(process.cwd());
  if (!root2) {
    io.stderr.write("horsy checks: not a git repository. Run it inside the repo you want to change.\n");
    return 1;
  }
  const hash = sub === "status" ? repoHashReadOnly(process.cwd()) : repoHash(process.cwd());
  const repos = loadSavedConfig().verify.repos;
  if (sub === "on" || sub === "off") {
    saveConfig({ verify: { repos: sub === "on" ? [.../* @__PURE__ */ new Set([...repos, hash])] : repos.filter((r) => r !== hash) } });
    const mode = loadConfig().mode;
    const when = mode === "active" ? 'active mode will run its test, lint and typecheck scripts in a temporary copy after each subagent or main-agent turn that edits files, and may block a failing "done"' : mode === "off" ? "its scripts will run when Horsy is on (shadow mode)" : "shadow mode will run its test, lint and typecheck scripts in a temporary copy after each subagent or main-agent turn that edits files";
    io.stdout.write(sub === "on" ? `checks: on in this repo; ${when}.
` : "checks: off in this repo; Horsy will not run its scripts there, and active mode routes only read-only tasks.\n");
    return 0;
  }
  const config = loadConfig();
  const on = config.mode !== "off" && repos.includes(hash);
  const names = detectChecksDetailed(root2).checks.map((c) => c.name);
  io.stdout.write(`checks: ${on ? "on" : "off"} in this repo (mode ${config.mode})
  would run: ${names.length > 0 ? names.join(", ") : "nothing detected"}
`);
  return 0;
}
var USAGE2, row, LAST_DECISIONS, MIN_CELL_N, WHY, rank, VERBS, CLASSES, MODE_USAGE, RECEIPT_USAGE, secs, CHECKS_USAGE;
var init_commands = __esm({
  "packages/audit/build/commands.js"() {
    "use strict";
    init_build();
    USAGE2 = "Usage: horsy reconcile [--now] [--repo <path>]\n";
    __name(reconcileCommand, "reconcileCommand");
    __name(tilde, "tilde");
    row = /* @__PURE__ */ __name((k, v2) => `  ${k.padEnd(15)} ${v2}
`, "row");
    LAST_DECISIONS = 8;
    MIN_CELL_N = 5;
    WHY = {
      default_unknown: "default unknown",
      below_gate: "not enough data yet",
      no_cheaper: "no cheaper model",
      class_inactive: "class not active",
      custom_agent: "custom agent",
      checks_off: "checks not on in this repo"
    };
    rank = /* @__PURE__ */ __name((m) => FAMILY_RANK[modelFamily(m) ?? ""] ?? Infinity, "rank");
    __name(decisionLine, "decisionLine");
    __name(statusCommand, "statusCommand");
    VERBS = ["search", "read", "explain", "review", "plan", "rename", "docs", "test", "fix", "refactor", "implement", "migrate", "other"];
    CLASSES = [...VERBS, ...VERBS.map((v2) => `${v2}-wide`)];
    MODE_USAGE = "Usage: horsy mode shadow | active [class...] | off\n";
    __name(envOverride, "envOverride");
    __name(modeCommand, "modeCommand");
    RECEIPT_USAGE = "Usage: horsy receipt [--session <id>] [--json]\n";
    secs = /* @__PURE__ */ __name((ms) => `${(ms / 1e3).toFixed(1)} s`, "secs");
    __name(receiptCommand, "receiptCommand");
    CHECKS_USAGE = "Usage: horsy checks on | off | status\n";
    __name(checksCommand, "checksCommand");
  }
});

// packages/audit/build/transcripts.js
import { createReadStream } from "node:fs";
import { lstat, open, readdir, realpath, stat } from "node:fs/promises";
import { basename as basename3, join as join16 } from "node:path";
function num2(v2, bad) {
  if (typeof v2 !== "number")
    return 0;
  if (!Number.isFinite(v2) || v2 < 0 || v2 > USAGE_MAX) {
    bad.v = true;
    return 0;
  }
  return Math.floor(v2) + 0;
}
function earlier(a, b) {
  if (a.line !== b.line)
    return a.line < b.line;
  if (a.file !== b.file)
    return a.file < b.file;
  return a.path < b.path;
}
function newState() {
  return { turns: /* @__PURE__ */ new Map(), calls: /* @__PURE__ */ new Map(), pendingResults: /* @__PURE__ */ new Map(), files: 0, files_failed: 0, lines: 0, skipped: 0, unknown_types: 0, suspicious_usage: 0, owner: /* @__PURE__ */ new Map() };
}
function emitLine(line, onLine) {
  const end = line.length > 0 && line[line.length - 1] === 13 ? line.length - 1 : line.length;
  if (end > 0)
    onLine(line.subarray(0, end));
}
function splitBuffer(buf, onLine, onOversize) {
  let pos2 = 0;
  while (pos2 < buf.length) {
    let nl = buf.indexOf(10, pos2);
    if (nl === -1)
      nl = buf.length;
    if (nl - pos2 > MAX_LINE_BYTES2)
      onOversize();
    else
      emitLine(buf.subarray(pos2, nl), onLine);
    pos2 = nl + 1;
  }
}
async function splitStream(path, onLine, onOversize) {
  let pending = [];
  let pendingLen = 0;
  let overflow = false;
  for await (const chunk of createReadStream(path, { highWaterMark: CHUNK_BYTES })) {
    let pos2 = 0;
    while (pos2 < chunk.length) {
      const nl = chunk.indexOf(10, pos2);
      if (nl === -1) {
        if (!overflow) {
          pending.push(chunk.subarray(pos2));
          pendingLen += chunk.length - pos2;
          if (pendingLen > MAX_LINE_BYTES2) {
            overflow = true;
            pending = [];
            pendingLen = 0;
          }
        }
        break;
      }
      if (overflow || pendingLen + (nl - pos2) > MAX_LINE_BYTES2)
        onOversize();
      else
        emitLine(pending.length === 0 ? chunk.subarray(pos2, nl) : Buffer.concat([...pending, chunk.subarray(pos2, nl)]), onLine);
      pending = [];
      pendingLen = 0;
      overflow = false;
      pos2 = nl + 1;
    }
  }
  if (overflow)
    onOversize();
  else if (pendingLen > 0)
    emitLine(Buffer.concat(pending), onLine);
}
function usageOf2(raw, st) {
  const u = isObj7(raw) ? raw : {};
  const cc = isObj7(u.cache_creation) ? u.cache_creation : {};
  const bad = { v: false };
  const usage = {
    input_tokens: num2(u.input_tokens, bad),
    output_tokens: num2(u.output_tokens, bad),
    cache_creation_input_tokens: num2(u.cache_creation_input_tokens, bad),
    cache_read_input_tokens: num2(u.cache_read_input_tokens, bad),
    cache_creation_1h_input_tokens: num2(cc.ephemeral_1h_input_tokens, bad)
  };
  if (bad.v)
    st.suspicious_usage++;
  return usage;
}
function applyResult(call2, r) {
  call2.agent_id = r.agent_id ?? call2.agent_id;
  call2.resolved_model = r.resolved_model ?? call2.resolved_model;
  call2.failed = call2.failed || r.failed;
}
function handleAssistant(o, ctx, st) {
  const msg = o.message;
  const id = isObj7(msg) ? str3(msg.id) : null;
  if (!isObj7(msg) || id === null) {
    st.skipped++;
    return;
  }
  const session_id = str3(o.sessionId) ?? ctx.fallbackSession;
  const ts = str3(o.timestamp) ?? "";
  const prev = st.turns.get(id);
  const rank2 = { line: timeOf(ts), file: ctx.firstTs, path: ctx.path };
  const sidechain = o.isSidechain === true || ctx.subagent;
  const agent_id = str3(o.agentId) ?? ctx.fallbackAgent;
  if (prev) {
    if (isObj7(msg.usage))
      prev.usage = usageOf2(msg.usage, st);
    if (earlier(rank2, st.owner.get(`m:${id}`))) {
      Object.assign(prev, { session_id, project: ctx.project, ts, sidechain, agent_id });
      st.owner.set(`m:${id}`, rank2);
    }
  } else {
    st.owner.set(`m:${id}`, rank2);
    st.turns.set(id, { session_id, project: ctx.project, ts, model: str3(msg.model) ?? "unknown", usage: usageOf2(msg.usage, st), sidechain, agent_id });
  }
  if (!Array.isArray(msg.content))
    return;
  for (const b of msg.content) {
    if (!isObj7(b) || b.type !== "tool_use" || b.name !== "Agent" && b.name !== "Task")
      continue;
    const tid = str3(b.id);
    if (tid === null)
      continue;
    const dup = st.calls.get(tid);
    if (dup) {
      if (earlier(rank2, st.owner.get(`c:${tid}`))) {
        Object.assign(dup, { session_id, project: ctx.project, ts });
        st.owner.set(`c:${tid}`, rank2);
      }
      continue;
    }
    st.owner.set(`c:${tid}`, rank2);
    const input = isObj7(b.input) ? b.input : {};
    const call2 = {
      session_id,
      project: ctx.project,
      ts,
      tool_use_id: tid,
      agent_id: null,
      description: str3(input.description) ?? "",
      prompt: (str3(input.prompt) ?? "").slice(0, PROMPT_MAX_CHARS),
      subagent_type: str3(input.subagent_type),
      explicit_model: str3(input.model),
      resolved_model: null,
      failed: false
    };
    const pend = st.pendingResults.get(tid);
    if (pend) {
      applyResult(call2, pend);
      st.pendingResults.delete(tid);
    }
    st.calls.set(tid, call2);
  }
}
function handleUser(o, st) {
  const msg = o.message;
  if (!isObj7(msg) || !Array.isArray(msg.content))
    return;
  const tur = isObj7(o.toolUseResult) ? o.toolUseResult : {};
  const status = str3(tur.status);
  for (const b of msg.content) {
    if (!isObj7(b) || b.type !== "tool_result")
      continue;
    const tid = str3(b.tool_use_id);
    if (tid === null)
      continue;
    const r = {
      agent_id: str3(tur.agentId),
      resolved_model: str3(tur.resolvedModel),
      failed: b.is_error === true || status === "failed" || status === "error"
    };
    const call2 = st.calls.get(tid);
    if (call2)
      applyResult(call2, r);
    else if (r.agent_id !== null)
      st.pendingResults.set(tid, r);
  }
}
async function load(path) {
  try {
    const fh = await open(path, "r");
    try {
      if ((await fh.stat()).size > IN_MEMORY_MAX_BYTES)
        return { stream: true };
      return { buf: await fh.readFile() };
    } finally {
      await fh.close();
    }
  } catch {
    return { error: true };
  }
}
async function fold(path, project, subagent, loaded, st) {
  const name = basename3(path, ".jsonl");
  const ctx = {
    project,
    path,
    firstTs: Infinity,
    subagent: subagent !== false,
    fallbackAgent: subagent !== false && name.startsWith("agent-") ? name.slice("agent-".length) : null,
    fallbackSession: subagent !== false ? subagent : name
    // subagent files live under <session>/subagents/
  };
  st.files++;
  if ("error" in loaded) {
    st.files_failed++;
    return;
  }
  const onLine = /* @__PURE__ */ __name((buf) => {
    st.lines++;
    try {
      const o = JSON.parse(buf.toString("utf8"));
      const type = isObj7(o) ? o.type : void 0;
      if (isObj7(o) && ctx.firstTs === Infinity && typeof o.timestamp === "string")
        ctx.firstTs = timeOf(o.timestamp);
      if (!isObj7(o) || typeof type !== "string" || !KNOWN_TYPES.has(type)) {
        st.unknown_types++;
        return;
      }
      if (type === "assistant")
        handleAssistant(o, ctx, st);
      else if (type === "user")
        handleUser(o, st);
    } catch {
      st.skipped++;
    }
  }, "onLine");
  const onOversize = /* @__PURE__ */ __name(() => {
    st.lines++;
    st.skipped++;
  }, "onOversize");
  if ("buf" in loaded) {
    splitBuffer(loaded.buf, onLine, onOversize);
    return;
  }
  try {
    await splitStream(path, onLine, onOversize);
  } catch {
    st.files_failed++;
  }
}
async function list(path) {
  const out = [];
  for (const e of (await readdir(path, { withFileTypes: true })).sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0)) {
    let dir2 = e.isDirectory();
    let file = e.isFile();
    if (e.isSymbolicLink()) {
      try {
        const s = await stat(join16(path, e.name));
        dir2 = s.isDirectory();
        file = s.isFile();
      } catch {
        continue;
      }
    }
    out.push({ name: e.name, dir: dir2, file, link: e.isSymbolicLink() });
  }
  return out;
}
async function parseProjects(root2, opts = {}) {
  const st = newState();
  const recent = /* @__PURE__ */ __name(async (p) => {
    if (opts.sinceMs === void 0)
      return true;
    try {
      return (await stat(p)).mtimeMs >= opts.sinceMs;
    } catch {
      return false;
    }
  }, "recent");
  const entries = [];
  const seen = /* @__PURE__ */ new Set();
  const enter = /* @__PURE__ */ __name(async (p) => {
    try {
      const real = await realpath(p);
      if (seen.has(real))
        return false;
      seen.add(real);
      return true;
    } catch {
      return false;
    }
  }, "enter");
  const walkSub = /* @__PURE__ */ __name(async (dir2, project, session, depth) => {
    if (!await enter(dir2))
      return;
    for (const e of await listOrEmpty(dir2)) {
      const p = join16(dir2, e.name);
      if (e.link)
        continue;
      if (e.file && e.name.endsWith(".jsonl")) {
        if (await recent(p))
          entries.push({ path: p, project, subagent: session });
      } else if (e.dir && depth < SUBAGENT_DEPTH)
        await walkSub(p, project, session, depth + 1);
    }
  }, "walkSub");
  let root_error = null;
  let top = [];
  try {
    top = await list(root2);
  } catch (err) {
    const code = err.code;
    root_error = code === "ENOENT" || code === "ENOTDIR" ? "missing" : "unreadable";
  }
  for (const pe of top) {
    const project = pe.name;
    if (!pe.dir || opts.project !== void 0 && project !== opts.project)
      continue;
    const pdir = join16(root2, project);
    if (!await enter(pdir))
      continue;
    const items = await listOrEmpty(pdir);
    for (const f of items) {
      if (f.file && f.name.endsWith(".jsonl") && await recent(join16(pdir, f.name)))
        entries.push({ path: join16(pdir, f.name), project, subagent: false });
    }
    for (const session of items) {
      if (!session.dir || session.link)
        continue;
      const sub = join16(pdir, session.name, "subagents");
      if (!await lstat(sub).then((l) => l.isSymbolicLink(), () => true))
        await walkSub(sub, project, session.name, 0);
    }
  }
  const window = Math.max(1, Math.floor(opts.concurrency ?? DEFAULT_CONCURRENCY));
  const inflight = /* @__PURE__ */ new Map();
  let next = 0;
  for (let i = 0; i < entries.length; i++) {
    for (; next < entries.length && next < i + window; next++)
      inflight.set(next, load(entries[next].path));
    const loaded = await inflight.get(i);
    inflight.delete(i);
    const e = entries[i];
    await fold(e.path, e.project, e.subagent, loaded, st);
  }
  const turns = [...st.turns.values()];
  const first = /* @__PURE__ */ new Map();
  for (const t of turns) {
    if (t.sidechain || t.model === "<synthetic>")
      continue;
    const parsed = Date.parse(t.ts);
    const time = Number.isNaN(parsed) ? Infinity : parsed;
    const cur = first.get(t.session_id);
    if (!cur || time < cur.t)
      first.set(t.session_id, { t: time, cc: t.usage.cache_creation_input_tokens });
  }
  return {
    turns,
    calls: [...st.calls.values()],
    files: st.files,
    files_failed: st.files_failed,
    lines: st.lines,
    skipped: st.skipped,
    unknown_types: st.unknown_types,
    suspicious_usage: st.suspicious_usage,
    root_error,
    session_first_cache_creation: new Map([...first].map(([k, v2]) => [k, v2.cc]))
  };
}
var MAX_LINE_BYTES2, CHUNK_BYTES, IN_MEMORY_MAX_BYTES, DEFAULT_CONCURRENCY, SUBAGENT_DEPTH, USAGE_MAX, PROMPT_MAX_CHARS, KNOWN_TYPES, isObj7, str3, timeOf, listOrEmpty;
var init_transcripts = __esm({
  "packages/audit/build/transcripts.js"() {
    "use strict";
    MAX_LINE_BYTES2 = 10 * 1024 * 1024;
    CHUNK_BYTES = 64 * 1024;
    IN_MEMORY_MAX_BYTES = 16 * 1024 * 1024;
    DEFAULT_CONCURRENCY = 16;
    SUBAGENT_DEPTH = 3;
    USAGE_MAX = 1e12;
    PROMPT_MAX_CHARS = 2e3;
    KNOWN_TYPES = /* @__PURE__ */ new Set([
      "user",
      "assistant",
      "attachment",
      "queue-operation",
      "last-prompt",
      "custom-title",
      "agent-name",
      "file-history-snapshot",
      "atis-latch",
      "bridge-session",
      "summary",
      "system",
      "cost-state",
      "mode",
      "ai-title",
      "file-history-delta",
      "permission-mode",
      "pr-link"
    ]);
    isObj7 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
    str3 = /* @__PURE__ */ __name((v2) => typeof v2 === "string" ? v2 : null, "str");
    __name(num2, "num");
    __name(earlier, "earlier");
    __name(newState, "newState");
    __name(emitLine, "emitLine");
    __name(splitBuffer, "splitBuffer");
    __name(splitStream, "splitStream");
    __name(usageOf2, "usageOf");
    timeOf = /* @__PURE__ */ __name((ts) => {
      const t = Date.parse(ts);
      return Number.isNaN(t) ? Infinity : t;
    }, "timeOf");
    __name(applyResult, "applyResult");
    __name(handleAssistant, "handleAssistant");
    __name(handleUser, "handleUser");
    __name(load, "load");
    __name(fold, "fold");
    __name(list, "list");
    listOrEmpty = /* @__PURE__ */ __name((path) => list(path).catch(() => []), "listOrEmpty");
    __name(parseProjects, "parseProjects");
  }
});

// packages/audit/build/cli.js
var cli_exports = {};
__export(cli_exports, {
  demoDir: () => demoDir,
  run: () => run
});
import { createHash as createHash5 } from "node:crypto";
import { existsSync as existsSync5, realpathSync as realpathSync6 } from "node:fs";
import { homedir as homedir4 } from "node:os";
import { join as join17 } from "node:path";
import { fileURLToPath } from "node:url";
function parseAuditArgs(args) {
  const envPlan = process.env["HORSY_PLAN"];
  const out = { days: 30, json: false, md: false, demo: false, help: false, plan: envPlan === "max" || envPlan === "pro" ? envPlan : "api", show_names: false };
  for (let i = 0; i < args.length; i++) {
    const raw = args[i];
    const eq = raw.startsWith("--") ? raw.indexOf("=") : -1;
    const flag = eq === -1 ? raw : raw.slice(0, eq);
    const inline = eq === -1 ? void 0 : raw.slice(eq + 1);
    const value = /* @__PURE__ */ __name(() => {
      const v2 = inline ?? args[++i];
      return v2 !== void 0 && v2.startsWith("--") ? void 0 : v2;
    }, "value");
    switch (flag) {
      case "--json":
        out.json = true;
        break;
      case "--md":
        out.md = true;
        break;
      case "--demo":
        out.demo = true;
        break;
      case "--show-names":
        out.show_names = true;
        break;
      case "--help":
      case "-h":
        out.help = true;
        break;
      case "--days": {
        const v2 = value();
        const n = Number(v2);
        if (v2 === void 0 || !/^\d+$/.test(v2) || n < 1 || n > MAX_DAYS)
          return `--days needs a whole number from 1 to ${MAX_DAYS}, got "${v2 ?? ""}"`;
        out.days = n;
        break;
      }
      case "--plan": {
        const v2 = value();
        if (v2 !== "max" && v2 !== "pro" && v2 !== "api")
          return `--plan needs max, pro or api, got "${v2 ?? ""}"`;
        out.plan = v2;
        break;
      }
      case "--project":
      case "--dir":
      case "--now": {
        const v2 = value();
        if (v2 === void 0 || v2 === "")
          return `${flag} needs a value`;
        if (flag === "--project")
          out.project = v2;
        else if (flag === "--dir")
          out.dir = v2;
        else if (!/^\d{4}-\d{2}-\d{2}(T|$)/.test(v2) || Number.isNaN(Date.parse(v2)))
          return `--now needs an ISO date (YYYY-MM-DD or with a T time), got "${v2}"`;
        else
          out.now = v2;
        break;
      }
      default:
        return `Unknown option "${raw}"`;
    }
  }
  if (out.json && out.md)
    return "--json and --md cannot be combined";
  return out;
}
function demoDir(moduleUrl = import.meta.url) {
  const bundled = fileURLToPath(new URL("../demo/projects", moduleUrl));
  return existsSync5(bundled) ? bundled : fileURLToPath(new URL("../fixtures/projects", moduleUrl));
}
function sessionFingerprints(ids) {
  return [...new Set(ids.map((id) => createHash5("sha256").update(id).digest("hex").slice(0, 8)))].sort().slice(0, FINGERPRINTS);
}
async function audit(args, io) {
  const envPlan = process.env["HORSY_PLAN"];
  if (envPlan !== void 0 && envPlan !== "" && envPlan !== "max" && envPlan !== "pro" && envPlan !== "api")
    io.stderr.write(`horsy audit: ignoring HORSY_PLAN="${clean(envPlan)}" (needs max, pro or api)
`);
  const a = parseAuditArgs(args);
  if (typeof a === "string") {
    io.stderr.write(`horsy audit: ${a}

${AUDIT_USAGE}`);
    return 2;
  }
  if (a.help) {
    io.stdout.write(AUDIT_USAGE);
    return 0;
  }
  const now = new Date(a.now ?? (a.demo ? DEMO_NOW : Date.now()));
  const dir2 = a.demo ? demoDir() : a.dir ?? join17(homedir4(), ".claude", "projects");
  const hint = a.demo ? "the demo data" : a.dir === void 0 ? DEFAULT_DIR_HINT : "the --dir you gave";
  const windowStart = now.getTime() - a.days * DAY_MS4;
  const since = a.demo ? {} : { sinceMs: windowStart - a.days * DAY_MS4 };
  const all = await parseProjects(dir2, since);
  const rootError = a.demo ? null : all.root_error;
  if (rootError && (a.dir !== void 0 || rootError === "unreadable")) {
    io.stderr.write(`horsy audit: ${rootError === "missing" ? "directory not found" : "cannot read directory"}: ${a.dir ?? DEFAULT_DIR_HINT}
`);
    return 2;
  }
  let parsed = all;
  let encoded = a.project;
  const isLabel = encoded !== void 0 && /^project-\d+$/.test(encoded);
  if (encoded !== void 0) {
    const ranking = analyze(all, { days: a.days, now }).by_project;
    if (isLabel)
      encoded = ranking[Number(encoded.slice("project-".length)) - 1]?.key;
    else if (!all.turns.some((t) => t.project === encoded))
      encoded = void 0;
    if (encoded === void 0) {
      const n = ranking.length;
      const avail = n === 0 ? "none" : n === 1 ? "project-1" : `project-1 \u2026 project-${n}`;
      io.stderr.write(`horsy audit: no project matches ${a.project}; available: ${avail}
`);
      return 2;
    }
    parsed = await parseProjects(dir2, { ...since, project: encoded });
  }
  const raw = analyze(parsed, { days: a.days, now });
  const how = a.show_names ? { decode: true } : encoded === void 0 ? {} : isLabel ? { label: a.project } : { decode: true };
  const report = anonymize(raw, how);
  report.share_line = planShareLine(report, a.plan);
  if (a.demo)
    report.share_line = `[demo] ${report.share_line}`;
  const inWindow = parsed.turns.filter((t) => t.model !== "<synthetic>" && Date.parse(t.ts) >= windowStart && Date.parse(t.ts) <= now.getTime());
  const window = {
    from: new Date(windowStart).toISOString().slice(0, 10),
    to: now.toISOString().slice(0, 10),
    sessions: new Set(inWindow.map((t) => t.session_id)).size,
    projects: report.by_project.length
  };
  if (a.json) {
    io.stdout.write(renderJson(report, { demo: a.demo, files_failed: parsed.files_failed, window, session_fingerprints: sessionFingerprints(inWindow.map((t) => t.session_id)) }));
    return 0;
  }
  const noData = parsed.files === 0 || report.by_model.length === 0 && report.tasks.length === 0;
  const unreadable = parsed.skipped + parsed.unknown_types;
  if (noData && !a.demo && parsed.files > 0 && unreadable + parsed.files_failed > 0) {
    io.stdout.write(`${formatNote(unreadable, parsed.files_failed)}
`);
    return 0;
  }
  if (noData) {
    const last = a.days === 1 ? "1 day" : `${a.days} days`;
    io.stdout.write(a.demo ? `No demo sessions fall in the last ${last}. Try a larger --days.
` : `No Claude Code sessions found in the last ${last} under ${hint}. Try --demo to see a sample report.
`);
    return 0;
  }
  const opts = {
    demo: a.demo,
    color: io.stdout.isTTY === true && !process.env["NO_COLOR"],
    parse: { lines: parsed.lines, skipped: parsed.skipped, unknown_types: parsed.unknown_types, files_failed: parsed.files_failed, suspicious_usage: parsed.suspicious_usage ?? 0 },
    plan: a.plan,
    window
  };
  io.stdout.write(a.md ? renderMarkdown(report, opts) : renderTerminal(report, opts));
  return 0;
}
async function run(argv, io = { stdout: process.stdout, stderr: process.stderr }) {
  const [cmd, ...rest] = argv;
  if (cmd === "--help" || cmd === "-h" || cmd === "help") {
    io.stdout.write(USAGE3);
    return 0;
  }
  const handler = cmd !== void 0 && Object.hasOwn(commands, cmd) ? commands[cmd] : void 0;
  if (!handler) {
    io.stderr.write(`${cmd === void 0 ? "" : `Unknown command "${cmd}"

`}${USAGE3}`);
    return 2;
  }
  try {
    return await handler(rest, io);
  } catch (e) {
    io.stderr.write(`horsy ${cmd}: ${e instanceof Error ? e.message : String(e)}
`);
    return 1;
  }
}
function isMain() {
  if (globalThis.horsyEmbedded)
    return false;
  const entry2 = process.argv[1];
  if (!entry2)
    return false;
  try {
    return realpathSync6(entry2) === realpathSync6(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}
var DAY_MS4, DEMO_NOW, MAX_DAYS, DEFAULT_DIR_HINT, USAGE3, AUDIT_USAGE, FINGERPRINTS, commands;
var init_cli = __esm({
  "packages/audit/build/cli.js"() {
    "use strict";
    init_analyze();
    init_merge();
    init_commands();
    init_render();
    init_transcripts();
    DAY_MS4 = 864e5;
    DEMO_NOW = "2026-10-16T00:00:00Z";
    MAX_DAYS = 365;
    DEFAULT_DIR_HINT = "~/.claude/projects";
    USAGE3 = `Usage: horsy <command> [options]

Commands:
  audit    Where your Claude Code spend went, and 3 things to change (reads local transcripts only)
  merge    Combine several "horsy audit --json" files into a team view (horsy merge <report.json> [...] [--json] [--allow-mixed])
  reconcile  Turn git history into delayed outcomes: kept / reverted / reworked (horsy reconcile [--now] [--repo <path>])
  status   Mode, decider, last routing decisions and this repo's best cells
  mode     Switch mode: horsy mode shadow | active [class...] | off
  receipt  A session's bill: tasks, models, cost vs default, checks, time (horsy receipt [--session <id>] [--json])
  checks   Run this repo's own checks after agent edits in shadow mode: horsy checks on | off | status

Run "horsy <command> --help" for options.
`;
    AUDIT_USAGE = `Usage: horsy audit [options]

  --days <n>        Window in days, 1-365 (default 30)
  --project <name>  One project: its label (project-N) or its directory name under --dir
  --json            Machine-readable report
  --md              Markdown report
  --plan <p>        max | pro | api (default: $HORSY_PLAN, else api). max/pro lead with fresh tokens (input +
                    output + cache writes), since a subscription limits tokens, not dollars; cache reads
                    are shown apart and dollars are labelled "at API prices"
  --show-names      Print short project names instead of project-N (local use; off by default)
  --dir <path>      Projects root (default ${DEFAULT_DIR_HINT})
  --demo            Use bundled synthetic sessions
  --now <iso>       ISO date or datetime. Treat this instant as "now" (default: current time; demo: fixture date)
  --help            Show this help
`;
    __name(parseAuditArgs, "parseAuditArgs");
    __name(demoDir, "demoDir");
    FINGERPRINTS = 2e3;
    __name(sessionFingerprints, "sessionFingerprints");
    __name(audit, "audit");
    commands = { audit, merge: mergeCommand, reconcile: reconcileCommand, status: statusCommand, mode: modeCommand, receipt: receiptCommand, checks: checksCommand };
    __name(run, "run");
    __name(isMain, "isMain");
    if (isMain()) {
      process.stdout.on("error", () => void 0);
      run(process.argv.slice(2)).then((code) => {
        process.exitCode = code;
      });
    }
  }
});

// packages/plugin/src/main.ts
init_build();

// packages/plugin/src/hooks.ts
init_build();
import { spawn as spawn2 } from "node:child_process";
import { randomBytes } from "node:crypto";
var isObj5 = /* @__PURE__ */ __name((v2) => typeof v2 === "object" && v2 !== null && !Array.isArray(v2), "isObj");
var nonEmpty = /* @__PURE__ */ __name((v2) => typeof v2 === "string" && v2 !== "", "nonEmpty");
var INLINE_CELLS_BYTES = 1e6;
async function preToolUse(input, env) {
  const { session_id, cwd, tool_input, tool_name, transcript_path, tool_use_id } = input;
  if (!nonEmpty(session_id) || !nonEmpty(cwd) || !isObj5(tool_input)) return null;
  if (tool_name !== void 0 && tool_name !== "Agent") return null;
  const t0 = Date.now();
  const config = loadConfig();
  const { events, output } = await route(
    {
      session_id,
      cwd,
      tool_input,
      ...typeof transcript_path === "string" ? { transcript_path } : {},
      ...typeof tool_use_id === "string" ? { tool_use_id } : {}
    },
    { config, cellStats: /* @__PURE__ */ __name(() => loadCellStats(config.ladder, { inline_max_bytes: INLINE_CELLS_BYTES }), "cellStats"), rng: mulberry32(randomBytes(4).readUInt32LE()), env, now: Date.now }
  );
  const hook_ms = Date.now() - t0;
  for (const e of events) {
    try {
      if (e.type === "route_decision") e.hook_ms = hook_ms;
      appendEvent(e);
    } catch {
    }
  }
  return output;
}
__name(preToolUse, "preToolUse");
function postToolUse(input) {
  const { session_id, tool_name, tool_response, tool_use_id } = input;
  if (loadConfig().mode !== "active" || !nonEmpty(session_id) || !nonEmpty(tool_use_id) || tool_name !== void 0 && tool_name !== "Agent" || !isObj5(tool_response)) return null;
  const agentId = tool_response["agentId"];
  if (!nonEmpty(agentId)) return null;
  const additionalContext = unverifiedWarning(session_id, agentId, tool_use_id);
  return additionalContext ? { hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext } } : null;
}
__name(postToolUse, "postToolUse");
function sessionStart(input) {
  if (nonEmpty(input["session_id"]) && typeof input["model"] === "string") recordSessionModel(input["session_id"], input["model"]);
  return null;
}
__name(sessionStart, "sessionStart");
function spawnBackground(args, script = process.argv[1]) {
  if (!script) return;
  const child = spawn2(process.execPath, [script, "hook", "verify-bg"], { detached: true, stdio: ["pipe", "ignore", "ignore"] });
  child.on("error", () => void 0);
  child.stdin.on("error", () => void 0);
  child.stdin.end(JSON.stringify(args));
  child.unref();
}
__name(spawnBackground, "spawnBackground");
var verifyDeps = /* @__PURE__ */ __name(() => ({ config: loadConfig(), now: Date.now, spawnBackground }), "verifyDeps");
async function afterStop(result) {
  const r = await result;
  loadCellStats(loadConfig().ladder);
  return r;
}
__name(afterStop, "afterStop");
function subagentStop(input) {
  const { session_id, cwd, agent_id, agent_type, agent_transcript_path, stop_hook_active, last_assistant_message } = input;
  if (nonEmpty(session_id)) noteSessionModel(typeof input["transcript_path"] === "string" ? input["transcript_path"] : void 0, session_id);
  if (!nonEmpty(session_id) || !nonEmpty(cwd) || !nonEmpty(agent_id) || !nonEmpty(agent_transcript_path)) return Promise.resolve(null);
  return afterStop(onSubagentStop(
    {
      session_id,
      cwd,
      agent_id,
      agent_type: typeof agent_type === "string" ? agent_type : "",
      agent_transcript_path,
      stop_hook_active: stop_hook_active === true,
      ...typeof last_assistant_message === "string" ? { last_assistant_message } : {}
    },
    verifyDeps()
  ));
}
__name(subagentStop, "subagentStop");
function stop(input) {
  const { session_id, cwd, transcript_path, stop_hook_active } = input;
  if (nonEmpty(session_id)) noteSessionModel(typeof transcript_path === "string" ? transcript_path : void 0, session_id);
  if (!nonEmpty(session_id) || !nonEmpty(cwd)) return Promise.resolve(null);
  return afterStop(onStop(
    { session_id, cwd, transcript_path: typeof transcript_path === "string" ? transcript_path : "", stop_hook_active: stop_hook_active === true },
    verifyDeps()
  ));
}
__name(stop, "stop");
async function runHook(event, stdin, env) {
  if (env["HORSY_TEST_HANG"] === "1") await new Promise(() => void 0);
  const input = JSON.parse(stdin);
  if (!isObj5(input) || loadConfig().mode === "off") return null;
  if (event === "pre-tool-use") return preToolUse(input, env);
  if (event === "post-tool-use") return postToolUse(input);
  if (event === "session-start") return sessionStart(input);
  if (event === "subagent-stop") return subagentStop(input);
  if (event === "stop") return stop(input);
  return null;
}
__name(runHook, "runHook");

// packages/plugin/src/main.ts
var MAX_STDIN = 5 * 1024 * 1024;
var DEADLINE_MS = { "pre-tool-use": 2e3, "session-start": 1e3 };
var VERIFY_EVENTS = /* @__PURE__ */ new Set(["subagent-stop", "stop", "verify-bg"]);
function deadlineMs(event) {
  if (!VERIFY_EVENTS.has(event)) return DEADLINE_MS[event] ?? 1e3;
  try {
    return (loadConfig().verify.timeout_s + 30) * 1e3;
  } catch {
    return 33e4;
  }
}
__name(deadlineMs, "deadlineMs");
async function readStdin() {
  if (process.stdin.isTTY) return "";
  const chunks = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > MAX_STDIN) return null;
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}
__name(readStdin, "readStdin");
async function hookMain(event) {
  process.on("uncaughtException", () => process.exit(0));
  process.on("unhandledRejection", () => process.exit(0));
  let timer;
  try {
    const work = (async () => {
      const stdin = await readStdin();
      return stdin === null ? null : runHook(event, stdin, process.env);
    })();
    const deadline = new Promise((resolve3) => {
      timer = setTimeout(resolve3, deadlineMs(event), null);
    });
    const output = await Promise.race([work, deadline]);
    if (output) await new Promise((resolve3) => process.stdout.write(JSON.stringify(output), () => resolve3()));
  } catch {
  }
  clearTimeout(timer);
  process.exitCode = 0;
  return process.exit(0);
}
__name(hookMain, "hookMain");
async function verifyBgMain() {
  process.on("uncaughtException", () => process.exit(0));
  process.on("unhandledRejection", () => process.exit(0));
  setTimeout(() => process.exit(0), deadlineMs("verify-bg"));
  try {
    await verifyInBackground(JSON.parse(await readStdin() ?? ""));
  } catch {
  }
  return process.exit(0);
}
__name(verifyBgMain, "verifyBgMain");
async function main(argv) {
  if (argv[0] === "hook") return argv[1] === "verify-bg" ? verifyBgMain() : hookMain(argv[1] ?? "");
  globalThis.horsyEmbedded = true;
  const { run: run2 } = await Promise.resolve().then(() => (init_cli(), cli_exports));
  return run2(argv);
}
__name(main, "main");
process.stdout.on("error", () => void 0);
process.exitCode = await main(process.argv.slice(2));
