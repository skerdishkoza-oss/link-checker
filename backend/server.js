// backend/server.js - SLACK INTEGRATED WITH PROXY ROTATION
// Install dependencies: npm install express cors puppeteer axios dotenv @slack/web-api node-cron hpagent

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const axios = require('axios');
const { URL } = require('url');
const { WebClient } = require('@slack/web-api');
const cron = require('node-cron');
const { HttpProxyAgent, HttpsProxyAgent } = require('hpagent');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

const token = process.env.SLACK_BOT_TOKEN ? process.env.SLACK_BOT_TOKEN.trim() : null;
const slackClient = new WebClient(token);
const SLACK_CHANNEL_ID = process.env.SLACK_CHANNEL_ID;

if (token) {
  console.log(`🔑 Slack Client Initialized with token starting: ${token.substring(0, 10)}...`);
} else {
  console.warn('⚠️ SLACK_BOT_TOKEN not found in environment variables');
}

// --- PROXY CONFIGURATION ---

const PROXY_ENABLED = process.env.PROXY_ENABLED === 'true';

const PROXY_CONFIG = {
  'US': 'http://cH23v0hy3IOdrFOa:wifi;us;;;@rotating.proxyempire.io:9111',
  'GB': 'http://cH23v0hy3IOdrFOa:wifi;gb;;;@rotating.proxyempire.io:9112',
  'IE': 'http://cH23v0hy3IOdrFOa:wifi;ie;;;@rotating.proxyempire.io:9113',
  'DK': 'http://cH23v0hy3IOdrFOa:wifi;dk;;;@rotating.proxyempire.io:9114',
  'PT': 'http://cH23v0hy3IOdrFOa:wifi;pt;;;@rotating.proxyempire.io:9115',
  'ES': 'http://cH23v0hy3IOdrFOa:wifi;es;;;@rotating.proxyempire.io:9116',
  'MX': 'http://cH23v0hy3IOdrFOa:wifi;mx;;;@rotating.proxyempire.io:9117',
  'CA': 'http://cH23v0hy3IOdrFOa:wifi;ca;;;@rotating.proxyempire.io:9118',
  'AU': 'http://cH23v0hy3IOdrFOa:wifi;au;;;@rotating.proxyempire.io:9119',
  'IT': 'http://cH23v0hy3IOdrFOa:wifi;it;;;@rotating.proxyempire.io:9120',
  'DE': 'http://cH23v0hy3IOdrFOa:wifi;de;;;berlin@rotating.proxyempire.io:9000',
  'PL': 'http://cH23v0hy3IOdrFOa:wifi;pl;;;warsaw@rotating.proxyempire.io:9000',
  'BR': 'http://cH23v0hy3IOdrFOa:wifi;br;;;rio+de+janeiro@rotating.proxyempire.io:9000',
  'JP': 'http://cH23v0hy3IOdrFOa:wifi;jp;;;tokyo@rotating.proxyempire.io:9000',
  'NL': 'http://cH23v0hy3IOdrFOa:wifi;nl;;;amsterdam@rotating.proxyempire.io:9000',
  'DEFAULT': 'http://cH23v0hy3IOdrFOa:wifi;us;;;@rotating.proxyempire.io:9111'
};

const GEO_PROXY_MAPPING = {
  'us': 'US', 'usa': 'US', 'gb': 'GB', 'uk': 'GB', 'ie': 'IE', 'ireland': 'IE',
  'dk': 'DK', 'denmark': 'DK', 'pt': 'PT', 'portugal': 'PT', 'es': 'ES', 'spain': 'ES',
  'mx': 'MX', 'mexico': 'MX', 'ca': 'CA', 'canada': 'CA', 'au': 'AU', 'australia': 'AU',
  'it': 'IT', 'italy': 'IT', 'de': 'DE', 'germany': 'DE', 'pl': 'PL', 'poland': 'PL',
  'br': 'BR', 'brasil': 'BR', 'jp': 'JP', 'japan': 'JP', 'nl': 'NL', 'netherland': 'NL'
};

console.log(`🌍 Proxy Multi-Geo Scanning: ${PROXY_ENABLED ? 'ENABLED ✅' : 'DISABLED ❌'}`);
if (PROXY_ENABLED) {
  console.log(`📍 Available Proxy Locations: ${Object.keys(PROXY_CONFIG).filter(k => k !== 'DEFAULT').join(', ')}`);
}

const analysisCache = new Map();
let currentProxyLocation = null;

// --- PROXY VERIFICATION ---

async function verifyProxy(proxyLocation = 'US') {
  if (!PROXY_ENABLED) {
    console.log('ℹ️  Proxy disabled, skipping verification');
    return true;
  }
  try {
    console.log(`\n🔍 Verifying proxy connection for ${proxyLocation}...`);
    const proxyConfig = getProxyForLocation(proxyLocation);
    if (!proxyConfig) { console.error('❌ PROXY CONFIG NOT FOUND'); return false; }
    const axiosConfig = {
      timeout: 10000,
      httpAgent: new HttpProxyAgent({ proxy: proxyConfig.url }),
      httpsAgent: new HttpsProxyAgent({ proxy: proxyConfig.url })
    };
    const res = await axios.get('https://api.ipify.org?format=json', axiosConfig);
    console.log(`✅ PROXY VERIFIED for ${proxyLocation}. Current IP: ${res.data.ip}`);
    return true;
  } catch (e) {
    console.error(`❌ PROXY FAILED for ${proxyLocation}: ${e.message}`);
    return false;
  }
}

// --- PROXY HELPER FUNCTIONS ---

function getProxyForLocation(location) {
  if (!PROXY_ENABLED) return null;
  const normalizedLocation = location.toUpperCase().trim();
  if (PROXY_CONFIG[normalizedLocation]) return { url: PROXY_CONFIG[normalizedLocation], location: normalizedLocation };
  const mapped = GEO_PROXY_MAPPING[location.toLowerCase()];
  if (mapped && PROXY_CONFIG[mapped]) return { url: PROXY_CONFIG[mapped], location: mapped };
  return { url: PROXY_CONFIG.DEFAULT, location: 'US' };
}

function parseProxyUrl(proxyUrl) {
  try {
    const match = proxyUrl.match(/^(https?):\/\/([^:]+):([^@]+)@([^:]+):(\d+)$/);
    if (!match) { console.error('❌ Invalid proxy URL format:', proxyUrl); return null; }
    return {
      protocol: match[1], username: match[2], password: match[3],
      host: match[4], port: match[5], serverUrl: `${match[1]}://${match[4]}:${match[5]}`
    };
  } catch (error) {
    console.error('❌ Error parsing proxy URL:', error.message);
    return null;
  }
}

// --- UTILITY FUNCTIONS ---

function extractGeoFromUrl(url) {
  const match = url.match(/\/([a-z]{2})-[^/]+/i);
  return match ? match[1].toLowerCase() : null;
}

function getProxyLocationForUrl(url, defaultLocation = 'US') {
  const geoCode = extractGeoFromUrl(url);
  if (geoCode) { const mapped = GEO_PROXY_MAPPING[geoCode]; if (mapped) return mapped; }
  return defaultLocation;
}

function isValidUrl(url) {
  try { new URL(url); return true; } catch { return false; }
}

function isAffiliateLink(url) {
  const affiliatePatterns = ['/api/click', '/track', '/aff', '/redirect', '/redir', '/goto', '/out', 'clickid', 'affid', 'tid='];
  return affiliatePatterns.some(pattern => url.toLowerCase().includes(pattern));
}

// URLs that are known-good but trigger false positives (e.g. geo-blocks, bot detection)
const URL_WHITELIST = [
  'https://www.gamcare.org.uk/',
  'https://s3-symbol-logo.tradingview.com/'
];

function isWhitelistedUrl(url) {
  return URL_WHITELIST.includes(url);
}

function isTrackingPixel(url) {
  const trackingPatterns = [
    'bat.bing.com', 'google-analytics.com', 'googletagmanager.com',
    'facebook.com/tr', 'doubleclick.net', 'analytics.', '/pixel',
    '/beacon', 'track.php', 'collect?'
  ];
  return trackingPatterns.some(pattern => url.toLowerCase().includes(pattern));
}

function isSuccessStatus(status) {
  if (typeof status === 'number' && status >= 200 && status < 300) return true;
  if (status === 304) return true;
  return false;
}

// --- RETRY & CONCURRENCY HELPERS ---

async function withRetry(fn, retries = 3, delayMs = 2000) {
  const TRANSIENT_ERRORS = [
    'ERR_TUNNEL_CONNECTION_FAILED', 'ERR_NO_SUPPORTED_PROXIES', 'ERR_PROXY_CONNECTION_FAILED',
    'ERR_EMPTY_RESPONSE', 'Session with given id not found', 'Target closed',
    'ECONNRESET', 'ETIMEDOUT', 'socket hang up'
  ];
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const result = await fn();
      const isTransientResult = result && result.statusText &&
        TRANSIENT_ERRORS.some(e => result.statusText.includes(e));
      if (isTransientResult && attempt < retries) {
        console.log(`   🔄 Transient proxy/browser error — retry ${attempt}/${retries - 1}...`);
        await new Promise(r => setTimeout(r, delayMs));
        continue;
      }
      return result;
    } catch (err) {
      const isTransient = TRANSIENT_ERRORS.some(e => err.message.includes(e));
      if (isTransient && attempt < retries) {
        console.log(`   🔄 Caught transient error — retry ${attempt}/${retries - 1}: ${err.message}`);
        await new Promise(r => setTimeout(r, delayMs));
      } else { throw err; }
    }
  }
}

function createConcurrencyLimiter(maxConcurrent) {
  let active = 0;
  const queue = [];
  return function limit(fn) {
    return new Promise((resolve, reject) => {
      const run = async () => {
        active++;
        try { resolve(await fn()); } catch (e) { reject(e); } finally {
          active--;
          if (queue.length) queue.shift()();
        }
      };
      active < maxConcurrent ? run() : queue.push(run);
    });
  };
}

// --- ERROR VERIFICATION ---
// After an initial failure, confirms the link is genuinely broken by retrying
// 2 more times (10s apart), giving 3 total attempts before marking as a real issue.
// If any retry succeeds the original failure is treated as a temporary blip.

async function verifyLinkError(initialResult, url, useGeoRotation, proxyLocations) {
  const ADDITIONAL_ATTEMPTS = 2;   // already have 1 failure → 2 more = 3 total
  const VERIFY_DELAY_MS = 10_000;  // 10 seconds between attempts

  for (let attempt = 1; attempt <= ADDITIONAL_ATTEMPTS; attempt++) {
    console.log(`   🔁 Verifying failure — attempt ${attempt + 1}/3 (waiting 10s)...`);
    await new Promise(r => setTimeout(r, VERIFY_DELAY_MS));

    const result = await checkLinkStatus(url, useGeoRotation, proxyLocations);

    if (isSuccessStatus(result.status) || result.treatAsWorking || result.proxyIssue || result.status === 304) {
      console.log(`   ✅ Recovered on attempt ${attempt + 1}/3 — treating as a fluke, not an error`);
      return result;
    }

    console.log(`   ❌ Attempt ${attempt + 1}/3 also failed: ${result.status}`);
  }

  console.log(`   🔴 Confirmed broken after 3 attempts`);
  return initialResult;
}

// --- ERROR TRACKING ---
// After each scheduled scan, errored links are rechecked at +30, +60, +90 minutes.
// Only still-broken links are carried forward to the next round.
// Slack receives a formatted follow-up report after each recheck.

// key: scanId  →  value: { siteName, siteConfig, issues[], followUpsDone }
const errorTracker = new Map();

function scheduleErrorFollowUps(scanId, siteName, issues, siteConfig) {
  if (!issues || issues.length === 0) return;

  errorTracker.set(scanId, {
    siteName,
    siteConfig,
    issues: issues.map(i => ({ ...i })),
    followUpsDone: 0
  });

  console.log(`\n⏲️  Scheduling follow-up rechecks for ${issues.length} error(s) in ${siteName}`);
  console.log(`   → Round 1 in 30 min | Round 2 in 60 min | Round 3 in 90 min`);

  [30, 60, 90].forEach((delayMinutes, idx) => {
    setTimeout(() => runFollowUpCheck(scanId, idx + 1), delayMinutes * 60 * 1000);
  });
}

// Re-crawls a single page and returns the set of all href/src URLs found on it.
// Used by follow-up checks to detect links that have been fixed on the page itself.
async function getLiveLinksOnPage(pageUrl, proxyLocation = null) {
  let page = null;
  try {
    const { browser, credentials } = await getBrowserForProxy(proxyLocation);
    page = await browser.newPage();
    if (credentials) await page.authenticate({ username: credentials.username, password: credentials.password });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
    await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
    const urls = await page.evaluate(() =>
      Array.from(document.querySelectorAll('a[href], img[src], link[href], script[src]'))
        .map(el => el.href || el.src)
        .filter(Boolean)
    );
    await page.close();
    return new Set(urls);
  } catch (err) {
    console.log(`   ⚠️ Could not re-crawl ${pageUrl}: ${err.message}`);
    if (page) await page.close().catch(() => {});
    return null;  // null = crawl failed, fall back to URL-only check
  }
}

async function runFollowUpCheck(scanId, round) {
  const tracked = errorTracker.get(scanId);
  if (!tracked) return;

  const { siteName, siteConfig, issues } = tracked;
  const useProxy  = siteConfig.useProxy || false;
  const proxyLocs = siteConfig.proxyLocations || null;
  const autoGeo   = siteConfig.autoDetectGeo || false;

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`🔁 Follow-up check ${round}/3 for ${siteName} (${issues.length} link(s) to recheck)`);
  console.log(`${'─'.repeat(60)}`);

  // Group issues by their source page so we only re-crawl each page once
  const pageGroups = new Map();
  for (const issue of issues) {
    if (!pageGroups.has(issue.pageUrl)) pageGroups.set(issue.pageUrl, []);
    pageGroups.get(issue.pageUrl).push(issue);
  }

  const persistentErrors = [];
  const recoveredLinks   = [];

  for (const [pageUrl, pageIssues] of pageGroups) {
    const proxyForPage = (useProxy && PROXY_ENABLED)
      ? (autoGeo && pageIssues[0].pageProxyLocation ? pageIssues[0].pageProxyLocation : (proxyLocs ? proxyLocs[0] : null))
      : null;

    console.log(`   🌐 Re-crawling source page: ${pageUrl}`);
    const liveLinks = await getLiveLinksOnPage(pageUrl, proxyForPage);

    for (const issue of pageIssues) {
      let linkProxyLocation = null;
      if (useProxy && PROXY_ENABLED) {
        if (autoGeo && issue.pageProxyLocation) linkProxyLocation = [issue.pageProxyLocation];
        else if (proxyLocs && proxyLocs.length > 0) linkProxyLocation = proxyLocs;
      }

      console.log(`   🔍 Rechecking: ${issue.linkUrl.substring(0, 80)}...`);

      // If we successfully crawled the page and the broken URL is no longer in it,
      // the link was fixed at the source — no need to check the URL itself.
      if (liveLinks !== null && !liveLinks.has(issue.linkUrl)) {
        console.log(`   ✅ Link no longer present on page — was fixed at source`);
        recoveredLinks.push({ ...issue, fixedAtSource: true });
        continue;
      }

      // URL still on page (or crawl failed) — check whether it works now
      const statusInfo = await checkLinkStatus(issue.linkUrl, useProxy && PROXY_ENABLED, linkProxyLocation);

      if (!isSuccessStatus(statusInfo.status) && !statusInfo.treatAsWorking && !statusInfo.proxyIssue) {
        console.log(`   ❌ Still broken: ${statusInfo.status}`);
        persistentErrors.push({ ...issue, latestStatus: statusInfo.status, latestStatusText: statusInfo.statusText });
      } else {
        console.log(`   ✅ Recovered`);
        recoveredLinks.push(issue);
      }
    }
  }

  console.log(`\n   ✅ Recovered: ${recoveredLinks.length}  |  🔴 Still broken: ${persistentErrors.length}`);

  // Carry forward only still-broken links for the next round
  tracked.issues = persistentErrors;
  tracked.followUpsDone = round;

  if (round === 3 || persistentErrors.length === 0) {
    errorTracker.delete(scanId);
    if (persistentErrors.length === 0) console.log(`   🎉 All errors resolved for ${siteName}`);
  }

  if (token && SLACK_CHANNEL_ID) {
    await sendFollowUpSlackReport(siteName, round, persistentErrors, recoveredLinks);
  }
}

async function sendFollowUpSlackReport(siteName, round, persistentErrors, recoveredLinks) {
  if (!token || !SLACK_CHANNEL_ID) return;

  const isLastRound   = round === 3;
  const minutesMark   = round * 30;
  const priorityEmoji = { 'Critical': '🔴', 'High': '🟠', 'Medium': '🟡', 'Low': '🟢' };

  const headerText = isLastRound
    ? `🔁 Final Follow-up Check (3/3) — ${siteName}`
    : `🔁 Follow-up Check ${round}/3 — ${siteName}`;

  const blocks = [
    {
      type: 'header',
      text: { type: 'plain_text', text: headerText, emoji: true }
    },
    {
      type: 'context',
      elements: [{ type: 'mrkdwn', text: `Checked *${minutesMark} minutes* after the original scan` }]
    }
  ];

  // Persistent errors section
  if (persistentErrors.length > 0) {
    const sectionTitle = isLastRound
      ? `🔴 *Confirmed persistent errors (${persistentErrors.length}) — failed across all checks:*`
      : `🔴 *Still broken (${persistentErrors.length}):*`;

    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: sectionTitle } });

    // Chunk into groups of 5 to stay under Slack's 3000-char block limit
    const CHUNK = 5;
    for (let i = 0; i < persistentErrors.length; i += CHUNK) {
      const lines = persistentErrors.slice(i, i + CHUNK).map(issue => {
        const emoji  = priorityEmoji[issue.priority] || '⚪';
        const status = issue.latestStatus || issue.status;
        const stText = issue.latestStatusText || issue.statusText;
        return [
          `${emoji} *${issue.type}*  \`${status} ${stText}\``,
          `> URL: \`${issue.linkUrl}\``,
          `> Page: <${issue.pageUrl}|${issue.pageUrl}>`
        ].join('\n');
      });
      blocks.push({ type: 'section', text: { type: 'mrkdwn', text: lines.join('\n\n') } });
    }
  }

  // Recovered section
  if (recoveredLinks.length > 0) {
    const recoveredLines = recoveredLinks.map(i =>
      i.fixedAtSource
        ? `> 🛠️ \`${i.linkUrl}\`  _(removed/replaced on page)_`
        : `> ✅ \`${i.linkUrl}\`  _(URL now responding)_`
    ).join('\n');
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `*Resolved since last check (${recoveredLinks.length}):*\n${recoveredLines}` }
    });
  }

  // Footer context
  if (!isLastRound && persistentErrors.length > 0) {
    blocks.push({
      type: 'context',
      elements: [{ type: 'mrkdwn', text: `⏭️ Next recheck in 30 minutes (round ${round + 1}/3)` }]
    });
  } else if (isLastRound && persistentErrors.length > 0) {
    blocks.push({
      type: 'context',
      elements: [{ type: 'mrkdwn', text: `⚠️ These ${persistentErrors.length} error(s) persisted through all 3 rechecks. Manual investigation recommended.` }]
    });
  } else if (persistentErrors.length === 0) {
    blocks.push({
      type: 'context',
      elements: [{ type: 'mrkdwn', text: `🎉 All previously reported errors have been resolved.` }]
    });
  }

  blocks.push({ type: 'divider' });

  try {
    await slackClient.chat.postMessage({
      channel: SLACK_CHANNEL_ID,
      text: `${headerText} — ${persistentErrors.length} still broken, ${recoveredLinks.length} recovered`,
      blocks
    });
    console.log(`✅ Follow-up Slack report (round ${round}/3) sent for ${siteName}`);
  } catch (err) {
    console.error(`❌ Failed to send follow-up Slack report (round ${round}):`, err.message);
  }
}

// --- LINK CHECKING LOGIC WITH PROXY SUPPORT ---

const browserPool = new Map();

async function getBrowserForProxy(proxyLocation = null) {
  const key = proxyLocation || 'NO_PROXY';
  if (browserPool.has(key)) return browserPool.get(key);

  const launchOptions = {
    headless: 'new',
    protocolTimeout: 120000,  // 2 min — prevents Page.captureScreenshot CDP timeout
    args: [
      '--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security',
      '--ignore-certificate-errors', '--ignore-certificate-errors-spki-list',
      '--disable-features=IsolateOrigins,site-per-process'
    ]
  };

  let proxyCredentials = null;

  if (proxyLocation && PROXY_ENABLED) {
    const proxyConfig = getProxyForLocation(proxyLocation);
    if (proxyConfig) {
      const parsedProxy = parseProxyUrl(proxyConfig.url);
      if (parsedProxy) {
        launchOptions.args.push(`--proxy-server=${parsedProxy.serverUrl}`);
        proxyCredentials = { username: parsedProxy.username, password: parsedProxy.password };
        console.log(`   🚀 Launching browser for ${proxyLocation} proxy: ${parsedProxy.host}:${parsedProxy.port}`);
      }
    }
  }

  const browser = await puppeteer.launch(launchOptions);
  browserPool.set(key, { browser, credentials: proxyCredentials });
  return { browser, credentials: proxyCredentials };
}

async function closeAllBrowsers() {
  console.log('\n🔒 Closing all browser instances...');
  for (const [key, { browser }] of browserPool.entries()) {
    try { await browser.close(); console.log(`   ✅ Closed browser: ${key}`); }
    catch (error) { console.error(`   ❌ Error closing browser ${key}:`, error.message); }
  }
  browserPool.clear();
}

async function checkLinkWithBrowser(url, proxyLocation = null) {
  let page = null;
  try {
    const { browser, credentials } = await getBrowserForProxy(proxyLocation);
    page = await browser.newPage();
    if (credentials) await page.authenticate({ username: credentials.username, password: credentials.password });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

    const startTime = Date.now();
    const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    const finalUrl = page.url();
    const responseTime = Date.now() - startTime;
    const status = response ? response.status() : 'ERROR';
    await page.close();
    return {
      status, statusText: response ? response.statusText() : 'No response',
      responseTime, redirectCount: 0, finalUrl,
      checkedWithBrowser: true, isAffiliate: true,
      proxyLocation: proxyLocation || currentProxyLocation
    };
  } catch (error) {
    if (page) await page.close().catch(() => {});
    const msg = error.message || '';
    if (msg.includes('Timeout') || msg.includes('timeout'))
      return { status: 200, statusText: 'Slow response (timeout)', responseTime: 30000, treatAsWorking: true };
    if (msg.includes('detached') || msg.includes('Session'))
      return { status: 200, statusText: 'Browser session error', responseTime: 0, treatAsWorking: true };
    if (msg.includes('ERR_CERT') || msg.includes('ERR_SSL') || msg.includes('ERR_TUNNEL') ||
        msg.includes('ERR_CONNECTION_CLOSED') || msg.includes('ERR_SSL_PROTOCOL_ERROR') ||
        msg.includes('ERR_SSL_VERSION_OR_CIPHER_MISMATCH') || msg.includes('ERR_BAD_SSL_CLIENT_AUTH_CERT') ||
        msg.includes('net::ERR_SSL'))
      return { status: 200, statusText: 'Proxy SSL issue', responseTime: 0, treatAsWorking: true, proxyIssue: true };
    console.error(`   ❌ Browser error: ${error.message}`);
    return { status: 'ERROR', statusText: error.message, responseTime: 0, checkedWithBrowser: true, isAffiliate: true, proxyLocation: proxyLocation || currentProxyLocation };
  }
}

async function checkLinkWithGeoRotation(url, locations = null) {
  if (!PROXY_ENABLED) return await checkLinkWithBrowser(url, null);

  const locationsToTest = locations && locations.length > 0 ? locations : ['US'];
  const results = [];

  if (locationsToTest.length > 1) console.log(`   🌍 Testing in: ${locationsToTest.join(', ')}`);

  for (const location of locationsToTest) {
    if (locationsToTest.length > 1) console.log(`   🌍 Checking from ${location}...`);
    try {
      const result = await withRetry(() => checkLinkWithBrowser(url, location), 3, 2000);
      results.push({ ...result, location });

      if (result.statusText && (
        result.statusText.includes('ERR_TUNNEL_CONNECTION_FAILED') ||
        result.statusText.includes('ERR_NO_SUPPORTED_PROXIES') ||
        result.statusText.includes('ERR_PROXY_CONNECTION_FAILED')
      )) { console.log(`   ⚠️  Proxy error in ${location}, trying next...`); continue; }

      if (isSuccessStatus(result.status)) {
        if (locationsToTest.length > 1) console.log(`   ✅ Link works in ${location}`);
        break;
      } else {
        if (locationsToTest.length > 1) console.log(`   ❌ Failed in ${location} (${result.status})`);
      }
    } catch (error) {
      console.log(`   ❌ Exception in ${location}: ${error.message}`);
      results.push({ status: 'ERROR', statusText: error.message, location });
    }
  }

  const successfulResult = results.find(r => isSuccessStatus(r.status));
  if (successfulResult) return { ...successfulResult, testedLocations: results.map(r => r.location), geoRestricted: false };

  const lastResult = results[results.length - 1] || results[0];
  return { ...lastResult, testedLocations: results.map(r => r.location), geoRestricted: results.length > 1, allResults: results };
}

async function checkLinkStatus(url, useGeoRotation = false, proxyLocations = null) {
  try {
    if (isTrackingPixel(url)) return { status: 200, statusText: 'Tracking Pixel', responseTime: 0, skip: true };
    if (isWhitelistedUrl(url)) return { status: 200, statusText: 'Whitelisted URL', responseTime: 0, skip: true };
    if (url.startsWith('data:') || url.startsWith('blob:')) return { status: 200, statusText: 'Inline Content', responseTime: 0, skip: true };
    if (url.startsWith('mailto:')) {
      const emailPattern = /^mailto:[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
      return { status: emailPattern.test(url) ? 200 : 'Invalid', statusText: emailPattern.test(url) ? 'Valid Email' : 'Malformed Email', responseTime: 0 };
    }
    if (url.startsWith('tel:') || url.startsWith('javascript:') || url.startsWith('#'))
      return { status: 200, statusText: 'Internal/Phone', responseTime: 0 };

    if (isAffiliateLink(url)) {
      const result = useGeoRotation && PROXY_ENABLED
        ? await checkLinkWithGeoRotation(url, proxyLocations)
        : await checkLinkWithBrowser(url, proxyLocations ? proxyLocations[0] : null);

      if (result.statusText && (
        result.statusText.includes('ERR_CERT_AUTHORITY_INVALID') || result.statusText.includes('ERR_SSL_PROTOCOL_ERROR') ||
        result.statusText.includes('ERR_TUNNEL_CONNECTION_FAILED') || result.statusText.includes('ERR_CONNECTION_CLOSED') ||
        result.statusText.includes('Session with given id not found') || result.statusText.includes('ERR_SSL_VERSION_OR_CIPHER_MISMATCH') ||
        result.statusText.includes('ERR_BAD_SSL_CLIENT_AUTH_CERT') || result.statusText.includes('net::ERR_SSL') ||
        result.statusText.includes('500 - Internal Server Error') || result.statusText.includes('ERR_EMPTY_RESPONSE at') ||
        result.statusText.includes('Parse Error: Header overflow') ||
        result.statusText.includes('unable to verify the first certificate; if the root CA is installed locally, try running Node.js with --use-system-ca')
      )) {
        console.log(`   ⚠️ Proxy SSL issue (link likely works): ${url.substring(0, 60)}...`);
        return { status: 200, statusText: 'Proxy SSL issue (link works)', responseTime: result.responseTime, treatAsWorking: true, proxyIssue: true };
      }

      if (result.status === 403) return { ...result, treatAsWorking: true, statusText: 'Anti-bot protection (link works in browsers)' };
      return result;
    }

    const startTime = Date.now();
    const axiosConfig = {
      timeout: 60000, maxRedirects: 5, validateStatus: () => true,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      }
    };
    const response = await axios.get(url, axiosConfig);
    return {
      status: response.status, statusText: response.statusText,
      responseTime: Date.now() - startTime,
      redirectCount: response.request._redirectable?._redirectCount || 0,
      finalUrl: response.request.res?.responseUrl || url
    };
  } catch (error) {
    if (error.code === 'ENOTFOUND')     return { status: 'DNS_ERROR',           statusText: 'Domain not found',      responseTime: 0 };
    if (error.code === 'ETIMEDOUT' ||
        error.code === 'ECONNABORTED')  return { status: 'TIMEOUT',             statusText: 'Request timeout',       responseTime: 60000 };
    if (error.code === 'ECONNREFUSED')  return { status: 'CONNECTION_REFUSED',  statusText: 'Connection refused',    responseTime: 0 };
    if (error.response)                 return { status: error.response.status, statusText: error.response.statusText, responseTime: 0 };
    return { status: 'ERROR', statusText: error.message, responseTime: 0 };
  }
}

function determinePriorityFromStatus(status) {
  if (status === 404 || status === 500 || status === 'ERROR') return 'Critical';
  if (status === 403 || status === 401 || status === 'TIMEOUT') return 'High';
  if (status >= 300 && status < 400) return 'Medium';
  return 'Low';
}

function getIssueType(status) {
  if (status === 404) return 'Broken Link (404)';
  if (status === 403) return 'Access Forbidden (403)';
  if (status === 500) return 'Server Error (500)';
  if (status === 'TIMEOUT') return 'Timeout';
  if (status === 'DNS_ERROR') return 'DNS Error';
  return 'Unknown Issue';
}

function calculateImpactScore(linkData, appearanceCount) {
  let score = 50;
  if (linkData.status === 404) score += 30;
  else if (linkData.status >= 500) score += 35;
  if (linkData.context.toLowerCase().includes('cta')) score += 20;
  if (linkData.context.toLowerCase().includes('nav')) score += 10;
  score += Math.min(appearanceCount * 5, 20);
  return Math.min(100, score);
}

// --- SCREENSHOT FUNCTION ---

async function takeErrorScreenshot(pageUrl, selector, proxyLocation = null) {
  let page = null;
  try {
    console.log(`\n📸 SCREENSHOT: Loading ${pageUrl}`);
    const { browser, credentials } = await getBrowserForProxy(proxyLocation);
    page = await browser.newPage();
    if (credentials) await page.authenticate({ username: credentials.username, password: credentials.password });
    await page.setViewport({ width: 1920, height: 1080 });
    await page.goto(pageUrl, { waitUntil: 'networkidle0', timeout: 0 }).catch(() => {});

    if (!selector) {
      console.log('❌ No selector provided for screenshot');
      await page.close();
      await browser.close();
      return null;
    }

    const bodyHeight = await page.evaluate(() => Math.max(
      document.body.scrollHeight, document.body.offsetHeight,
      document.documentElement.clientHeight, document.documentElement.scrollHeight,
      document.documentElement.offsetHeight
    ));
    console.log(`   📏 Resizing viewport to full height: ${bodyHeight}px`);
    await page.setViewport({ width: 1920, height: bodyHeight + 500 });
    console.log(`🎯 Targeting specific element: ${selector}`);

    try { await page.waitForSelector(selector, { timeout: 0 }); }
    catch (e) { console.log('❌ Element not found in DOM via selector'); await page.close(); await browser.close(); return null; }

    const elementHandle = await page.$(selector);
    await page.evaluate((el) => {
      let parent = el.parentElement;
      while (parent) {
        const style = window.getComputedStyle(parent);
        if (style.display === 'none')      parent.style.setProperty('display', 'block', 'important');
        if (style.visibility === 'hidden') parent.style.setProperty('visibility', 'visible', 'important');
        if (style.opacity === '0')         parent.style.setProperty('opacity', '1', 'important');
        if (style.height === '0px')        parent.style.setProperty('height', 'auto', 'important');
        if (parent.tagName === 'DETAILS')  parent.open = true;
        parent = parent.parentElement;
      }
      el.style.setProperty('display', 'inline-block', 'important');
      el.style.setProperty('visibility', 'visible', 'important');
      el.style.setProperty('opacity', '1', 'important');
      el.style.border = '5px solid #ff0000';
      el.style.boxShadow = '0 0 0 5px white, 0 0 15px 5px rgba(255,0,0,0.8)';
      el.style.borderRadius = '4px';
      el.style.zIndex = '2147483647';
      el.style.position = 'relative';
      el.setAttribute('data-error-highlight', 'true');
      el.scrollIntoView({ behavior: 'auto', block: 'center', inline: 'center' });
    }, elementHandle);

    await new Promise(resolve => setTimeout(resolve, 3000));
    const boundingBox = await elementHandle.boundingBox();

    if (boundingBox) {
      const screenshot = await page.screenshot({
        encoding: 'base64', type: 'jpeg', quality: 80,
        clip: {
          x: Math.max(0, boundingBox.x - 500), y: Math.max(0, boundingBox.y - 500),
          width: Math.min(1920, boundingBox.width + 1000), height: Math.min(1080, boundingBox.height + 1000)
        }
      });
      console.log(`✅ Element Screenshot captured`);
      await page.close();
      return screenshot;
    } else {
      console.log('⚠️ Element has 0 size, falling back to viewport screenshot');
      const screenshot = await page.screenshot({ encoding: 'base64', type: 'jpeg', quality: 80 });
      await page.close();
      return screenshot;
    }
  } catch (error) {
    console.log(`❌ Screenshot FAILED: ${error.message}`);
    if (page) await page.close().catch(() => {});
    return null;
  }
}

// --- CRAWLER WITH SELECTOR GENERATION ---

const generateUniqueSelector = `
  function getUniqueSelector(el) {
    if (el.id) return '#' + el.id;
    let path = [];
    while (el.nodeType === Node.ELEMENT_NODE) {
      let selector = el.nodeName.toLowerCase();
      if (el.id) { selector = '#' + el.id; path.unshift(selector); break; }
      else {
        let sib = el, nth = 1;
        while (sib = sib.previousElementSibling) { if (sib.nodeName.toLowerCase() == selector) nth++; }
        if (nth != 1) selector += ":nth-of-type("+nth+")";
      }
      path.unshift(selector);
      el = el.parentNode;
    }
    return path.join(" > ");
  }
`;

async function crawlSpecificPages(pagesConfig, siteConfig = {}) {
  const allLinks = [];
  const scannedPages = new Set();

  for (const pageConfig of pagesConfig) {
    const pageUrl   = typeof pageConfig === 'string' ? pageConfig : pageConfig.url;
    const pageProxy = typeof pageConfig === 'object'  ? pageConfig.proxy : null;

    if (scannedPages.has(pageUrl)) { console.log(`⏭️  Skipping ${pageUrl} (already scanned)`); continue; }
    scannedPages.add(pageUrl);

    let proxyForPage = null;
    if (siteConfig.useProxy) {
      if (pageProxy) proxyForPage = pageProxy;
      else if (siteConfig.autoDetectGeo) proxyForPage = getProxyLocationForUrl(pageUrl, siteConfig.defaultProxyLocation || 'US');
      else if (siteConfig.proxyLocations && siteConfig.proxyLocations.length > 0) proxyForPage = siteConfig.proxyLocations[0];
      else proxyForPage = siteConfig.defaultProxyLocation || 'US';
    }

    console.log(`Scanning page: ${pageUrl}${proxyForPage ? ` (via ${proxyForPage} proxy)` : ''}`);
    let page = null;

    try {
      const { browser, credentials } = await getBrowserForProxy(proxyForPage);
      page = await browser.newPage();
      if (credentials) await page.authenticate({ username: credentials.username, password: credentials.password });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
      await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 0 });
      await page.evaluate(generateUniqueSelector);

      const links = await page.evaluate(() => {
        const results = [];
        document.querySelectorAll('a[href], img[src], link[href], script[src]').forEach(el => {
          let url, text, type;
          if (el.tagName === 'A')      { url = el.href;  text = el.innerText.trim() || el.getAttribute('aria-label') || 'No text'; type = 'link'; }
          else if (el.tagName === 'IMG')    { url = el.src;   text = el.alt || 'Image';      type = 'image'; }
          else if (el.tagName === 'LINK')   { url = el.href;  text = 'Stylesheet';            type = 'css'; }
          else if (el.tagName === 'SCRIPT') { url = el.src;   text = 'Script';               type = 'script'; }
          if (!url) return;

          let context = 'Unknown';
          const parent = el.closest('header, nav, footer, main, section, aside, div');
          if (parent) {
            const classes = parent.className || '';
            const id = parent.id || '';
            context = `${parent.tagName.toLowerCase()}${id ? '#' + id : ''}${classes ? '.' + classes.split(' ')[0] : ''}`;
          }
          if (el.classList && (el.classList.contains('btn') || el.classList.contains('cta'))) context += ' - CTA button';
          const selector = window.getUniqueSelector ? window.getUniqueSelector(el) : null;
          results.push({ url, text, context, type, selector });
        });
        return results;
      });

      allLinks.push(...links.map(link => ({ ...link, pageUrl, pageProxyLocation: proxyForPage })));
      await page.close();
    } catch (error) {
      // SSL/cipher errors from the proxy can't be bypassed with --ignore-certificate-errors.
      // Fall back to a direct (no-proxy) crawl so we still collect links from the page.
      // pageProxyLocation is kept on each link so link *checking* still uses the proxy.
      const isProxySSLError = proxyForPage && (
        error.message.includes('ERR_SSL_VERSION_OR_CIPHER_MISMATCH') ||
        error.message.includes('ERR_SSL_PROTOCOL_ERROR') ||
        error.message.includes('ERR_SSL_VERSION_INTERFERENCE') ||
        error.message.includes('net::ERR_SSL') ||
        error.message.includes('ERR_CERT')
      );

      if (isProxySSLError) {
        console.log(`   ⚠️ SSL/proxy error on ${pageUrl} — retrying without proxy to collect links...`);
        if (page) await page.close().catch(() => {});
        page = null;
        try {
          const { browser: directBrowser } = await getBrowserForProxy(null);
          page = await directBrowser.newPage();
          await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
          await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 0 });
          await page.evaluate(generateUniqueSelector);

          const links = await page.evaluate(() => {
            const results = [];
            document.querySelectorAll('a[href], img[src], link[href], script[src]').forEach(el => {
              let url, text, type;
              if (el.tagName === 'A')           { url = el.href;  text = el.innerText.trim() || el.getAttribute('aria-label') || 'No text'; type = 'link'; }
              else if (el.tagName === 'IMG')    { url = el.src;   text = el.alt || 'Image';  type = 'image'; }
              else if (el.tagName === 'LINK')   { url = el.href;  text = 'Stylesheet';       type = 'css'; }
              else if (el.tagName === 'SCRIPT') { url = el.src;   text = 'Script';           type = 'script'; }
              if (!url) return;
              let context = 'Unknown';
              const parent = el.closest('header, nav, footer, main, section, aside, div');
              if (parent) {
                const classes = parent.className || '';
                const id = parent.id || '';
                context = `${parent.tagName.toLowerCase()}${id ? '#' + id : ''}${classes ? '.' + classes.split(' ')[0] : ''}`;
              }
              if (el.classList && (el.classList.contains('btn') || el.classList.contains('cta'))) context += ' - CTA button';
              const selector = window.getUniqueSelector ? window.getUniqueSelector(el) : null;
              results.push({ url, text, context, type, selector });
            });
            return results;
          });

          // Keep proxyForPage so link checking still routes through the correct proxy
          allLinks.push(...links.map(link => ({ ...link, pageUrl, pageProxyLocation: proxyForPage })));
          console.log(`   ✅ Direct crawl succeeded — collected ${links.length} links from ${pageUrl}`);
          await page.close();
        } catch (retryError) {
          console.error(`   ❌ Direct crawl also failed for ${pageUrl}:`, retryError.message);
          if (page) await page.close().catch(() => {});
        }
      } else {
        console.error(`Error scanning ${pageUrl}:`, error.message);
        if (page) await page.close().catch(() => {});
      }
    }
  }

  return { pages: Array.from(scannedPages), links: allLinks };
}

// --- CONFIGURATION: DOMAINS TO SCAN ---

const DOMAINS_TO_SCAN = [
  {
    name: 'https://top10payrollservice.com/',
    pages: [
      'https://top10payrollservice.com/home',
      'https://top10payrollservice.com/about-us/',
      'https://top10payrollservice.com/cookie-policy/',
      'https://top10payrollservice.com/terms-of-use/',
      'https://top10payrollservice.com/partner-with-us/',
      'https://top10payrollservice.com/gusto-review/',
      'https://top10payrollservice.com/surepayroll-review/',
      'https://top10payrollservice.com/remote-review/',
      'https://top10payrollservice.com/deel-review/',
      'https://top10payrollservice.com/p-payroll/',
      'https://top10payrollservice.com/paystub/',
      'https://top10payrollservice.com/payroll-providers/',
      'https://top10payrollservice.com/payroll-services/',
      'https://top10payrollservice.com/payroll-software/',
      'https://top10payrollservice.com/payroll-companies/',
      'https://top10payrollservice.com/payroll-apps/',
      'https://top10payrollservice.com/b-paystub/',
      'https://top10payrollservice.com/b-payroll/',
      'https://top10payrollservice.com/m-home/',
      'https://top10payrollservice.com/m-paystub/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10carwarranty.com/',
    pages: [
      'https://top10carwarranty.com/home/',
      'https://top10carwarranty.com/crossover-car-warranty/',
      'https://top10carwarranty.com/crossover-electric-vehicle-warranty/',
      'https://top10carwarranty.com/luxury-car-warranty/',
      'https://top10carwarranty.com/passenger-car-warranty/',
      'https://top10carwarranty.com/pickup-truck-warranty/',
      'https://top10carwarranty.com/suv-warranty/',
      'https://top10carwarranty.com/used-car-warranty/',
      'https://top10carwarranty.com/van-minivan-warranty/',
      'https://top10carwarranty.com/home-warranty/',
      'https://top10carwarranty.com/american-dream-review/',
      'https://top10carwarranty.com/autopom-review/',
      'https://top10carwarranty.com/carshield-review/',
      'https://top10carwarranty.com/about-us/',
      'https://top10carwarranty.com/contact-us/',
      'https://top10carwarranty.com/cookie-policy/',
      'https://top10carwarranty.com/partner-with-us/',
      'https://top10carwarranty.com/privacy-policy/',
      'https://top10carwarranty.com/terms-of-use/',
      'https://top10carwarranty.com/chaiz-review/',
      'https://top10carwarranty.com/choice-review/',
      'https://top10carwarranty.com/premier-auto-review/',
      'https://top10carwarranty.com/select-auto-protect-review/',
      'https://top10carwarranty.com/first-american-review/',
      'https://top10carwarranty.com/carchex-review/',
      'https://top10carwarranty.com/p-cw/',
      'https://top10carwarranty.com/b-home/',
      'https://top10carwarranty.com/b-crossover-car-warranty/',
      'https://top10carwarranty.com/b-crossover-electric-vehicle-warranty/',
      'https://top10carwarranty.com/b-luxury-car-warranty/',
      'https://top10carwarranty.com/b-pickup-truck-warranty/',
      'https://top10carwarranty.com/b-suv-warranty/',
      'https://top10carwarranty.com/b-used-car-warranty/',
      'https://top10carwarranty.com/b-van-minivan-warranty/',
      'https://top10carwarranty.com/b-home-warranty/',
      'https://top10carwarranty.com/auto-vehicle-warranty/',
      'https://top10carwarranty.com/b-auto-vehicle-warranty/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://best10homewarranties.com/',
    pages: [
      'https://best10homewarranties.com/american-home-shield-warranty-alternatives/',
      'https://best10homewarranties.com/ac-warranty',
      'https://best10homewarranties.com/best-garage-doors-warranty/',
      'https://best10homewarranties.com/best-home-protection-warranty/',
      'https://best10homewarranties.com/best-home-warranty-companies/',
      'https://best10homewarranties.com/best-home-warranty-for-condos/',
      'https://best10homewarranties.com/best-home-warranty-plans/',
      'https://best10homewarranties.com/best-home-warranty-services/',
      'https://best10homewarranties.com/best-pool-warranty-reviews/',
      'https://best10homewarranties.com/best-refrigerator-warranty-reviews/',
      'https://best10homewarranties.com/best-roof-warranty-companies/',
      'https://best10homewarranties.com/best-sewer-line-warranty/',
      'https://best10homewarranties.com/best-stove-warranty/',
      'https://best10homewarranties.com/best-water-heater-warranty/',
      'https://best10homewarranties.com/cheap-home-warranty/',
      'https://best10homewarranties.com/choice-home-warranty/',
      'https://best10homewarranties.com/home-appliances-warranty/',
      'https://best10homewarranties.com/home-warranty-insurance-quotes/',
      'https://best10homewarranties.com/home-warranty-insurance/',
      'https://best10homewarranties.com/home/',
      'https://best10homewarranties.com/b-american-home-shield-warranty-alternatives/',
      'https://best10homewarranties.com/b-best-ac-warranty-reviews/',
      'https://best10homewarranties.com/b-best-home-warranty-for-condos/',
      'https://best10homewarranties.com/b-best-sewer-line-warranty/',
      'https://best10homewarranties.com/b-choice-home-warranty/',
      'https://best10homewarranties.com/b-home-appliances-warranty/',
      'https://best10homewarranties.com/b-home-warranty/',
      'https://best10homewarranties.com/best-home-repair-warranty/',
      'https://best10homewarranties.com/best-home-warranty-rental/',
      'https://best10homewarranties.com/home-warranty-for-veterans/',
      'https://best10homewarranties.com/mobile-home-warranty/',
      'https://best10homewarranties.com/about-us/',
      'https://best10homewarranties.com/cookie-policy/',
      'https://best10homewarranties.com/terms-of-use/',
      'https://best10homewarranties.com/privacy-policy/',
      'https://best10homewarranties.com/partner-with-us/',
      'https://best10homewarranties.com/choice-review/',
      'https://best10homewarranties.com/select-review/',
      'https://best10homewarranties.com/first-premier-review/',
      'https://best10homewarranties.com/first-american-review/',
      'https://best10homewarranties.com/hwa-review/',
      'https://best10homewarranties.com/hsc-review/',
      'https://best10homewarranties.com/p-hw/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10goldinvestment.com/',
    pages: [
      'https://top10goldinvestment.com/buying-gold/',
      'https://top10goldinvestment.com/gold-silver/',
      'https://top10goldinvestment.com/home/',
      'https://top10goldinvestment.com/invest-in-gold/',
      'https://top10goldinvestment.com/ira-investments/',
      'https://top10goldinvestment.com/precious-metals/',
      'https://top10goldinvestment.com/ira-silver/',
      'https://top10goldinvestment.com/b-buying-gold/',
      'https://top10goldinvestment.com/about-us/',
      'https://top10goldinvestment.com/cookie-policy/',
      'https://top10goldinvestment.com/privacy-policy/',
      'https://top10goldinvestment.com/terms-of-use/',
      'https://top10goldinvestment.com/partner-with-us/',
      'https://top10goldinvestment.com/augusta-review/',
      'https://top10goldinvestment.com/birch-review/',
      'https://top10silverinvestment.com/preserve-gold-review/',
      'https://top10goldinvestment.com/hartford-review/',
      'https://top10goldinvestment.com/learcapital-review/',
      'https://top10goldinvestment.com/priority-gold-review/',
      'https://top10goldinvestment.com/es-buying-gold/',
      'https://top10goldinvestment.com/es-ira-investments/',
      'https://top10goldinvestment.com/es-cookie-policy/',
      'https://top10goldinvestment.com/es-about-us/',
      'https://top10goldinvestment.com/es-terms-of-use/',
      'https://top10goldinvestment.com/es-privacy-policy/',
      'https://top10goldinvestment.com/es-partner-with-us/',
      'https://top10goldinvestment.com/es-hartford-review/',
      'https://top10goldinvestment.com/es-learcapital-review/',
      'https://top10goldinvestment.com/es-augusta-review/',
      'https://top10goldinvestment.com/es-birch-review/',
      'https://top10silverinvestment.com/es-preserve-gold-review/',
      'https://top10goldinvestment.com/silver-investments/',
      'https://top10goldinvestment.com/b-silver-investments/',
      'https://top10goldinvestment.com/b-precious-metals/',
      'https://top10goldinvestment.com/es-priority-gold-review/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10stocktrading.com/',
    pages: [
      'https://top10stocktrading.com/home/',
      'https://top10stocktrading.com/cookie-policy/',
      'https://top10stocktrading.com/terms-of-use/',
      'https://top10stocktrading.com/privacy-policy/',
      'https://top10stocktrading.com/advertiser-disclosure/',
      'https://top10stocktrading.com/partner-with-us/',
      'https://top10stocktrading.com/sofi-review/',
      'https://top10stocktrading.com/robinhood-review/',
      'https://top10stocktrading.com/public-review/',
      'https://top10stocktrading.com/kraken-review/',
      'https://top10stocktrading.com/moomoo-review/',
      'https://top10stocktrading.com/plus500-review/',
      'https://top10stocktrading.com/b-stocks/',
      'https://top10stocktrading.com/beginners/',
      'https://top10stocktrading.com/day-trading/',
      'https://top10stocktrading.com/ira/',
      'https://top10stocktrading.com/etoro-review/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10psychicreading.com/',
    pages: [
      'https://top10psychicreading.com/home/',
      'https://top10psychicreading.com/cookie-policy/',
      'https://top10psychicreading.com/terms-of-use/',
      'https://top10psychicreading.com/privacy-policy/',
      'https://top10psychicreading.com/keen-review/',
      'https://top10psychicreading.com/kasamba-review/',
      'https://top10psychicreading.com/purple-garden-review/',
      'https://top10psychicreading.com/california-psychics-review/',
      'https://top10psychicreading.com/b-home/',
      'https://top10psychicreading.com/partner-with-us/',
      'https://top10psychicreading.com/tarot/',
      'https://top10psychicreading.com/astrology-horoscopes/',
      'https://top10psychicreading.com/financial/',
      'https://top10psychicreading.com/love-relationships/',
      'https://top10psychicreading.com/dream-analysis/https://top10psychicreading.com/dream-analysis/',
      'https://top10psychicreading.com/lifepath/',
      'https://top10psychicreading.com/mediums/',
      'https://top10psychicreading.com/palm-reading/',
      'https://top10psychicreading.com/spiritual-blossom-review/',
      'https://top10psychicreading.com/mysticsense-review/',
      'https://top10psychicreading.com/chat/',
      'https://top10psychicreading.com/phone-calls/',
      'https://top10psychicreading.com/cheap/',
      'https://top10psychicreading.com/trusted-legit/',
      'https://top10psychicreading.com/fortune-reading/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://best10-antiviruses.com/',
    pages: [
      'https://best10-antiviruses.com/home',
      'https://best10-antiviruses.com/uk-mac-and-ios/',
      'https://best10-antiviruses.com/uk-intego-review/',
      'https://best10-antiviruses.com/uk-kaspersky-review/',
      'https://best10-antiviruses.com/uk-panda-review/',
      'https://best10-antiviruses.com/uk-norton-review/',
      'https://best10-antiviruses.com/uk-totalav-review/',
      'https://best10-antiviruses.com/uk-avira-review/',
      'https://best10-antiviruses.com/uk-avast-review/',
      'https://best10-antiviruses.com/uk-mcafee-review/',
      'https://best10-antiviruses.com/uk-surfshark-review/',
      'https://best10-antiviruses.com/uk-cookie-policy/',
      'https://best10-antiviruses.com/uk-terms-of-use/',
      'https://best10-antiviruses.com/uk-privacy-policy/',
      'https://best10-antiviruses.com/uk-windows-and-android/',
      'https://best10-antiviruses.com/home-mac-and-ios/',
      'https://best10-antiviruses.com/home-norton-review/',
      'https://best10-antiviruses.com/home-windows-and-android/',
      'https://best10-antiviruses.com/home-panda-review/',
      'https://best10-antiviruses.com/home-mcafee-review/',
      'https://best10-antiviruses.com/home-totalav-review/',
      'https://best10-antiviruses.com/home-cookie-policy/',
      'https://best10-antiviruses.com/home-terms-of-use/',
      'https://best10-antiviruses.com/home-privacy-policy/',
      'https://best10-antiviruses.com/home-surfshark-review/',
      'https://best10-antiviruses.com/mx-windows-and-android/',
      'https://best10-antiviruses.com/mx-totalav-review/',
      'https://best10-antiviruses.com/mx-norton-review/',
      'https://best10-antiviruses.com/mx-avast-review/',
      'https://best10-antiviruses.com/mx-kaspersky-review/',
      'https://best10-antiviruses.com/mx-surfshark-review/',
      'https://best10-antiviruses.com/mx-panda-review/',
      'https://best10-antiviruses.com/mx-avira-review/',
      'https://best10-antiviruses.com/mx-mcafee-review/',
      'https://best10-antiviruses.com/mx-cookie-policy/',
      'https://best10-antiviruses.com/mx-terms-of-use/',
      'https://best10-antiviruses.com/mx-privacy-policy/',
      'https://best10-antiviruses.com/mx-mac-and-ios/',
      'https://best10-antiviruses.com/mx-intego-review/',
      'https://best10-antiviruses.com/es-windows-and-android/',
      'https://best10-antiviruses.com/es-avast-review/',
      'https://best10-antiviruses.com/es-totalav-review/',
      'https://best10-antiviruses.com/es-kaspersky-review/',
      'https://best10-antiviruses.com/es-panda-review/',
      'https://best10-antiviruses.com/es-norton-review/',
      'https://best10-antiviruses.com/es-mcafee-review/',
      'https://best10-antiviruses.com/es-avira-review/',
      'https://best10-antiviruses.com/es-surfshark-review/',
      'https://best10-antiviruses.com/es-cookie-policy/',
      'https://best10-antiviruses.com/es-terms-of-use/',
      'https://best10-antiviruses.com/es-privacy-policy/',
      'https://best10-antiviruses.com/es-mac-and-ios/',
      'https://best10-antiviruses.com/es-intego-review/',
      'https://best10-antiviruses.com/it-mac-and-ios/',
      'https://best10-antiviruses.com/it-kaspersky-review/',
      'https://best10-antiviruses.com/it-panda-review/',
      'https://best10-antiviruses.com/it-avira-review/',
      'https://best10-antiviruses.com/it-avast-review/',
      'https://best10-antiviruses.com/it-norton-review/',
      'https://best10-antiviruses.com/it-totalav-review/',
      'https://best10-antiviruses.com/it-surfshark-review/',
      'https://best10-antiviruses.com/it-mcafee-review/',
      'https://best10-antiviruses.com/it-cookie-policy/',
      'https://best10-antiviruses.com/it-terms-of-use/',
      'https://best10-antiviruses.com/it-privacy-policy/',
      'https://best10-antiviruses.com/it-windows-and-android/',
      'https://best10-antiviruses.com/de-mac-and-ios/',
      'https://best10-antiviruses.com/de-kaspersky-review/',
      'https://best10-antiviruses.com/de-avira-review/',
      'https://best10-antiviruses.com/de-norton-review/',
      'https://best10-antiviruses.com/de-totalav-review/',
      'https://best10-antiviruses.com/de-avast-review/',
      'https://best10-antiviruses.com/de-surfshark-review/',
      'https://best10-antiviruses.com/de-panda-review/',
      'https://best10-antiviruses.com/de-mcafee-review/',
      'https://best10-antiviruses.com/de-cookie-policy/',
      'https://best10-antiviruses.com/de-terms-of-use/',
      'https://best10-antiviruses.com/de-privacy-policy/',
      'https://best10-antiviruses.com/de-intego-review/',
      'https://best10-antiviruses.com/pl-windows-and-android/',
      'https://best10-antiviruses.com/pl-avast-review/',
      'https://best10-antiviruses.com/pl-panda-review/',
      'https://best10-antiviruses.com/pl-totalav-review/',
      'https://best10-antiviruses.com/pl-norton-review/',
      'https://best10-antiviruses.com/pl-avira-review/',
      'https://best10-antiviruses.com/pl-surfshark-review/',
      'https://best10-antiviruses.com/pl-mcafee-review/',
      'https://best10-antiviruses.com/pl-cookie-policy/',
      'https://best10-antiviruses.com/pl-terms-of-use/',
      'https://best10-antiviruses.com/pl-privacy-policy/',
      'https://best10-antiviruses.com/pl-mac-and-ios/',
      'https://best10-antiviruses.com/pl-intego-review/',
      'https://best10-antiviruses.com/pl-mackeeper-review/',
      'https://best10-antiviruses.com/br-mac-and-ios/',
      'https://best10-antiviruses.com/br-kaspersky-review/',
      'https://best10-antiviruses.com/br-panda-review/',
      'https://best10-antiviruses.com/br-norton-review/',
      'https://best10-antiviruses.com/br-avast-review/',
      'https://best10-antiviruses.com/br-mcafee-review/',
      'https://best10-antiviruses.com/br-avira-review/',
      'https://best10-antiviruses.com/br-totalav-review/',
      'https://best10-antiviruses.com/br-surfshark-review/',
      'https://best10-antiviruses.com/br-cookie-policy/',
      'https://best10-antiviruses.com/br-terms-of-use/',
      'https://best10-antiviruses.com/br-privacy-policy/',
      'https://best10-antiviruses.com/br-windows-and-android/',
      'https://best10-antiviruses.com/jp-mac-and-ios/',
      'https://best10-antiviruses.com/jp-panda-review/',
      'https://best10-antiviruses.com/jp-totalav-review/',
      'https://best10-antiviruses.com/jp-mcafee-review/',
      'https://best10-antiviruses.com/jp-surfshark-review/',
      'https://best10-antiviruses.com/jp-cookie-policy/',
      'https://best10-antiviruses.com/jp-privacy-policy/',
      'https://best10-antiviruses.com/jp-terms-of-use/',
      'https://best10-antiviruses.com/nl-mac-and-ios/',
      'https://best10-antiviruses.com/nl-mcafee-review/',
      'https://best10-antiviruses.com/nl-norton-review/',
      'https://best10-antiviruses.com/nl-panda-review/',
      'https://best10-antiviruses.com/nl-totalav-review/',
      'https://best10-antiviruses.com/nl-kaspersky-review/',
      'https://best10-antiviruses.com/nl-avast-review/',
      'https://best10-antiviruses.com/nl-surfshark-review/',
      'https://best10-antiviruses.com/nl-avira-review/',
      'https://best10-antiviruses.com/nl-cookie-policy/',
      'https://best10-antiviruses.com/nl-terms-of-use/',
      'https://best10-antiviruses.com/nl-privacy-policy/',
      'https://best10-antiviruses.com/nl-intego-review/'
    ],
    useProxy: true, autoDetectGeo: true, defaultProxyLocation: 'GB'
  },
  {
    name: 'https://top10silverinvestment.com/',
    pages: [
      'https://top10silverinvestment.com/home/',
      'https://top10silverinvestment.com/b-home/',
      'https://top10silverinvestment.com/b-buying-gold/',
      'https://top10silverinvestment.com/buying-gold/',
      'https://top10silverinvestment.com/precious-metals/',
      'https://top10silverinvestment.com/b-precious-metals/',
      'https://top10silverinvestment.com/ira-silver/',
      'https://top10silverinvestment.com/b-ira-silver/',
      'https://top10silverinvestment.com/gold-ira/',
      'https://top10silverinvestment.com/b-gold-ira/',
      'https://top10silverinvestment.com/hartford-review/',
      'https://top10silverinvestment.com/priority-gold-review/',
      'https://top10silverinvestment.com/learcapital-review/',
      'https://top10silverinvestment.com/preserve-gold-review/',
      'https://top10silverinvestment.com/birch-review/',
      'https://top10silverinvestment.com/about-us/',
      'https://top10silverinvestment.com/cookie-policy/',
      'https://top10silverinvestment.com/terms-of-use/',
      'https://top10silverinvestment.com/privacy-policy/',
      'https://top10silverinvestment.com/advertise-with-us/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10parentalcontrol.com/',
    pages: [
      'https://top10parentalcontrol.com/home-general/',
      'https://top10parentalcontrol.com/us-bark-review/',
      'https://top10parentalcontrol.com/us-qustodio-review/',
      'https://top10parentalcontrol.com/us-norton-review/',
      'https://top10parentalcontrol.com/us-mspy-review/',
      'https://top10parentalcontrol.com/us-eyezy-review/',
      'https://top10parentalcontrol.com/us-parentaler-review/',
      'https://top10parentalcontrol.com/us-cookie-policy/',
      'https://top10parentalcontrol.com/us-terms-of-use/',
      'https://top10parentalcontrol.com/us-privacy-policy/',
      'https://top10parentalcontrol.com/us-home/',
      'https://top10parentalcontrol.com/au-home/',
      'https://top10parentalcontrol.com/au-qustodio-review/',
      'https://top10parentalcontrol.com/au-bark-review/',
      'https://top10parentalcontrol.com/au-mspy-review/',
      'https://top10parentalcontrol.com/au-norton-review/',
      'https://top10parentalcontrol.com/au-eyezy-review/',
      'https://top10parentalcontrol.com/au-parentaler-review/',
      'https://top10parentalcontrol.com/au-cookie-policy/',
      'https://top10parentalcontrol.com/au-terms-of-use/',
      'https://top10parentalcontrol.com/au-privacy-policy/',
      'https://top10parentalcontrol.com/pt-home/',
      'https://top10parentalcontrol.com/pt-mspy-review/',
      'https://top10parentalcontrol.com/pt-norton-review/',
      'https://top10parentalcontrol.com/pt-eyezy-review/',
      'https://top10parentalcontrol.com/pt-parentaler-review/',
      'https://top10parentalcontrol.com/pt-cookie-policy/',
      'https://top10parentalcontrol.com/pt-terms-of-use/',
      'https://top10parentalcontrol.com/pt-privacy-policy/',
      'https://top10parentalcontrol.com/uk-home/',
      'https://top10parentalcontrol.com/uk-qustodio-review/',
      'https://top10parentalcontrol.com/uk-mspy-review/',
      'https://top10parentalcontrol.com/uk-norton-review/',
      'https://top10parentalcontrol.com/uk-eyezy-review/',
      'https://top10parentalcontrol.com/uk-parentaler-review/',
      'https://top10parentalcontrol.com/uk-cookie-policy/',
      'https://top10parentalcontrol.com/uk-terms-of-use/',
      'https://top10parentalcontrol.com/uk-privacy-policy/',
      'https://top10parentalcontrol.com/ca-home/',
      'https://top10parentalcontrol.com/ca-qustodio-review/',
      'https://top10parentalcontrol.com/ca-norton-review/',
      'https://top10parentalcontrol.com/ca-mspy-review/',
      'https://top10parentalcontrol.com/ca-eyezy-review/',
      'https://top10parentalcontrol.com/ca-parentaler-review/',
      'https://top10parentalcontrol.com/ca-cookie-policy/',
      'https://top10parentalcontrol.com/ca-terms-of-use/',
      'https://top10parentalcontrol.com/ca-privacy-policy/'
    ],
    useProxy: true, autoDetectGeo: true, defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10backgroundchecks.com/',
    pages: [
      'https://top10backgroundchecks.com/home/',
      'https://top10backgroundchecks.com/truthfinder-review/',
      'https://top10backgroundchecks.com/peoplelooker-review/',
      'https://top10backgroundchecks.com/intelius-review/',
      'https://top10backgroundchecks.com/beenverified-review/',
      'https://top10backgroundchecks.com/instantcheckmate-review/',
      'https://top10backgroundchecks.com/spokeo-review/',
      'https://top10backgroundchecks.com/ussearch-review/',
      'https://top10backgroundchecks.com/cookie-policy/',
      'https://top10backgroundchecks.com/terms-of-use/',
      'https://top10backgroundchecks.com/privacy-policy/',
      'https://top10backgroundchecks.com/partner-with-us/',
      'https://top10backgroundchecks.com/cheaters/',
      'https://top10backgroundchecks.com/criminals/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10taxrelief.com/',
    pages: [
      'https://top10taxrelief.com/home/',
      'https://top10taxrelief.com/alleviate-review/',
      'https://top10taxrelief.com/anthem-tax-review/',
      'https://top10taxrelief.com/tax-relief-advocates-review/',
      'https://top10taxrelief.com/tax-relief-helpers-review/',
      'https://top10taxrelief.com/tax-hardship-center-review/',
      'https://top10taxrelief.com/cookie-policy/',
      'https://top10taxrelief.com/terms-of-use/',
      'https://top10taxrelief.com/privacy-policy/',
      'https://top10taxrelief.com/partner-with-us/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10weightlosstreatments.com/',
    pages: [
      'https://top10weightlosstreatments.com/home/',
      'https://top10weightlosstreatments.com/medvi-review/',
      'https://top10weightlosstreatments.com/ro-review/',
      'https://top10weightlosstreatments.com/sprouthealth-review/',
      'https://top10weightlosstreatments.com/shed-review/',
      'https://top10weightlosstreatments.com/hers-review/',
      'https://top10weightlosstreatments.com/hims-review/',
      'https://top10weightlosstreatments.com/mystart-review/',
      'https://top10weightlosstreatments.com/skinnyrx-review/',
      'https://top10weightlosstreatments.com/partner-with-us/',
      'https://top10weightlosstreatments.com/cookie-policy/',
      'https://top10weightlosstreatments.com/terms-of-use/',
      'https://top10weightlosstreatments.com/privacy-policy/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10nadplus.com/',
    pages: [
      'https://top10nadplus.com/home/',
      'https://top10nadplus.com/cookie-policy/',
      'https://top10nadplus.com/terms-of-use/',
      'https://top10nadplus.com/privacy-policy/',
      'https://top10nadplus.com/partner-with-us/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10sermorelin.com/',
    pages: [
      'https://top10sermorelin.com/home/',
      'https://top10sermorelin.com/readyrx-review/',
      'https://top10sermorelin.com/maximus-review/',
      'https://top10sermorelin.com/bmimd-review/',
      'https://top10sermorelin.com/eden-review/',
      'https://top10sermorelin.com/cookie-policy/',
      'https://top10sermorelin.com/terms-of-use/',
      'https://top10sermorelin.com/privacy-policy/',
      'https://top10sermorelin.com/partner-with-us/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['US'], defaultProxyLocation: 'US'
  },
  {
    name: 'https://top10onlinesportsbetting.co.uk/',
    pages: [
      'https://top10onlinesportsbetting.co.uk/football-betting',
      'https://top10onlinesportsbetting.co.uk/cricket/',
      'https://top10onlinesportsbetting.co.uk/esports',
      'https://top10onlinesportsbetting.co.uk/formula',
      'https://top10onlinesportsbetting.co.uk/darts',
      'https://top10onlinesportsbetting.co.uk/greyhounds',
      'https://top10onlinesportsbetting.co.uk/best-bookies',
      'https://top10onlinesportsbetting.co.uk/horse-racing',
      'https://top10onlinesportsbetting.co.uk/ufc',
      'https://top10onlinesportsbetting.co.uk/a-zsports',
      'https://top10onlinesportsbetting.co.uk/about-us',
      'https://top10onlinesportsbetting.co.uk/american-football',
      'https://top10onlinesportsbetting.co.uk/b-sport',
      'https://top10onlinesportsbetting.co.uk/b-horse-racing',
      'https://top10onlinesportsbetting.co.uk/b-esports',
      'https://top10onlinesportsbetting.co.uk/b-football',
      'https://top10onlinesportsbetting.co.uk/b-online-casino',
      'https://top10onlinesportsbetting.co.uk/b-s-online-casino',
      'https://top10onlinesportsbetting.co.uk/online-casino',
      'https://top10onlinesportsbetting.co.uk/casino-page',
      'https://top10onlinesportsbetting.co.uk/cookie-policy',
      'https://top10onlinesportsbetting.co.uk/disclosure',
      'https://top10onlinesportsbetting.co.uk/privacy-policy',
      'https://top10onlinesportsbetting.co.uk/home',
      'https://top10onlinesportsbetting.co.uk/rugby',
      'https://top10onlinesportsbetting.co.uk/tennis',
      'https://top10onlinesportsbetting.co.uk/golf',
      'https://top10onlinesportsbetting.co.uk/premier-league',
      'https://top10onlinesportsbetting.co.uk/boxing/',
      'https://top10onlinesportsbetting.co.uk/in-play',
      'https://top10onlinesportsbetting.co.uk/motogp',
      'https://top10onlinesportsbetting.co.uk/political',
      'https://top10onlinesportsbetting.co.uk/snooker',
      'https://top10onlinesportsbetting.co.uk/nba',
      'https://top10onlinesportsbetting.co.uk/partner-with-us',
      'https://top10onlinesportsbetting.co.uk/terms-of-use'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['GB'], defaultProxyLocation: 'GB'
  },
  {
    name: 'https://top10onlinecasinoreviews.co.uk/',
    pages: [
      'https://top10onlinecasinoreviews.co.uk/home/',
      'https://top10onlinecasinoreviews.co.uk/apps/',
      'https://top10onlinecasinoreviews.co.uk/b-casino/',
      'https://top10onlinecasinoreviews.co.uk/b-slots/',
      'https://top10onlinecasinoreviews.co.uk/casino-gambling/',
      'https://top10onlinecasinoreviews.co.uk/b-no-verification/',
      'https://top10onlinecasinoreviews.co.uk/black-jack/',
      'https://top10onlinecasinoreviews.co.uk/bonus/',
      'https://top10onlinecasinoreviews.co.uk/casino2/',
      'https://top10onlinecasinoreviews.co.uk/live-casino/',
      'https://top10onlinecasinoreviews.co.uk/m-casino/',
      'https://top10onlinecasinoreviews.co.uk/new-casinos/',
      'https://top10onlinecasinoreviews.co.uk/offers/',
      'https://top10onlinecasinoreviews.co.uk/p-best-casino/',
      'https://top10onlinecasinoreviews.co.uk/p-d-casino/',
      'https://top10onlinecasinoreviews.co.uk/plinko/',
      'https://top10onlinecasinoreviews.co.uk/roulette/',
      'https://top10onlinecasinoreviews.co.uk/slingo/',
      'https://top10onlinecasinoreviews.co.uk/slots/',
      'https://top10onlinecasinoreviews.co.uk/trusted-legit/',
      'https://top10onlinecasinoreviews.co.uk/no-wager/',
      'https://top10onlinecasinoreviews.co.uk/b-no-wager/',
      'https://top10onlinecasinoreviews.co.uk/no-wager-slots/',
      'https://top10onlinecasinoreviews.co.uk/cookie-policy/',
      'https://top10onlinecasinoreviews.co.uk/games/',
      'https://top10onlinecasinoreviews.co.uk/partner-with-us/',
      'https://top10onlinecasinoreviews.co.uk/privacy-policy/',
      'https://top10onlinecasinoreviews.co.uk/real-money/',
      'https://top10onlinecasinoreviews.co.uk/sites/',
      'https://top10onlinecasinoreviews.co.uk/terms-of-use/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['GB'], defaultProxyLocation: 'GB'
  },
  {
    name: 'https://www.toponlinecasinoreview.co.uk/',
    pages: [
      'https://www.toponlinecasinoreview.co.uk/home/',
      'https://www.toponlinecasinoreview.co.uk/casinos/',
      'https://www.toponlinecasinoreview.co.uk/games/',
      'https://www.toponlinecasinoreview.co.uk/no-verification/',
      'https://www.toponlinecasinoreview.co.uk/no-wager/',
      'https://www.toponlinecasinoreview.co.uk/offers/',
      'https://www.toponlinecasinoreview.co.uk/plinko/',
      'https://www.toponlinecasinoreview.co.uk/slingo/',
      'https://www.toponlinecasinoreview.co.uk/slots/',
      'https://toponlinecasinoreview.co.uk/privacy-policy/',
      'https://toponlinecasinoreview.co.uk/terms-of-use/',
      'https://toponlinecasinoreview.co.uk/cookie-policy/'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['GB'], defaultProxyLocation: 'GB'
  },
  {
    name: 'https://top10onlinebingoreviews.co.uk/',
    pages: [
      'https://top10onlinebingoreviews.co.uk/b-bingo/',
      'https://top10onlinebingoreviews.co.uk/b-new-bingo/',
      'https://top10onlinebingoreviews.co.uk/bonus/',
      'https://top10onlinebingoreviews.co.uk/games/',
      'https://top10onlinebingoreviews.co.uk/new/',
      'https://top10onlinebingoreviews.co.uk/no-wager/',
      'https://top10onlinebingoreviews.co.uk/slots/',
      'https://top10onlinebingoreviews.co.uk/home',
      'https://top10onlinebingoreviews.co.uk/offers',
      'https://top10onlinebingoreviews.co.uk/b-bonus',
      'https://top10onlinebingoreviews.co.uk/b-games',
      'https://top10onlinebingoreviews.co.uk/b-slots',
      'https://top10onlinebingoreviews.co.uk/b-no-wager/',
      'https://top10onlinebingoreviews.co.uk/c-bonus',
      'https://top10onlinebingoreviews.co.uk/c-offers',
      'https://top10onlinebingoreviews.co.uk/c-p-best-casino',
      'https://top10onlinebingoreviews.co.uk/c-slots',
      'https://top10onlinebingoreviews.co.uk/cookie-policy',
      'https://top10onlinebingoreviews.co.uk/partner-with-us',
      'https://top10onlinebingoreviews.co.uk/privacy',
      'https://top10onlinebingoreviews.co.uk/s-betting-offers',
      'https://top10onlinebingoreviews.co.uk/s-free-bets',
      'https://top10onlinebingoreviews.co.uk/s-p-best-sport',
      'https://top10onlinebingoreviews.co.uk/s-slots',
      'https://top10onlinebingoreviews.co.uk/terms'
    ],
    useProxy: true, autoDetectGeo: false, proxyLocations: ['GB'], defaultProxyLocation: 'GB'
  }
];

// --- SLACK NOTIFICATION FUNCTION ---

async function sendSlackNotification(issue, domainName) {
  if (!token || !SLACK_CHANNEL_ID) { console.warn('⚠️ Slack not configured, skipping notification'); return false; }
  try {
    const priorityEmoji = { 'Critical': '🔴', 'High': '🟠', 'Medium': '🟡', 'Low': '🟢' };
    const emoji = priorityEmoji[issue.priority] || '⚪';
    const blocks = [
      { type: 'header', text: { type: 'plain_text', text: `${emoji} ${issue.priority} Priority Issue Detected`, emoji: true } },
      { type: 'section', fields: [
        { type: 'mrkdwn', text: `*Site:*\n${domainName}` },
        { type: 'mrkdwn', text: `*Issue Type:*\n${issue.type}` },
        { type: 'mrkdwn', text: `*Status:*\n${issue.status} - ${issue.statusText}` },
        { type: 'mrkdwn', text: `*Impact Score:*\n${issue.impactScore}/100` }
      ]},
      { type: 'section', fields: [
        { type: 'mrkdwn', text: `*Link Text:*\n${issue.linkText}` },
        { type: 'mrkdwn', text: `*Found on Page:*\n<${issue.pageUrl}|View Page>` }
      ]},
      { type: 'section', text: { type: 'mrkdwn', text: `*Broken URL:*\n\`${issue.linkUrl}\`` } },
      { type: 'section', text: { type: 'mrkdwn', text: `*Context:*\n${issue.context}` } }
    ];
    if (issue.proxyLocation || issue.geoRestricted) {
      const geoInfo = [];
      if (issue.proxyLocation)  geoInfo.push(`*Tested From:* ${issue.proxyLocation} proxy`);
      if (issue.geoRestricted)  geoInfo.push(`*⚠️ GEO-RESTRICTED:* Failed in ${issue.testedLocations.join(', ')}`);
      blocks.push({ type: 'section', text: { type: 'mrkdwn', text: geoInfo.join('\n') } });
    }
    blocks.push({ type: 'divider' });

    console.log(`📤 Sending Slack notification for issue #${issue.id}...`);
    const messageResponse = await slackClient.chat.postMessage({
      channel: SLACK_CHANNEL_ID,
      text: `${emoji} ${issue.priority} Priority Issue: ${issue.type} on ${domainName}`,
      blocks
    });
    if (!messageResponse.ok) { console.error(`❌ Slack message failed: ${messageResponse.error}`); return false; }

    if (issue.screenshot) {
      try {
        const buffer = Buffer.from(issue.screenshot, 'base64');
        const uploadResponse = await slackClient.files.uploadV2({
          channel_id: SLACK_CHANNEL_ID, file: buffer,
          filename: `error-${issue.id}-${Date.now()}.jpg`,
          title: `Screenshot: ${issue.linkText}`,
          initial_comment: `Screenshot showing the broken element (highlighted in red)`,
          thread_ts: messageResponse.ts
        });
        if (!uploadResponse.ok) console.warn(`⚠️ Screenshot upload failed: ${uploadResponse.error}`);
        else console.log(`   📸 Screenshot uploaded successfully`);
      } catch (uploadError) {
        console.warn(`⚠️ Screenshot upload error:`, uploadError.message);
      }
    }

    console.log(`✅ Slack notification sent for issue #${issue.id}`);
    await new Promise(resolve => setTimeout(resolve, 1000));
    return true;
  } catch (error) {
    console.error(`❌ Failed to send Slack notification:`, error.message);
    if (error.data) console.error('   Error details:', JSON.stringify(error.data, null, 2));
    return false;
  }
}

// --- CORE SCAN FUNCTION ---

async function performScan(pagesToScan, siteName, sendToSlack = false, siteConfig = {}) {
  const useProxy      = siteConfig.useProxy || false;
  const proxyLocations = siteConfig.proxyLocations || null;
  const autoDetectGeo = siteConfig.autoDetectGeo || false;

  console.log(`\n${'='.repeat(60)}`);
  console.log(`🚀 Starting scan for: ${siteName}`);
  console.log(`📄 Pages to scan: ${pagesToScan.length}`);
  console.log(`🌍 Proxy Geo-Rotation: ${useProxy && PROXY_ENABLED ? 'ENABLED ✅' : 'DISABLED'}`);
  if (useProxy && PROXY_ENABLED) {
    if (autoDetectGeo) {
      console.log(`🤖 Auto-Detect Geo: ENABLED`);
      console.log(`📍 Default Proxy: ${siteConfig.defaultProxyLocation || 'US'}`);
    } else if (proxyLocations && proxyLocations.length > 0) {
      console.log(`📍 Proxy Locations: ${proxyLocations.join(', ')}`);
    }
  }
  console.log(`${'='.repeat(60)}\n`);

  try {
    const { pages, links } = await crawlSpecificPages(pagesToScan, siteConfig);
    console.log(`\n✅ Scanned ${pages.length} pages, found ${links.length} links\n`);

    const linkAppearances = new Map();
    links.forEach(link => linkAppearances.set(link.url, (linkAppearances.get(link.url) || 0) + 1));

    const uniqueLinks = [];
    const seenUrls = new Set();
    links.forEach(link => { if (!seenUrls.has(link.url)) { seenUrls.add(link.url); uniqueLinks.push(link); } });

    console.log(`🔍 Checking ${uniqueLinks.length} unique links (concurrency: 5)...\n`);

    const CONCURRENCY = 5;
    const limit = createConcurrencyLimiter(CONCURRENCY);
    let processed = 0;

    const issuePromises = uniqueLinks.map(link => limit(async () => {
      const idx = ++processed;
      console.log(`[${idx}/${uniqueLinks.length}] ${link.url.substring(0, 80)}...`);

      let linkProxyLocation = null;
      if (useProxy && PROXY_ENABLED) {
        if (autoDetectGeo && link.pageProxyLocation) linkProxyLocation = [link.pageProxyLocation];
        else if (proxyLocations && proxyLocations.length > 0) linkProxyLocation = proxyLocations;
      }

      const statusInfo = await checkLinkStatus(link.url, useProxy, linkProxyLocation);

      // Fast exits — no verification needed for these
      if (statusInfo.skip)                                  { console.log(`   ℹ️ Skipped\n`); return null; }
      if (statusInfo.status === 403 && statusInfo.isAffiliate) return null;
      if (statusInfo.status === 304)                           return null;
      if (statusInfo.proxyIssue)                            { console.log(`   ✅ Proxy SSL issue but link works\n`); return null; }

      const looksLikeIssue = (
        !isSuccessStatus(statusInfo.status) &&
        !statusInfo.treatAsWorking &&
        !statusInfo.proxyIssue &&
        statusInfo.status !== 200
      );
      if (!looksLikeIssue) return null;

      // --- VERIFY: retry 2 more times before treating as a confirmed error ---
      console.log(`   ⚠️ Possible issue (${statusInfo.status}) — verifying with 2 more attempts...`);
      const verifiedStatus = await verifyLinkError(statusInfo, link.url, useProxy, linkProxyLocation);

      const isActualIssue = (
        !isSuccessStatus(verifiedStatus.status) &&
        !verifiedStatus.treatAsWorking &&
        !verifiedStatus.proxyIssue &&
        verifiedStatus.status !== 200
      );

      if (!isActualIssue) { console.log(`   ✅ False alarm — link recovered during verification\n`); return null; }

      const appearanceCount = linkAppearances.get(link.url);
      let friendlyMessage = `Issue: ${verifiedStatus.status}`;
      if (verifiedStatus.proxyLocation) friendlyMessage += ` (tested from ${verifiedStatus.proxyLocation} proxy)`;
      if (verifiedStatus.geoRestricted) friendlyMessage += ` - GEO-RESTRICTED (failed in ${verifiedStatus.testedLocations.join(', ')})`;

      let screenshot = null;
      const shouldTakeScreenshot = (
        (verifiedStatus.status === 404 || verifiedStatus.status === 500 ||
         verifiedStatus.status === 'ERROR' || verifiedStatus.status === 'DNS_ERROR' ||
         verifiedStatus.status === 'TIMEOUT') && !verifiedStatus.treatAsWorking
      );
      if (shouldTakeScreenshot) screenshot = await takeErrorScreenshot(link.pageUrl, link.selector, link.pageProxyLocation);

      return {
        pageUrl:          link.pageUrl,
        linkText:         link.text,
        linkUrl:          link.url,
        pageProxyLocation: link.pageProxyLocation,   // preserved for follow-up checks
        status:           verifiedStatus.status,
        statusText:       verifiedStatus.statusText,
        responseTime:     verifiedStatus.responseTime,
        redirectCount:    verifiedStatus.redirectCount || 0,
        finalUrl:         verifiedStatus.finalUrl || link.url,
        priority:         determinePriorityFromStatus(verifiedStatus.status),
        type:             getIssueType(verifiedStatus.status),
        context:          link.context,
        aiAnalysis:       friendlyMessage,
        impactScore:      calculateImpactScore({ ...link, status: verifiedStatus.status }, appearanceCount),
        appearancesCount: appearanceCount,
        linkType:         link.type,
        screenshot,
        proxyLocation:    verifiedStatus.proxyLocation,
        geoRestricted:    verifiedStatus.geoRestricted || false,
        testedLocations:  verifiedStatus.testedLocations || []
      };
    }));

    const rawResults = await Promise.all(issuePromises);
    const results = rawResults
      .filter(Boolean)
      .sort((a, b) => ({ 'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3 }[a.priority] - { 'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3 }[b.priority]))
      .map((issue, i) => ({ ...issue, id: i + 1 }));

    // Send per-issue Slack notifications (Critical/High only)
    if (sendToSlack) {
      for (const issue of results) {
        if (issue.priority === 'Critical' || issue.priority === 'High') {
          await sendSlackNotification(issue, siteName);
        }
      }

      // Schedule follow-up rechecks at +30, +60, +90 minutes for all confirmed errors
      const criticalHighIssues = results.filter(r => r.priority === 'Critical' || r.priority === 'High');
      if (results.length > 0) {
        const scanId = `${Date.now()}-${siteName}`;
        scheduleErrorFollowUps(scanId, siteName, criticalHighIssues, siteConfig);
      }
    }

    const totalLinks  = links.length;
    const issueLinks  = results.length;
    const healthScore = Math.round(((totalLinks - issueLinks) / totalLinks) * 100);

    const stats = {
      totalPages:    pages.length,
      totalLinks,
      brokenLinks:   issueLinks,
      criticalIssues: results.filter(r => r.priority === 'Critical').length,
      highIssues:    results.filter(r => r.priority === 'High').length,
      avgImpactScore: results.length > 0
        ? Math.round(results.reduce((acc, r) => acc + r.impactScore, 0) / results.length)
        : 0
    };

    return { healthScore, stats, results, pages };

  } catch (error) {
    console.error('❌ Scan error:', error);
    throw error;
  } finally {
    await closeAllBrowsers();
  }
}

// --- SCHEDULED SCAN (every 12 hours: midnight and noon) ---

async function runScheduledScan() {
  console.log('\n🔔 Starting scheduled scan...');
  console.log(`📢 Slack Channel: ${SLACK_CHANNEL_ID}`);
  console.log(`🔑 Slack Token: ${token ? 'Configured ✅' : 'Missing ❌'}`);
  console.log(`🌍 Proxy Multi-Geo: ${PROXY_ENABLED ? 'ENABLED ✅' : 'DISABLED ❌'}\n`);

  if (PROXY_ENABLED) {
    const proxyWorking = await verifyProxy('US');
    if (!proxyWorking) console.error('⚠️  WARNING: Proxy verification failed. Continuing anyway...');
  }

  const healthyDomains = [];  // collected across all sites, sent in one message at the end

  for (const site of DOMAINS_TO_SCAN) {
    try {
      console.log(`\n📊 Scanning ${site.name}...`);
      const siteConfig = {
        useProxy: site.useProxy || false,
        proxyLocations: site.proxyLocations || null,
        autoDetectGeo: site.autoDetectGeo || false,
        defaultProxyLocation: site.defaultProxyLocation || 'US'
      };

      const scanResult = await performScan(site.pages, site.name, true, siteConfig);

      if (scanResult.stats.criticalIssues === 0 && scanResult.stats.highIssues === 0) {
        // No issues — collect for the consolidated healthy-domains message
        healthyDomains.push({ name: site.name, totalLinks: scanResult.stats.totalLinks, totalPages: scanResult.stats.totalPages });
        console.log(`✅ No issues found for ${site.name} — will include in healthy domains summary`);
      } else if (token && SLACK_CHANNEL_ID) {
        // Has issues — send the per-site summary immediately (existing behaviour)
        console.log(`\n📤 Sending summary report to Slack...`);
        try {
          const summaryResponse = await slackClient.chat.postMessage({
            channel: SLACK_CHANNEL_ID,
            text: `Scan Complete: ${site.name} - Health Score: ${scanResult.healthScore}%`,
            blocks: [
              { type: 'header', text: { type: 'plain_text', text: `📊 Scan Report: ${site.name}`, emoji: true } },
              { type: 'section', fields: [
                { type: 'mrkdwn', text: `*Health Score:*\n${scanResult.healthScore}%` },
                { type: 'mrkdwn', text: `*Total Links Checked:*\n${scanResult.stats.totalLinks}` },
                { type: 'mrkdwn', text: `*Critical Issues:*\n${scanResult.stats.criticalIssues}` },
                { type: 'mrkdwn', text: `*High Priority Issues:*\n${scanResult.stats.highIssues}` }
              ]},
              { type: 'context', elements: [{ type: 'mrkdwn', text: `Scanned ${scanResult.stats.totalPages} pages | Found ${scanResult.stats.brokenLinks} total issues${scanResult.stats.brokenLinks > 0 ? ' | ⏲️ Follow-up rechecks scheduled at +30, +60, +90 min' : ''}` }] }
            ]
          });
          if (summaryResponse.ok) console.log(`✅ Summary report sent to Slack`);
          else console.error(`❌ Summary report failed: ${summaryResponse.error}`);
        } catch (summaryError) {
          console.error(`❌ Error sending summary to Slack:`, summaryError.message);
        }
      }

      console.log(`✅ Scan completed for ${site.name}`);
    } catch (error) {
      console.error(`❌ Error scanning ${site.name}:`, error.message);
    }
  }

  // Send one consolidated message for all healthy domains
  if (healthyDomains.length > 0 && token && SLACK_CHANNEL_ID) {
    console.log(`\n📤 Sending consolidated healthy-domains report (${healthyDomains.length} site(s))...`);
    try {
      const domainLines = healthyDomains.map(d =>
        `> ✅ *${d.name}*  —  ${d.totalPages} pages | ${d.totalLinks} links checked`
      ).join('\n');

      await slackClient.chat.postMessage({
        channel: SLACK_CHANNEL_ID,
        text: `✅ ${healthyDomains.length} domain(s) fully healthy — no issues found`,
        blocks: [
          { type: 'header', text: { type: 'plain_text', text: `✅ Healthy Domains (${healthyDomains.length})`, emoji: true } },
          { type: 'section', text: { type: 'mrkdwn', text: `The following domain(s) completed their scan with *no broken links or errors*:\n\n${domainLines}` } },
          { type: 'context', elements: [{ type: 'mrkdwn', text: `Part of the scheduled scan completed at ${new Date().toUTCString()}` }] }
        ]
      });
      console.log(`✅ Healthy domains summary sent to Slack`);
    } catch (err) {
      console.error(`❌ Failed to send healthy domains summary:`, err.message);
    }
  }

  console.log('\n✅ All scheduled scans completed\n');
}


cron.schedule('0 */8 * * *', () => {
  console.log('⏰ Triggering scheduled scan (every 6 hours)');
  runScheduledScan();
});

console.log('⏰ Scan scheduled every 6 hours');

// --- API ENDPOINTS ---

app.post('/api/scan', async (req, res) => {
  const { domain, pages, useProxy, proxyLocations, autoDetectGeo, defaultProxyLocation } = req.body;
  let pagesToScan = [];
  if (pages && Array.isArray(pages)) pagesToScan = pages;
  else if (domain && isValidUrl(domain)) pagesToScan = [domain];
  else return res.status(400).json({ error: 'Invalid domain URL or pages array' });

  try {
    const siteConfig = {
      useProxy: useProxy === true && PROXY_ENABLED,
      proxyLocations: proxyLocations && Array.isArray(proxyLocations) ? proxyLocations : null,
      autoDetectGeo: autoDetectGeo === true,
      defaultProxyLocation: defaultProxyLocation || 'US'
    };
    const result = await performScan(pagesToScan, domain || 'Manual Scan', false, siteConfig);
    res.json(result);
  } catch (error) {
    console.error('❌ Scan error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/proxy/status', async (req, res) => {
  try {
    res.json({ enabled: PROXY_ENABLED, currentLocation: currentProxyLocation, availableLocations: Object.keys(PROXY_CONFIG).filter(k => k !== 'DEFAULT'), mapping: GEO_PROXY_MAPPING });
  } catch (error) { res.status(500).json({ error: error.message }); }
});

app.post('/api/proxy/verify', async (req, res) => {
  const loc = req.body.location || 'US';
  try {
    const isWorking = await verifyProxy(loc);
    res.json({ success: isWorking, location: loc, message: isWorking ? `Proxy working for ${loc}` : `Proxy failed for ${loc}` });
  } catch (error) { res.status(500).json({ error: error.message }); }
});

app.post('/api/proxy/test', async (req, res) => {
  const testLocation = req.body.location || 'US';
  const testUrl = req.body.url || 'https://api.ipify.org?format=json';
  try {
    console.log(`\n🧪 Testing proxy ${testLocation} with Puppeteer...`);
    const result = await checkLinkWithBrowser(testUrl, testLocation);
    res.json({
      success: result.status === 200 || isSuccessStatus(result.status),
      location: testLocation, status: result.status, statusText: result.statusText,
      proxyLocation: result.proxyLocation, responseTime: result.responseTime,
      message: result.status === 200 ? `Proxy working correctly` : `Proxy returned status ${result.status}`
    });
  } catch (error) { res.status(500).json({ success: false, error: error.message, location: testLocation }); }
});

app.post('/api/scan/scheduled', async (req, res) => {
  try {
    res.json({ message: 'Scheduled scan started in background', sites: DOMAINS_TO_SCAN.length, timestamp: new Date().toISOString() });
    runScheduledScan().catch(err => console.error('Background scan error:', err));
  } catch (error) { res.status(500).json({ error: error.message }); }
});

app.get('/api/scan/scheduled', (req, res) => {
  res.json({ message: 'Use POST method to trigger scan', sites: DOMAINS_TO_SCAN.length, configured: DOMAINS_TO_SCAN.map(s => s.name) });
});

app.get('/api/domains', (req, res) => {
  res.json({
    geoMapping: GEO_PROXY_MAPPING,
    proxyConfig: Object.keys(PROXY_CONFIG).filter(k => k !== 'DEFAULT'),
    sites: DOMAINS_TO_SCAN.map(site => ({
      name: site.name, pageCount: Array.isArray(site.pages) ? site.pages.length : 0,
      pages: site.pages, useProxy: site.useProxy || false, proxyLocations: site.proxyLocations || [],
      autoDetectGeo: site.autoDetectGeo || false, defaultProxyLocation: site.defaultProxyLocation || 'US'
    }))
  });
});

app.get('/api/health', async (req, res) => {
  const totalPages = DOMAINS_TO_SCAN.reduce((sum, site) => sum + (Array.isArray(site.pages) ? site.pages.length : 0), 0);
  res.json({
    status: 'ok', timestamp: new Date().toISOString(),
    slackConfigured: !!(token && SLACK_CHANNEL_ID),
    sitesConfigured: DOMAINS_TO_SCAN.length, totalPagesConfigured: totalPages,
    activeFollowUpTrackers: errorTracker.size,
    proxy: { enabled: PROXY_ENABLED, currentLocation: currentProxyLocation, availableLocations: Object.keys(PROXY_CONFIG).filter(k => k !== 'DEFAULT'), geoMapping: GEO_PROXY_MAPPING }
  });
});

app.listen(PORT, async () => {
  const totalPages = DOMAINS_TO_SCAN.reduce((sum, site) => sum + (Array.isArray(site.pages) ? site.pages.length : 0), 0);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`🚀 Link Checker Backend running on http://localhost:${PORT}`);
  console.log(`📢 Slack Integration: ${token && SLACK_CHANNEL_ID ? 'ENABLED ✅' : 'DISABLED ❌'}`);
  console.log(`🌍 Proxy Multi-Geo Scanning: ${PROXY_ENABLED ? 'ENABLED ✅' : 'DISABLED ❌'}`);
  if (PROXY_ENABLED) {
    console.log(`📍 Proxy Locations: ${Object.keys(PROXY_CONFIG).filter(k => k !== 'DEFAULT').join(', ')}`);
    console.log(`🗺️  Geo Mapping: ${Object.keys(GEO_PROXY_MAPPING).length} country codes configured`);
  }
  console.log(`📊 Configured Sites: ${DOMAINS_TO_SCAN.length}`);
  console.log(`📄 Total Pages to Scan: ${totalPages}`);
  console.log(`⏰ Scan Schedule: Every 6 hours (00:00 and 12:00)`);
  console.log(`${'='.repeat(60)}`);
  if (PROXY_ENABLED) { console.log(''); await verifyProxy('US'); console.log(''); }
});
