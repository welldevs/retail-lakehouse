// Screenshots of EVERY tab of both Streamlit panels, each one to the bottom — or nothing.
//
//   make dashboard-shots        # panels running on 8501/8502; see the Makefile target
//
// WHY IT REFUSES INSTEAD OF DOING ITS BEST. Streamlit scrolls inside its own container, so a
// fixed viewport with `fullPage` captures only the first screen. On 2026-10-30 a review
// trusted shots cut at 1000 px and missed ten defects below the fold (CR-018); one of those
// shots, from a model two regenerations old, was still published in docs/screens. Here
// the height of each tab is MEASURED, the viewport grows to fit it, and a tab that still
// does not fit fails the run.
//
// It writes docs/screens/dashboard/MANIFEST.json with the seal (`capture_id`) the shots
// show and `reviewed_at: null`. test_docs.py stays red until the seal matches docs/FREEZE.md
// and someone has READ every PNG and run `make dashboard-shots-approve`.
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const [outdir, captureId] = process.argv.slice(2);
const PANELS = [
  { name: 'operations', url: process.env.OPERATIONS_URL || 'http://localhost:8501' },
  { name: 'prices', url: process.env.PRICES_URL || 'http://localhost:8502' },
];
const WIDTH = 1600;

const slug = (text) => text.trim().toLowerCase().normalize('NFD')
  .replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');

async function settle(page) {
  await page.waitForTimeout(1500);
  await page.waitForFunction(
    () => !document.querySelector('[data-testid="stStatusWidget"]'), null, { timeout: 180000 });
  await page.waitForTimeout(1500);
}

// Streamlit's own scroll container. NOT the tallest element on the page: a dataframe
// scrolls internally and reports every row as scrollHeight, which made 7,000 px shots of
// mostly blank page (first run, 2026-10-30).
const mainBox = (page) => page.evaluate(() => {
  const main = document.querySelector('[data-testid="stMain"]');
  return { content: main.scrollHeight, visible: main.clientHeight };
});

// A table wider than its frame scrolls sideways, and the shot shows it cut mid-column
// (CR-018: the stage table lost its Orders column in a 3:2 split). Returns the first header
// of every visible table that would hide columns.
const sidewaysTables = (page) => page.evaluate(() =>
  [...document.querySelectorAll('[data-testid="stDataFrame"]')]
    .filter((frame) => frame.getBoundingClientRect().height > 0)
    .map((frame) => ({ frame, scroller: frame.querySelector('.dvn-scroller') }))
    .filter(({ scroller }) => scroller && scroller.scrollWidth > scroller.clientWidth + 2)
    .map(({ frame, scroller }) =>
      `${(frame.innerText || '').split('\n')[0].trim() || 'table'} `
      + `(${scroller.scrollWidth}px in ${scroller.clientWidth}px)`));

(async () => {
  if (!outdir || !/^[0-9a-f]{64}$/.test(captureId || '')) {
    console.error('usage: node dashboard_shots.js <outdir> <capture_id (64 hex)>');
    process.exit(2);
  }
  fs.mkdirSync(outdir, { recursive: true });
  for (const f of fs.readdirSync(outdir)) if (f.endsWith('.png')) fs.unlinkSync(path.join(outdir, f));

  const browser = await chromium.launch();
  const files = [];
  const problems = [];
  for (const panel of PANELS) {
    const page = await browser.newPage({ viewport: { width: WIDTH, height: 1000 } });
    await page.goto(panel.url, { waitUntil: 'networkidle' });
    await page.waitForSelector('[data-testid="stTab"]', { timeout: 180000 });
    await settle(page);

    if (panel.name === 'prices') {
      // The Product tab is empty until someone searches: a published shot of an empty tab
      // demonstrates nothing. Aguacate is among the biggest observed drops.
      const search = page.getByPlaceholder('e.g. coca cola');
      await search.fill(process.env.SHOTS_PRODUCT || 'Aguacate');
      await search.press('Enter');
      await settle(page);
    }

    if (panel.name === 'operations') {
      // The README cover: the first screen, as a visitor lands on it.
      const file = 'cover.png';
      await page.screenshot({ path: path.join(outdir, file) });
      files.push({ file, panel: panel.name, tab: 'cover', height: 1000, content_height: 1000 });
    }

    const tabs = await page.$$('[data-testid="stTab"]');
    for (let i = 0; i < tabs.length; i++) {
      const tab = (await tabs[i].innerText()).trim();
      await tabs[i].click();
      await settle(page);
      const needed = (await mainBox(page)).content;
      const height = needed + 40;
      await page.setViewportSize({ width: WIDTH, height });
      await settle(page);
      const box = await mainBox(page);
      const after = box.content;
      if (box.content > box.visible + 1) {
        console.error(`TRUNCATED: ${panel.name} / ${tab}: ${box.content}px of content, `
          + `${box.visible}px visible`);
        process.exit(1);
      }
      for (const t of await sidewaysTables(page)) problems.push(`${panel.name} / ${tab}: ${t}`);
      const file = `${panel.name}-${String(i + 1).padStart(2, '0')}-${slug(tab)}.png`;
      await page.screenshot({ path: path.join(outdir, file) });
      files.push({ file, panel: panel.name, tab, height, content_height: after });
      console.log(`saved ${file}  (${after}px of content, viewport ${height}px)`);
      await page.setViewportSize({ width: WIDTH, height: 1000 });
    }
    await page.close();
  }
  await browser.close();
  if (problems.length) {
    console.error(`\nCOLUMNS HIDDEN — ${problems.length} table(s) scroll sideways:`);
    for (const p of problems) console.error(`  - ${p}`);
    console.error('Fix the layout (full width, fewer columns) and shoot again. No manifest written.');
    process.exit(1);
  }

  const manifest = {
    capture_id: captureId,
    taken_at: new Date().toISOString(),
    reviewed_at: null,
    reviewed_note: null,
    files,
  };
  fs.writeFileSync(path.join(outdir, 'MANIFEST.json'), JSON.stringify(manifest, null, 2) + '\n');
  console.log(`\n${files.length} screenshots. NOT reviewed: open every PNG, read it like a business`);
  console.log('user, and only then run `make dashboard-shots-approve NOTE="..."`.');
})().catch((e) => { console.error(e); process.exit(1); });
