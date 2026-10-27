// 本机文档验收：复制单HTML到临时目录离线打开，仅检查文档呈现与交互。
// 不连接服务器、不运行推理/CPU候选测试、不修改源码；运行时只会生成文档检查回执和HTML截图。
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {spawn} from 'node:child_process';
import {fileURLToPath, pathToFileURL} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const report = path.join(here, 'PP分段就绪调度_源码依据与修改设计.html');
const receiptPath = path.join(here, '文档检查.json');
const workspaceRoot = path.resolve(here, '../../../..');
const screenshotDir = path.join(workspaceRoot, '.codex', 'qa', 'stage-report');
const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'ks-stage-ready-doc-'));
const copied = path.join(temp, '独立复制后的说明.html');
let profile;
let browser;
let ws;
let browserPort;
let id = 0;
const pending = new Map();
const runtimeErrors = [];
const consoleErrors = [];
const externalRequests = [];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const isExternalUrl = value => typeof value === 'string' && /^(?:https?:)?\/\//i.test(value);
const errorText = value => {
  if (value instanceof Error) return value.stack || value.message;
  return String(value);
};

function send(method, params = {}) {
  return new Promise((resolve, reject) => {
    const requestId = ++id;
    const timer = setTimeout(() => {
      pending.delete(requestId);
      reject(new Error(`Browser timeout: ${method}`));
    }, 15000);
    pending.set(requestId, {resolve, reject, timer});
    ws.send(JSON.stringify({id: requestId, method, params}));
  });
}

async function evaluate(expression) {
  const response = await send('Runtime.evaluate', {
    expression,
    returnByValue: true,
    awaitPromise: true,
  });
  if (response.exceptionDetails) {
    throw new Error(JSON.stringify(response.exceptionDetails));
  }
  return response.result?.value;
}

async function waitUntilReady() {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const state = await evaluate('document.readyState');
    if (state === 'interactive' || state === 'complete') {
      // 给内联脚本一次事件循环和布局机会，不依赖某个生成器的 data-ready 约定。
      await sleep(100);
      return;
    }
    await sleep(50);
  }
  throw new Error('Document did not reach interactive state');
}

async function inspectDocument() {
  return evaluate(`(()=>{
    const sectionIds = ['summary', 'flow', 'changes', 'code', 'validation', 'gaps', 'reproduce'];
    const resourceSelector = 'base[href],link[href],script[src],img[src],iframe[src],frame[src],audio[src],video[src],source[src],object[data],embed[src]';
    const resourceRefs = [...document.querySelectorAll(resourceSelector)]
      .map(node => ({tag: node.tagName.toLowerCase(), value: node.getAttribute('href') || node.getAttribute('src') || node.getAttribute('data')}))
      .filter(item => /^(?:https?:)?\\/\\//i.test(item.value || ''));
    const styleText = [...document.querySelectorAll('style,[style]')]
      .map(node => node.textContent || node.getAttribute('style') || '').join('\\n');
    const cssExternalRefs = styleText.match(/url\\(\\s*['"]?(?:https?:)?\\/\\//gi) || [];
    const bodyText = document.body?.innerText || '';
    const placeholderMatches = bodyText.match(/\\uFFFD|ï¿½|__([A-Z][A-Z0-9_]{2,})__|\\{\\{[^{}]+\\}\\}/g) || [];
    const syntaxTokens = [...document.querySelectorAll('code span[class]')].filter(node => {
      const classes = String(node.className || '');
      return /(?:\\btoken\\b|\\btok[-_]\\w+|(?:^|\\s)(?:k|kn|kd|kc|kp|kr|s|s1|s2|sa|sd|nf|nc|o|ow|nb|bp|mi|mf|c|c1|cm)(?:\\s|$))/i.test(classes);
    });
    const anchors = [...document.querySelectorAll('a[href^="#"]')].map(anchor => {
      const href = anchor.getAttribute('href') || '';
      const targetId = href.slice(1);
      return {href, id: targetId, exists: Boolean(targetId && document.getElementById(targetId))};
    });
    const diffContainers = [...document.querySelectorAll('.diff-grid,.diff-table')];
    const directPairCount = node => {
      const children = [...node.children];
      if (node.tagName === 'TR') return children.filter(child => /^(TD|TH)$/i.test(child.tagName)).length;
      const marked = children.filter(child => /(?:^|[-_])(old|new|left|right|side|cell)(?:$|[-_])/i.test(String(child.className || '')) || child.hasAttribute('data-side'));
      return marked.length || node.querySelectorAll(':scope > td,:scope > th,:scope > [data-side]').length;
    };
    const countDiffGroups = container => {
      const explicit = [...container.querySelectorAll('.diff-group,.diff-row,[data-diff-group]')]
        .filter(node => directPairCount(node) >= 2);
      if (explicit.length) return explicit.length;
      const rows = [...container.querySelectorAll('tr')].filter(row => directPairCount(row) >= 2);
      if (rows.length) return rows.length;
      const direct = [...container.children];
      const directGroups = direct.filter(node => directPairCount(node) >= 2);
      if (directGroups.length >= 5) return directGroups.length;
      if (direct.length === 2) {
        const childCounts = direct.map(node => node.children.length);
        if (Math.min(...childCounts) >= 5) return Math.min(...childCounts);
      }
      return directGroups.length;
    };
    const diffGroupCounts = diffContainers.map(container => ({
      tag: container.tagName.toLowerCase(),
      className: String(container.className || ''),
      groups: countDiffGroups(container),
    }));
    const diffDetails = [...document.querySelectorAll('details')]
      .filter(node => node.querySelector('.diff-grid,.diff-table'));
    const originalDiffOpen = diffDetails.map(node => node.open);
    diffDetails.forEach(node => { node.open = true; });
    const diffCells = [...document.querySelectorAll('.diff-grid td,.diff-table td')];
    const measuredCells = diffCells.map((node, index) => ({
      index,
      scrollWidth: node.scrollWidth,
      clientWidth: node.clientWidth,
      overflow: node.scrollWidth - node.clientWidth,
    }));
    const longestDiffTd = measuredCells.reduce((longest, current) => (
      current.scrollWidth > longest.scrollWidth ? current : longest
    ), {index: -1, scrollWidth: 0, clientWidth: 0, overflow: 0});
    const diffScrollContainers = [...document.querySelectorAll('.diff-scroll,.diff-grid')].map(node => {
      const before = node.scrollLeft;
      const canScroll = node.scrollWidth > node.clientWidth + 2
        && ['auto', 'scroll'].includes(getComputedStyle(node).overflowX);
      node.scrollLeft = Math.min(before + 32, Math.max(before, node.scrollWidth - node.clientWidth));
      const moved = node.scrollLeft > before;
      node.scrollLeft = before;
      return {
        className: String(node.className || ''),
        clientWidth: node.clientWidth,
        scrollWidth: node.scrollWidth,
        overflowX: getComputedStyle(node).overflowX,
        canScroll,
        moved,
      };
    });
    diffDetails.forEach((node, index) => { node.open = originalDiffOpen[index]; });
    return {
      title: document.title,
      sections: {
        required: sectionIds,
        found: sectionIds.filter(id => document.querySelector('section#' + id)),
        missing: sectionIds.filter(id => !document.querySelector('section#' + id)),
      },
      resourceRefs,
      cssExternalRefs,
      codeBlocks: document.querySelectorAll('pre,code').length,
      syntaxTokenCount: syntaxTokens.length,
      diffContainers: diffContainers.length,
      diffGroupCounts,
      diffGroups: Math.max(0, ...diffGroupCounts.map(item => item.groups)),
      diffLayout: {
        longestDiffTd,
        diffScrollContainers,
        scrollContainerPass: diffScrollContainers.some(item => item.canScroll && item.moved),
        pass: longestDiffTd.overflow <= 2
          && diffScrollContainers.some(item => item.canScroll && item.moved),
      },
      detailsCount: document.querySelectorAll('details').length,
      anchors,
      brokenAnchors: anchors.filter(item => !item.exists),
      placeholderMatches,
      desktop: {
        innerWidth: window.innerWidth,
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: Math.max(document.documentElement.scrollWidth, document.body?.scrollWidth || 0),
        pageOverflow: Math.max(document.documentElement.scrollWidth, document.body?.scrollWidth || 0) > window.innerWidth + 1,
      },
    };
  })()`);
}

async function inspectModeToggle() {
  return evaluate(`(async()=>{
    const buttons = [...document.querySelectorAll('button[data-mode],[role="button"][data-mode]')];
    const oldButtons = buttons.filter(button => button.dataset.mode === 'old');
    const newButtons = buttons.filter(button => button.dataset.mode === 'new');
    const timeline = document.getElementById('timeline');
    const settle = () => new Promise(resolve => setTimeout(resolve, 0));
    const state = () => timeline ? {
      outerHTML: timeline.outerHTML,
      mode: timeline.dataset.mode || '',
      className: String(timeline.className || ''),
      hidden: Boolean(timeline.hidden),
    } : null;
    const pressedCount = () => buttons.filter(button => button.getAttribute('aria-pressed') === 'true').length;
    const activate = async button => {
      if (!button) return {pressedCount: 0, state: null};
      button.click();
      await settle();
      return {pressedCount: pressedCount(), state: state()};
    };
    const oldState = await activate(oldButtons[0]);
    const newState = await activate(newButtons[0]);
    return {
      buttonCount: buttons.length,
      oldButtonCount: oldButtons.length,
      newButtonCount: newButtons.length,
      timelinePresent: Boolean(timeline),
      oldPressedCount: oldState.pressedCount,
      newPressedCount: newState.pressedCount,
      oldMode: oldState.state?.mode || '',
      newMode: newState.state?.mode || '',
      timelineChanged: Boolean(oldState.state && newState.state && oldState.state.outerHTML !== newState.state.outerHTML),
      pass: oldButtons.length === 1 && newButtons.length === 1 && Boolean(timeline)
        && oldState.pressedCount === 1 && newState.pressedCount === 1
        && Boolean(oldState.state && newState.state && oldState.state.outerHTML !== newState.state.outerHTML),
    };
  })()`);
}

async function inspectFoldables() {
  return evaluate(`(async()=>{
    const details = [...document.querySelectorAll('details')];
    const diffDetails = details.filter(node => node.querySelector('.diff-grid,.diff-table'));
    const target = diffDetails[0] || details[0];
    let toggled = false;
    let restored = true;
    if (target?.querySelector('summary')) {
      const before = target.open;
      target.querySelector('summary').click();
      await new Promise(resolve => setTimeout(resolve, 0));
      toggled = target.open !== before;
      target.open = before;
      restored = target.open === before;
    }
    const original = details.map(node => node.open);
    details.forEach(node => { node.open = true; });
    const allOpen = details.every(node => node.open);
    details.forEach((node, index) => { node.open = original[index]; });
    return {
      count: details.length,
      diffDetails: diffDetails.length,
      toggled,
      restored,
      allOpen,
      pass: details.length > 0 && toggled && restored && allOpen,
    };
  })()`);
}

async function inspectViewport() {
  return evaluate(`(()=>{
    const root = document.documentElement;
    const body = document.body;
    const scrollWidth = Math.max(root.scrollWidth, body?.scrollWidth || 0);
    const scrollHeight = Math.max(root.scrollHeight, body?.scrollHeight || 0);
    const wrappers = [...document.querySelectorAll('.table-scroll,.diff-scroll,.code-scroll')];
    return {
      innerWidth: window.innerWidth,
      clientWidth: root.clientWidth,
      scrollWidth,
      scrollHeight,
      pageOverflow: scrollWidth > window.innerWidth + 1,
      scrollWrappers: wrappers.length,
      scrollWrappersUsable: wrappers.every(node => ['auto', 'scroll'].includes(getComputedStyle(node).overflowX)),
    };
  })()`);
}

async function captureScreenshot(filename, fullPage = true, clip = null) {
  await fs.mkdir(screenshotDir, {recursive: true});
  const params = {
    format: 'png',
    fromSurface: true,
    captureBeyondViewport: fullPage || Boolean(clip),
  };
  if (clip) params.clip = {...clip, scale: 1};
  const result = await send('Page.captureScreenshot', params);
  const target = path.join(screenshotDir, filename);
  await fs.writeFile(target, Buffer.from(result.data, 'base64'));
  return {path: target, mode: fullPage ? 'full-page' : (clip ? 'viewport-clip' : 'first-screen')};
}

async function prepareDiffScreenshot() {
  return evaluate(`(()=>{
    const details = [...document.querySelectorAll('details')];
    const target = details.find(node => node.querySelector('.diff-grid,.diff-table'));
    if (!target) return {found: false};
    target.open = true;
    target.scrollIntoView({block: 'start', inline: 'nearest'});
    const rect = target.getBoundingClientRect();
    const width = Math.min(window.innerWidth, Math.max(320, rect.width));
    const height = Math.min(1000, Math.max(600, rect.height));
    return {
      found: true,
      summary: target.querySelector('summary')?.textContent?.trim() || '',
      clip: {x: Math.max(0, rect.left + window.scrollX), y: Math.max(0, rect.top + window.scrollY), width, height},
    };
  })()`);
}

async function restoreAfterDiffScreenshot() {
  await evaluate(`(()=>{
    const details = [...document.querySelectorAll('details')];
    const target = details.find(node => node.querySelector('.diff-grid,.diff-table'));
    if (target) target.open = false;
    window.scrollTo(0, 0);
    return true;
  })()`);
}

let receipt = null;
let passed = false;
let failure = null;
let screenshot = null;
let screenshots = {};

try {
  await fs.copyFile(report, copied);
  profile = await fs.mkdtemp(path.join(os.tmpdir(), 'ks-stage-ready-edge-'));
  const edgeCandidates = [
    'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
  ];
  let edge;
  for (const candidate of edgeCandidates) {
    try {
      await fs.access(candidate);
      edge = candidate;
      break;
    } catch {}
  }
  if (!edge) throw new Error('Microsoft Edge executable not found');
  browser = spawn(edge, [
    '--headless=new',
    '--disable-gpu',
    '--no-first-run',
    '--disable-extensions',
    '--disable-background-networking',
    '--proxy-server=direct://',
    '--proxy-bypass-list=*',
    '--remote-debugging-port=0',
    `--user-data-dir=${profile}`,
    'about:blank',
  ], {windowsHide: true, stdio: 'ignore'});

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      browserPort = (await fs.readFile(path.join(profile, 'DevToolsActivePort'), 'utf8')).split(/\r?\n/)[0];
      if (browserPort) break;
    } catch {
      await sleep(100);
    }
  }
  if (!browserPort) throw new Error('Local Edge did not start');
  const tabs = await (await fetch(`http://127.0.0.1:${browserPort}/json/list`)).json();
  const page = tabs.find(tab => tab.type === 'page');
  if (!page?.webSocketDebuggerUrl) throw new Error('Local Edge page target not found');
  ws = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, {once: true});
    ws.addEventListener('error', reject, {once: true});
  });
  ws.addEventListener('message', event => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }
    if (message.method === 'Runtime.exceptionThrown') {
      runtimeErrors.push({description: message.params?.exceptionDetails?.text || 'Runtime exception'});
    }
    if (message.method === 'Log.entryAdded' && message.params?.entry?.level === 'error') {
      consoleErrors.push({text: message.params.entry.text || message.params.entry.url || 'console error'});
    }
    if (message.method === 'Network.requestWillBeSent') {
      const url = message.params?.request?.url;
      if (isExternalUrl(url)) externalRequests.push({url, type: message.params?.type || 'Other'});
    }
    if (pending.has(message.id)) {
      const request = pending.get(message.id);
      clearTimeout(request.timer);
      pending.delete(message.id);
      if (message.error) request.reject(new Error(JSON.stringify(message.error)));
      else request.resolve(message.result);
    }
  });

  await send('Page.enable');
  await send('Runtime.enable');
  await send('Log.enable');
  await send('Network.enable');
  await send('Network.setBlockedURLs', {urls: ['http://*', 'https://*']});
  await send('Emulation.setDeviceMetricsOverride', {width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false});
  await send('Page.navigate', {url: pathToFileURL(copied).href});
  await waitUntilReady();

  const documentInfo = await inspectDocument();
  const modeToggle = await inspectModeToggle();
  const foldables = await inspectFoldables();
  const desktop = await inspectViewport();
  await evaluate('window.scrollTo(0, 0)');
  await sleep(80);
  try {
    screenshots.fullPage = await captureScreenshot('stage-report-desktop-full.png', true);
  } catch (error) {
    screenshots.fullPage = {error: errorText(error)};
  }
  await evaluate('window.scrollTo(0, 0)');
  await sleep(80);
  try {
    screenshots.firstScreen = await captureScreenshot('stage-report-desktop-first-screen.png', false);
  } catch (error) {
    screenshots.firstScreen = {error: errorText(error)};
  }
  try {
    const prepared = await prepareDiffScreenshot();
    if (!prepared.found) throw new Error('First diff details element not found');
    await sleep(120);
    screenshots.firstDiffExpanded = {
      ...(await captureScreenshot('stage-report-diff-expanded.png', false, prepared.clip)),
      summary: prepared.summary,
    };
  } catch (error) {
    screenshots.firstDiffExpanded = {error: errorText(error)};
  } finally {
    try { await restoreAfterDiffScreenshot(); } catch {}
  }
  screenshot = screenshots.fullPage?.path ? screenshots.fullPage : screenshots.firstScreen;

  await send('Emulation.setDeviceMetricsOverride', {width: 390, height: 844, deviceScaleFactor: 1, mobile: true});
  const mobile = await inspectViewport();

  const requiredSectionsPass = documentInfo.sections.missing.length === 0;
  const externalResources = {
    domResourceRefs: documentInfo.resourceRefs,
    cssExternalRefs: documentInfo.cssExternalRefs,
    networkRequests: externalRequests,
    count: documentInfo.resourceRefs.length + documentInfo.cssExternalRefs.length + externalRequests.length,
  };
  passed = modeToggle.pass
    && requiredSectionsPass
    && documentInfo.syntaxTokenCount > 30
    && documentInfo.diffContainers > 0
    && documentInfo.diffGroups >= 5
    && documentInfo.diffLayout.pass
    && foldables.pass
    && documentInfo.brokenAnchors.length === 0
    && externalResources.count === 0
    && runtimeErrors.length === 0
    && consoleErrors.length === 0
    && documentInfo.placeholderMatches.length === 0
    && !desktop.pageOverflow
    && !mobile.pageOverflow
    && Boolean(screenshots.fullPage?.path)
    && Boolean(screenshots.firstScreen?.path)
    && Boolean(screenshots.firstDiffExpanded?.path);
  receipt = {
    status: passed ? 'PASS' : 'REVIEW_REQUIRED',
    document_only: true,
    report,
    standaloneCopiedFile: copied,
    offline: true,
    viewportChecks: {desktop, mobile},
    sections: documentInfo.sections,
    modeToggle,
    code: {blocks: documentInfo.codeBlocks, syntaxTokenCount: documentInfo.syntaxTokenCount, requiredMinimum: 31},
    diff: {containers: documentInfo.diffContainers, groups: documentInfo.diffGroups, groupCounts: documentInfo.diffGroupCounts},
    diffLayout: documentInfo.diffLayout,
    foldables,
    anchors: {checked: documentInfo.anchors.length, broken: documentInfo.brokenAnchors},
    externalResources,
    placeholders: documentInfo.placeholderMatches,
    jsErrors: [...runtimeErrors, ...consoleErrors],
    screenshot,
    screenshots,
    inferenceTestsExecuted: false,
    cpuCandidateTestsExecuted: false,
    serverActions: 0,
    sshUsed: false,
  };
} catch (error) {
  failure = errorText(error);
  passed = false;
  receipt = {
    status: 'REVIEW_REQUIRED',
    document_only: true,
    report,
    standaloneCopiedFile: copied,
    offline: true,
    error: failure,
    jsErrors: [...runtimeErrors, ...consoleErrors],
    externalRequests,
    screenshot,
    screenshots,
    inferenceTestsExecuted: false,
    cpuCandidateTestsExecuted: false,
    serverActions: 0,
    sshUsed: false,
  };
} finally {
  // 保留旧脚本的浏览器/WebSocket清理；临时HTML和临时Edge profile也只删除本次创建的目录。
  if (ws?.readyState === 1) {
    try { await send('Browser.close'); } catch {}
    try { ws.close(); } catch {}
  }
  if (browser && browser.exitCode === null) browser.kill();
  try { await fs.rm(temp, {recursive: true, force: true}); } catch {}
  if (profile) {
    try { await fs.rm(profile, {recursive: true, force: true}); } catch {}
  }
}

if (!receipt) {
  receipt = {
    status: 'REVIEW_REQUIRED',
    document_only: true,
    report,
    error: failure || 'No document receipt was produced',
    inferenceTestsExecuted: false,
    cpuCandidateTestsExecuted: false,
    serverActions: 0,
    sshUsed: false,
  };
}
await fs.writeFile(receiptPath, JSON.stringify(receipt, null, 2), 'utf8');
console.log(JSON.stringify(receipt, null, 2));
if (!passed) process.exitCode = 1;
