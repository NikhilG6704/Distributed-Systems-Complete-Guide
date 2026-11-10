(() => {
  const q = (selector, root = document) => root.querySelector(selector);
  const esc = (value) => String(value).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
  const pageId = location.pathname.split('/').pop().replace(/\.html?$/i, '') || '_general';
  const onHome = pageId === 'index' || pageId === '_general';
  const pageTitle = () => q('.article h1')?.textContent?.trim() || 'Field Guide Notebook';
  const storeGet = (key) => { try { return localStorage.getItem(key); } catch { return null; } };
  const storeSet = (key, value) => { try { localStorage.setItem(key, value); return true; } catch { return false; } };
  let overlay;
  let activeObjectUrl;
  const FLOW_MAPS = {
    '01-distributed-systems-foundations': { title: 'A request crosses machine boundaries', steps: [['Client', 'Creates a request'], ['Network', 'Routes across machines'], ['Service', 'Runs application logic'], ['Storage', 'Reads or writes state'], ['Response', 'Returns with uncertainty']] },
    '02-distributed-systems-chapter': { title: 'Causality becomes an ordering', steps: [['Local event', 'Process updates its clock'], ['Send', 'Timestamp travels with message'], ['Receive', 'Merge the observed clock'], ['Order', 'Compare causal histories']] },
    '03-distributed-systems-chapter': { title: 'A reliable request / response', steps: [['Encode', 'Serialize the message'], ['Transmit', 'Cross an unreliable network'], ['Timeout', 'Caller cannot infer outcome'], ['Retry safely', 'Idempotency key avoids duplicates'], ['Acknowledge', 'Return the confirmed result']] },
    '04-consistency-models': { title: 'A write becomes visible', steps: [['Client write', 'Choose a consistency contract'], ['Leader orders', 'Assign a position in history'], ['Replicas update', 'Propagate the new value'], ['Commit rule', 'Wait for required acknowledgements'], ['Read observes', 'Return an allowed version']] },
    '05-consensus-and-coordination-with-svgs': { title: 'Raft leader election and commit', steps: [['Follower', 'Wait for a valid heartbeat'], ['Timeout', 'Start a new election term'], ['Candidate', 'Request votes from peers'], ['Quorum', 'Become leader with a majority'], ['Replicate', 'Append entries to followers'], ['Commit', 'Apply entries in log order']] },
    '06-replication-with-svgs': { title: 'Replicate a write safely', steps: [['Client write', 'Send to the primary'], ['Durable log', 'Record before acknowledging'], ['Fan out', 'Ship the change to replicas'], ['Quorum ack', 'Meet the configured write rule'], ['Commit', 'Expose the accepted version'], ['Read policy', 'Choose freshness vs. latency']] },
    '07-partitioning-sharding': { title: 'Route a key to its shard', steps: [['Input key', 'Choose a stable partition key'], ['Hash / map', 'Compute its shard position'], ['Locate owner', 'Resolve shard to a node'], ['Route request', 'Send to the owning replica'], ['Rebalance', 'Move ownership as capacity changes']] },
    '08-distributed-transactions': { title: 'Saga state transitions', steps: [['Start workflow', 'Persist intent and workflow id'], ['Reserve stock', 'Make an idempotent local change'], ['Charge payment', 'Call the next service'], ['All succeed', 'Commit the business outcome'], ['Failure', 'Run compensating actions']] },
    '09-storage-systems-bundle': { title: 'LSM-tree write path', steps: [['Client write', 'Validate and encode record'], ['WAL append', 'Persist recovery information'], ['Memtable', 'Update in-memory ordered state'], ['Flush', 'Write an immutable SSTable'], ['Compact', 'Merge files and reclaim space']] },
    '10-messaging-and-streaming': { title: 'A record flows through a log', steps: [['Producer', 'Choose key and serialize'], ['Partition', 'Route by key to an ordered log'], ['Append', 'Persist record at an offset'], ['Acknowledge', 'Confirm the configured durability'], ['Consumer', 'Process and commit progress']] },
    '11-batch-and-large-scale-computation-bundle': { title: 'MapReduce execution', steps: [['Input split', 'Divide data into partitions'], ['Map', 'Emit intermediate key/value pairs'], ['Shuffle', 'Group equal keys across workers'], ['Reduce', 'Aggregate each key group'], ['Output', 'Commit completed partitions']] },
    '12-caching-and-content-delivery-with-diagrams': { title: 'Cache-aside read path', steps: [['Read request', 'Check the local cache'], ['Cache hit', 'Return the fresh-enough value'], ['Cache miss', 'Fetch from the source of truth'], ['Populate', 'Set value and expiration'], ['Next read', 'Serve from the cache']] },
    '13-load-balancing-and-traffic-management-phase-2': { title: 'Traffic routing and failover', steps: [['Request arrives', 'Edge receives traffic'], ['Health filter', 'Remove unhealthy targets'], ['Select backend', 'Apply routing policy'], ['Forward', 'Track latency and outcome'], ['Recover', 'Retry safely or drain target']] },
    '14-fault-tolerance-and-reliability-phase-2': { title: 'Circuit breaker states', states: true, nodes: [{x:170,y:180,title:'CLOSED',detail:'Requests flow normally'},{x:500,y:72,title:'OPEN',detail:'Fail fast during outage'},{x:830,y:180,title:'HALF-OPEN',detail:'Probe recovery carefully'}], edges: [{from:0,to:1,label:'failure threshold'},{from:1,to:2,label:'cooldown expires'},{from:2,to:0,label:'probe succeeds'},{from:2,to:1,label:'probe fails'}] },
    '15-scalability-and-architecture-patterns-with-diagrams': { title: 'Scale a bottleneck', steps: [['Measure', 'Find the constrained resource'], ['Choose a lever', 'Cache, partition, or replicate'], ['Add capacity', 'Scale up or scale out'], ['Rebalance', 'Move work and state safely'], ['Verify', 'Measure latency and saturation']] },
    '16-observability-complete': { title: 'Turn signals into an incident response', steps: [['Instrument', 'Emit traces, metrics, and logs'], ['Collect', 'Keep context and timestamps'], ['Correlate', 'Follow work across services'], ['Detect', 'Alert on user-facing symptoms'], ['Respond', 'Mitigate, learn, and improve']] },
    '17-security-in-distributed-systems-complete': { title: 'Authorize a service request', steps: [['Identify', 'Establish workload identity'], ['Authenticate', 'Verify credentials and peer'], ['Authorize', 'Check policy and scope'], ['Protect', 'Encrypt and constrain data'], ['Audit', 'Record security-relevant events']] },
    '18-verification-and-testing-complete': { title: 'Verify behavior under failure', steps: [['Specify', 'Write invariants and properties'], ['Generate', 'Explore schedules and inputs'], ['Inject fault', 'Drop, delay, or reorder events'], ['Check', 'Compare execution to the model'], ['Refine', 'Keep the failing trace as evidence']] },
    '19-landmark-papers': { title: 'From research question to system design', steps: [['Question', 'Name the failure or guarantee'], ['Model', 'Make assumptions explicit'], ['Result', 'Prove a limit or mechanism'], ['Engineering', 'Translate into a protocol'], ['Practice', 'Check the production contract']] },
  };

  function createOverlay(kind, title, subtitle, content, footer) {
    closeOverlay();
    overlay = document.createElement('div');
    overlay.className = 'tool-overlay' + (document.body.classList.contains('light') ? ' light' : '');
    const dialogClass = { notes: 'notes-dialog', flow: 'flow-dialog', file: 'file-dialog' }[kind] || '';
    overlay.innerHTML = `<section class="tool-dialog ${dialogClass}" role="dialog" aria-modal="true" aria-label="${esc(title)}"><header class="tool-head"><div><div class="tool-title">${esc(title)}</div><div class="tool-subtitle">${esc(subtitle)}</div></div><button class="tool-close" type="button" aria-label="Close">×</button></header>${content}${footer}</section>`;
    document.body.append(overlay);
    overlay.querySelector('.tool-close').addEventListener('click', closeOverlay);
    overlay.addEventListener('click', (event) => { if (event.target === overlay) closeOverlay(); });
    return overlay;
  }

  function closeOverlay() {
    if (overlay) overlay.remove();
    overlay = null;
    if (activeObjectUrl) { URL.revokeObjectURL(activeObjectUrl); activeObjectUrl = null; }
  }

  function openNotes() {
    const key = `fieldnotes-note:${pageId}`;
    const initial = storeGet(key) || '';
    const panel = createOverlay(
      'notes', 'My notes', pageTitle().toUpperCase(),
      `<div class="note-info">Notes autosave in this browser’s local storage and return when you reopen this file in the same browser profile. They are kept separate from the chapter file; download a text copy to move them elsewhere.</div><textarea class="note-area" aria-label="Your notes" placeholder="Jot down an idea, question, or design decision…">${esc(initial)}</textarea>`,
      `<footer class="tool-foot"><span class="saved-status" data-note-status>Saved in this browser</span><div class="tool-actions"><button class="tool-btn" data-clear-note>Clear</button><button class="tool-btn" data-download-note>Download .txt</button><button class="tool-btn primary" data-close-note>Done</button></div></footer>`,
    );
    const area = panel.querySelector('.note-area');
    const status = panel.querySelector('[data-note-status]');
    let timer;
    area.focus();
    area.addEventListener('input', () => {
      status.textContent = 'Saving…';
      clearTimeout(timer);
      timer = setTimeout(() => {
        status.textContent = storeSet(key, area.value) ? 'Saved in this browser' : 'Storage unavailable — download to keep';
      }, 250);
    });
    panel.querySelector('[data-close-note]').addEventListener('click', () => {
      clearTimeout(timer);
      storeSet(key, area.value);
      closeOverlay();
    });
    panel.querySelector('[data-clear-note]').addEventListener('click', () => {
      area.value = '';
      storeSet(key, '');
      status.textContent = 'Note cleared';
      area.focus();
    });
    panel.querySelector('[data-download-note]').addEventListener('click', () => {
      const blob = new Blob([`# ${pageTitle()} — notes\n\n${area.value}\n`], { type: 'text/plain;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `${pageId}-notes.txt`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
  }

  function outlineFromArticle() {
    const h1 = pageTitle();
    const headings = [...document.querySelectorAll('.prose h2, .prose h3')];
    const sections = [];
    let parent;
    for (const heading of headings) {
      const node = { title: heading.textContent.trim(), id: heading.id, children: [] };
      if (heading.tagName === 'H2') {
        sections.push(node);
        parent = node;
      } else if (parent) parent.children.push(node);
      else sections.push(node);
    }
    return { title: h1, sections, local: true };
  }

  function outlineFromMarkdown(text, title) {
    const sections = [];
    let parent;
    const slug = (value) => value.toLowerCase().replace(/[`*_~]/g, '').replace(/[^\p{L}\p{N}\s-]/gu, '').trim().replace(/\s+/g, '-');
    for (const line of text.split(/\r?\n/)) {
      const match = line.match(/^(#{1,3})\s+(.+?)\s*#*\s*$/);
      if (!match) continue;
      const node = { title: match[2].replace(/\s*\{#.*$/, '').replace(/[*_`]/g, ''), id: slug(match[2]), children: [] };
      if (match[1].length <= 2) { sections.push(node); parent = node; }
      else if (parent) parent.children.push(node);
    }
    return { title, sections, local: false };
  }

  function openMindMap(outline) {
    if (!outline.sections.length) outline.sections.push({ title: 'Concepts and notes', id: '', children: [] });
    const branches = outline.sections.map((section) => {
      const parent = outline.local && section.id
        ? `<a class="mind-topic" href="#${encodeURIComponent(section.id)}">${esc(section.title)}</a>`
        : `<span class="mind-topic">${esc(section.title)}</span>`;
      const leaves = section.children.map((child) => outline.local && child.id
        ? `<a class="mind-leaf" href="#${encodeURIComponent(child.id)}">${esc(child.title)}</a>`
        : `<span class="mind-leaf">${esc(child.title)}</span>`).join('');
      return `<div class="mind-branch">${parent}<div class="mind-leaves">${leaves || '<span class="mind-leaf">Core ideas</span>'}</div></div>`;
    }).join('');
    const panel = createOverlay(
      'mindmap', 'Chapter mind map', `${outline.sections.length} MAIN BRANCHES · BUILT FROM HEADINGS`,
      `<div class="mind-scroll"><div class="mind-canvas"><div class="mind-root">${esc(outline.title)}</div><div class="mind-branches">${branches}</div></div></div>`,
      '<footer class="tool-foot"><span>Choose a topic to jump to it</span><div class="tool-actions"><button class="tool-btn primary" data-close-map>Close map</button></div></footer>',
    );
    panel.querySelector('[data-close-map]').addEventListener('click', closeOverlay);
    panel.querySelectorAll('a[href^="#"]').forEach((link) => link.addEventListener('click', (event) => {
      event.preventDefault();
      const target = document.getElementById(decodeURIComponent(link.hash.slice(1)));
      closeOverlay();
      target?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }));
  }

  function flowSvg(flow) {
    if (flow.nodes) {
      const width = 230, height = 76;
      const positions = flow.nodes.map((node) => ({ ...node, cx: node.x, cy: node.y, x: node.x - width/2, y: node.y - height/2 }));
      const edges = flow.edges.map((edge, i) => {
        const a = positions[edge.from], b = positions[edge.to];
        let d, lx, ly;
        if (edge.from === 2 && edge.to === 0) {
          d = `M${a.cx} ${a.y+height} C${a.cx} 300,${b.cx} 300,${b.cx} ${b.y+height}`; lx=500;ly=286;
        } else if (edge.from === 2 && edge.to === 1) {
          d = `M${a.x} ${a.cy-6} C680 20,650 18,${b.x+width} ${b.cy}`; lx=660;ly=38;
        } else {
          const startX = a.cx < b.cx ? a.x+width : a.x;
          const endX = a.cx < b.cx ? b.x : b.x+width;
          d = `M${startX} ${a.cy} C${(startX+endX)/2} ${a.cy},${(startX+endX)/2} ${b.cy},${endX} ${b.cy}`;
          lx=(startX+endX)/2;ly=(a.cy+b.cy)/2-12;
        }
        return `<path class="flow-edge" d="${d}" marker-end="url(#flow-arrow)"/><text class="edge-label" x="${lx}" y="${ly}" text-anchor="middle">${esc(edge.label)}</text><circle class="flow-packet" r="4"><animateMotion dur="${2.4+i*.3}s" repeatCount="indefinite" path="${d}"/></circle>`;
      }).join('');
      const nodes = positions.map((node) => `<g class="flow-node" role="group" aria-label="${esc(node.title)}: ${esc(node.detail)}"><rect x="${node.x}" y="${node.y}" width="${width}" height="${height}" rx="14"/><text class="state-title" x="${node.cx}" y="${node.cy-5}" text-anchor="middle">${esc(node.title)}</text><text class="state-detail" x="${node.cx}" y="${node.cy+18}" text-anchor="middle">${esc(node.detail)}</text></g>`).join('');
      return `<svg class="flow-svg state-svg" viewBox="0 0 1000 340" role="img" aria-label="${esc(flow.title)} state transition diagram"><defs><pattern id="flow-grid" width="22" height="22" patternUnits="userSpaceOnUse"><path d="M22 0H0V22" fill="none" stroke="#7d8c72" stroke-opacity=".1"/></pattern><marker id="flow-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0 0 6 3 0 6" fill="none" stroke="#9cad73" stroke-width="1.2"/></marker></defs><rect width="1000" height="340" rx="8" fill="url(#flow-grid)"/>${edges}${nodes}</svg>`;
    }
    const steps = flow.steps || [];
    const nodeW = 250, nodeH = 66, centerX = [150, 500, 850], rowGap = 124;
    const rowCount = Math.ceil(steps.length / 3);
    const viewH = Math.max(240, 82 + rowCount * rowGap);
    const pos = steps.map((step, i) => {
      const row = Math.floor(i / 3), offset = i % 3;
      const col = row % 2 ? 2 - offset : offset;
      return { x: centerX[col], y: 58 + row * rowGap, title: step[0], detail: step[1] };
    });
    const edges = pos.slice(0, -1).map((a, i) => {
      const b = pos[i + 1];
      let x1, y1, x2, y2, d, lx, ly;
      if (a.y === b.y) {
        const dir = b.x > a.x ? 1 : -1;
        x1 = a.x + dir * (nodeW / 2); x2 = b.x - dir * (nodeW / 2);
        y1 = y2 = a.y + nodeH / 2;
        d = `M${x1} ${y1} H${x2}`; lx = (x1 + x2) / 2; ly = y1 - 10;
      } else {
        x1 = x2 = a.x; y1 = a.y + nodeH; y2 = b.y;
        d = `M${x1} ${y1} C${x1} ${y1 + 25},${x2} ${y2 - 25},${x2} ${y2}`; lx = x1 + 44; ly = (y1 + y2) / 2;
      }
      const label = (flow.edges || [])[i] || '';
      return `<path class="flow-edge" d="${d}" marker-end="url(#flow-arrow)"/>${label ? `<text class="edge-label" x="${lx}" y="${ly}" text-anchor="middle">${esc(label)}</text>` : ''}<circle class="flow-packet" r="4"><animateMotion dur="${2.2 + (i % 3) * .45}s" repeatCount="indefinite" path="${d}"/></circle>`;
    }).join('');
    const nodes = pos.map((n, i) => `<g class="flow-node" role="group" aria-label="${esc(n.title)}: ${esc(n.detail)}"><rect x="${n.x-nodeW/2}" y="${n.y}" width="${nodeW}" height="${nodeH}" rx="9"/><circle class="flow-index" cx="${n.x-nodeW/2+18}" cy="${n.y+20}" r="10"/><text class="flow-number" x="${n.x-nodeW/2+18}" y="${n.y+24}" text-anchor="middle">${i+1}</text><text class="flow-title" x="${n.x-nodeW/2+36}" y="${n.y+24}">${esc(n.title)}</text><text class="flow-detail" x="${n.x-nodeW/2+17}" y="${n.y+48}">${esc(n.detail)}</text></g>`).join('');
    return `<svg class="flow-svg" viewBox="0 0 1000 ${viewH}" role="img" aria-label="${esc(flow.title)} transition diagram"><defs><pattern id="flow-grid" width="22" height="22" patternUnits="userSpaceOnUse"><path d="M22 0H0V22" fill="none" stroke="#7d8c72" stroke-opacity=".1"/></pattern><marker id="flow-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0 0 6 3 0 6" fill="none" stroke="#9cad73" stroke-width="1.2"/></marker></defs><rect width="1000" height="${viewH}" rx="8" fill="url(#flow-grid)"/>${edges}${nodes}</svg>`;
  }

  function openFlow() {
    const id = pageId;
    const flow = FLOW_MAPS[id] || { title: `${pageTitle()} concept flow`, steps: [['Start', 'Establish the initial condition'], ['Coordinate', 'Exchange state across components'], ['Transition', 'Apply the protocol rule'], ['Check', 'Verify the resulting state']] };
    const panel = createOverlay(
      'flow', 'Topic transition diagram', flow.title,
      `<div class="flow-scroll">${flowSvg(flow)}</div>`,
      '<footer class="tool-foot"><span>Follow the arrows to trace the topic workflow</span><div class="tool-actions"><button class="tool-btn primary" data-close-flow>Close diagram</button></div></footer>',
    );
    panel.querySelector('[data-close-flow]').addEventListener('click', closeOverlay);
  }

  function inlineMarkdown(node) {
    if (node.nodeType === Node.TEXT_NODE) return node.nodeValue.replace(/\s+/g, ' ');
    if (node.nodeType !== Node.ELEMENT_NODE) return '';
    const tag = node.tagName.toLowerCase();
    if (['script', 'style', 'noscript', 'svg', 'iframe', 'button', 'input'].includes(tag)) return '';
    const inner = [...node.childNodes].map(inlineMarkdown).join('').replace(/\s+/g, ' ').trim();
    if (!inner && tag !== 'img' && tag !== 'br') return '';
    if (tag === 'strong' || tag === 'b') return `**${inner}**`;
    if (tag === 'em' || tag === 'i') return `*${inner}*`;
    if (tag === 'del' || tag === 's') return `~~${inner}~~`;
    if (tag === 'code') return `\`${inner}\``;
    if (tag === 'br') return '  \n';
    if (tag === 'img') return `![${node.getAttribute('alt') || ''}](${node.getAttribute('src') || ''})`;
    if (tag === 'a') {
      const href = node.getAttribute('href') || '';
      return href ? `[${inner || href}](${href})` : inner;
    }
    return inner;
  }

  function blockMarkdown(node, depth = 0) {
    if (node.nodeType === Node.TEXT_NODE) return node.nodeValue.trim();
    if (node.nodeType !== Node.ELEMENT_NODE) return '';
    const tag = node.tagName.toLowerCase();
    const cls = node.classList;
    if (['script', 'style', 'noscript', 'svg', 'iframe', 'button', 'input', 'nav', 'aside'].includes(tag)
      || ['topbar', 'sidebar', 'toc', 'site-footer', 'chapter-nav', 'article-meta'].some((name) => cls.contains(name))) return '';
    const children = [...node.childNodes];
    const blocks = () => children.map((child) => blockMarkdown(child, depth)).filter(Boolean).join('\n\n');
    if (/^h[1-6]$/.test(tag)) return `${'#'.repeat(Number(tag[1]))} ${inlineMarkdown(node).trim()}`;
    if (tag === 'pre') {
      const code = node.querySelector('code');
      const language = code?.className.match(/language-([\w+-]+)/)?.[1] || '';
      return `\`\`\`${language}\n${(code || node).textContent.trim()}\n\`\`\``;
    }
    if (tag === 'ul' || tag === 'ol') {
      return [...node.children].filter((child) => child.tagName.toLowerCase() === 'li').map((item, i) => {
        const nested = [...item.children].filter((child) => ['ul', 'ol'].includes(child.tagName.toLowerCase()));
        const lead = [...item.childNodes].filter((child) => !(child.nodeType === Node.ELEMENT_NODE && ['ul', 'ol'].includes(child.tagName.toLowerCase()))).map(inlineMarkdown).join('').trim();
        const marker = tag === 'ol' ? `${i + 1}.` : '-';
        const sub = nested.map((list) => blockMarkdown(list, depth + 1)).filter(Boolean).join('\n').replace(/^/gm, '  ');
        return `${'  '.repeat(depth)}${marker} ${lead}${sub ? `\n${sub}` : ''}`;
      }).join('\n');
    }
    if (tag === 'blockquote') return blocks().split('\n').map((line) => `> ${line}`).join('\n');
    if (tag === 'hr') return '---';
    if (tag === 'table') {
      const rows = [...node.querySelectorAll('tr')].map((row) => [...row.children].map((cell) => inlineMarkdown(cell).replace(/\|/g, '\\|').trim()));
      if (!rows.length) return '';
      const line = (cells) => `| ${cells.join(' | ')} |`;
      return [line(rows[0]), line(rows[0].map(() => '---')), ...rows.slice(1).map(line)].join('\n');
    }
    if (['p', 'div', 'section', 'article', 'main', 'header', 'figure', 'figcaption', 'dl', 'dt', 'dd'].includes(tag)) return blocks();
    return inlineMarkdown(node);
  }

  function htmlToMarkdown(source, fallbackTitle = 'Converted document') {
    const doc = new DOMParser().parseFromString(source, 'text/html');
    const article = doc.querySelector('.article');
    const content = article?.querySelector('.prose') || article || doc.querySelector('main') || doc.body;
    const title = article?.querySelector('h1')?.textContent.trim() || doc.querySelector('h1')?.textContent.trim() || doc.title.trim() || fallbackTitle.replace(/\.(html?|xhtml)$/i, '');
    const deck = article?.querySelector('.article-deck')?.textContent.trim();
    let markdown = blockMarkdown(content).replace(/\n{3,}/g, '\n\n').trim();
    if (title && markdown.startsWith(`# ${title}`)) markdown = markdown.slice(title.length + 2).trimStart();
    return `${title ? `# ${title}\n\n` : ''}${deck ? `${deck}\n\n` : ''}${markdown}\n`;
  }

  function downloadMarkdown(markdown, name) {
    const blob = new Blob([markdown], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = name.replace(/\.(html?|xhtml)$/i, '') + '.md';
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  async function openFile(file) {
    if (!file) return;
    const name = file.name || 'Local file';
    const ext = name.split('.').pop().toLowerCase();
    const size = file.size < 1024 * 1024 ? `${Math.max(1, Math.round(file.size / 1024))} KB` : `${(file.size / 1024 / 1024).toFixed(1)} MB`;
    const textTypes = new Set(['txt','md','markdown','mdown','json','csv','tsv','yaml','yml','xml','html','htm','log','py','js','jsx','ts','tsx','java','go','rs','sql','sh','css','svg','toml','ini','conf','properties','c','cpp','h','hpp','rb','php','swift','kt','gradle','proto']);
    const isText = textTypes.has(ext) || file.type.startsWith('text/');
    let content = '', rawText = null, objectUrl = null, isMarkdown = ['md','markdown','mdown'].includes(ext);
    if (isText) {
      rawText = await file.text();
      if (ext === 'json') { try { rawText = JSON.stringify(JSON.parse(rawText), null, 2); } catch { /* show original malformed JSON */ } }
      content = `<pre class="file-text">${esc(rawText)}</pre>`;
    } else if (ext === 'pdf' || file.type === 'application/pdf') {
      objectUrl = URL.createObjectURL(file);
      content = `<iframe class="file-pdf" title="${esc(name)}" src="${objectUrl}"></iframe>`;
    } else if (file.type.startsWith('image/') || ['png','jpg','jpeg','gif','webp','svg','avif','bmp'].includes(ext)) {
      objectUrl = URL.createObjectURL(file);
      content = `<div class="file-media"><img src="${objectUrl}" alt="${esc(name)}"></div>`;
    } else if (file.type.startsWith('audio/') || ['mp3','wav','ogg','m4a','aac','flac'].includes(ext)) {
      objectUrl = URL.createObjectURL(file);
      content = `<div class="file-media file-audio"><div class="file-symbol">♫</div><audio controls src="${objectUrl}"></audio></div>`;
    } else if (file.type.startsWith('video/') || ['mp4','webm','mov','m4v'].includes(ext)) {
      objectUrl = URL.createObjectURL(file);
      content = `<div class="file-media"><video controls src="${objectUrl}"></video></div>`;
    } else {
      content = `<div class="file-unavailable"><div class="file-symbol">↧</div><h3>${esc(name)}</h3><p>This file type is not previewable in the browser. Download it to open it with a compatible app.</p></div>`;
    }
    const isHtml = ['html', 'htm', 'xhtml'].includes(ext);
    const mapAction = isText
      ? '<button class="tool-btn" data-map-file>Make mind map</button>'
      : '';
    const markdownAction = isHtml
      ? '<button class="tool-btn" data-convert-markdown>Convert to .md</button>'
      : '';
    const panel = createOverlay(
      'file', name, `${(file.type || 'FILE').toUpperCase()} · ${size} · LOCAL PREVIEW`,
      `<div class="file-preview">${content}</div>`,
      `<footer class="tool-foot"><span>Opened locally in this browser</span><div class="tool-actions">${mapAction}${markdownAction}<button class="tool-btn" data-download-file>Download</button><button class="tool-btn primary" data-close-file>Close</button></div></footer>`,
    );
    activeObjectUrl = objectUrl;
    panel.querySelector('[data-close-file]').addEventListener('click', closeOverlay);
    panel.querySelector('[data-download-file]').addEventListener('click', () => {
      const url = objectUrl || URL.createObjectURL(file);
      const link = document.createElement('a'); link.href = url; link.download = name; link.click();
      if (!objectUrl) setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
    panel.querySelector('[data-map-file]')?.addEventListener('click', () => {
      closeOverlay();
      openMindMap(outlineFromMarkdown(rawText || '', name.replace(/\.[^.]+$/, '')));
    });
    panel.querySelector('[data-convert-markdown]')?.addEventListener('click', () => {
      downloadMarkdown(htmlToMarkdown(rawText || '', name), name);
    });
  }

  function toggleSide(side) {
    if (side === 'left' && window.matchMedia('(max-width: 850px)').matches) {
      document.body.classList.remove('left-closed');
      q('#sidebar')?.classList.toggle('open');
      return;
    }
    const className = side === 'left' ? 'left-closed' : 'right-closed';
    document.body.classList.toggle(className);
    const hidden = document.body.classList.contains(className);
    document.querySelectorAll(`[data-side="${side}"]`).forEach((button) => {
      button.setAttribute('aria-pressed', String(hidden));
      button.title = `${hidden ? 'Show' : 'Hide'} ${side === 'left' ? 'chapter' : 'contents'} sidebar`;
    });
  }

  function toggleRead(id) {
    if (!id) return;
    let seen;
    try { seen = new Set(JSON.parse(storeGet('ds-seen') || '[]')); } catch { seen = new Set(); }
    if (seen.has(id)) seen.delete(id); else seen.add(id);
    storeSet('ds-seen', JSON.stringify([...seen]));
    document.querySelectorAll(`[data-read-toggle="${CSS.escape(id)}"]`).forEach((button) => {
      const isRead = seen.has(id);
      button.classList.toggle('is-read', isRead);
      button.setAttribute('aria-pressed', String(isRead));
      button.setAttribute('aria-label', isRead ? 'Mark chapter unread' : 'Mark chapter read');
      button.title = isRead ? 'Mark chapter unread' : 'Mark chapter read';
      button.textContent = (button.classList.contains('card-read-toggle') || button.classList.contains('chapter-read-toggle')) ? (isRead ? '✓ Read' : '○ Unread') : (isRead ? '✓' : '○');
    });
    window.dispatchEvent(new Event('ds-progress-change'));
  }

  document.addEventListener('click', (event) => {
    const side = event.target.closest('[data-side]')?.dataset.side;
    if (side) toggleSide(side);
    const readToggle = event.target.closest('[data-read-toggle]');
    if (readToggle) { event.preventDefault(); event.stopPropagation(); toggleRead(readToggle.dataset.readToggle); }
    if (event.target.closest('[data-theme-toggle]')) {
      document.body.classList.toggle('light');
      storeSet('ds-theme', document.body.classList.contains('light') ? 'light' : 'dark');
      event.target.closest('[data-theme-toggle]').textContent = document.body.classList.contains('light') ? '◑ Light' : '◐ Dark';
      window.dispatchEvent(new Event('ds-theme-change'));
    }
    if (event.target.closest('#reopen-left')) {
      document.body.classList.remove('left-closed');
      q('#sidebar')?.classList.add('open');
    }
    if (event.target.closest('#reopen-right')) document.body.classList.remove('right-closed');
    if (event.target.closest('[data-open-notes]')) openNotes();
    if (event.target.closest('[data-open-map]')) openMindMap(outlineFromArticle());
    if (event.target.closest('[data-open-flow]')) openFlow();
    if (event.target.closest('[data-open-map-file]')) q('#map-file-input')?.click();
    if (event.target.closest('[data-open-file]')) q('#local-file-input')?.click();
    if (event.target.closest('[data-export-markdown]')) {
      const article = q('.article');
      if (article) downloadMarkdown(htmlToMarkdown(article.outerHTML, pageTitle()), pageTitle());
    }
    if (event.target.closest('#menu-toggle')) {
      if (!window.matchMedia('(max-width: 850px)').matches) toggleSide('left');
    }
  });
  q('#map-file-input')?.addEventListener('change', (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => openMindMap(outlineFromMarkdown(String(reader.result || ''), file.name.replace(/\.(md|markdown)$/i, '')));
    reader.readAsText(file);
    event.target.value = '';
  });
  q('#local-file-input')?.addEventListener('change', (event) => {
    const file = event.target.files?.[0];
    openFile(file);
    event.target.value = '';
  });
  document.addEventListener('keydown', (event) => { if (event.key === 'Escape' && overlay) closeOverlay(); });
})();
