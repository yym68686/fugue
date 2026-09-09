const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({headless:true, executablePath:process.env.FUGUE_TUI_BROWSER || undefined});
  const root = process.env.FUGUE_TUI_SNAPSHOT_DIR;
  if(!root)throw Error('FUGUE_TUI_SNAPSHOT_DIR is required');
  for (const file of fs.readdirSync(root).sort()) {
    const match = /^(dashboard|startup|gap|cluster)-(\d+)x(\d+)-(carbon|light|terminal)\.ansi$/.exec(file);
    if (!match) continue;
    if (process.env.FUGUE_TUI_SCREEN_FILTER ? !new RegExp(process.env.FUGUE_TUI_SCREEN_FILTER).test(file) : match[4] !== 'carbon') continue;
    const cols = Number(match[2]), rows = Number(match[3]);
    const background=match[4]==='light'?'#fafafa':'#0d1117',foreground=match[4]==='light'?'#202329':'#dce6f0';
    const page = await browser.newPage({viewport:{width:cols*10+32,height:rows*21+32},deviceScaleFactor:1});
    await page.setContent(`<html><head><style>html,body{margin:0;background:${background};}#terminal{padding:16px;width:max-content;}</style></head><body><div id="terminal"></div></body></html>`);
    await page.addStyleTag({content:fs.readFileSync(require.resolve('@xterm/xterm/css/xterm.css'),'utf8')});
    await page.addScriptTag({content:fs.readFileSync(require.resolve('@xterm/xterm'),'utf8')});
    const contents=fs.readFileSync(path.join(root,file),'utf8');
    const dimensions=await page.evaluate(async ({cols,rows,contents,background,foreground})=>{
      const terminal=new Terminal({cols,rows,fontFamily:'Menlo,monospace',fontSize:16,lineHeight:1.1,convertEol:true,theme:{background,foreground},cursorBlink:false});
      terminal.open(document.getElementById('terminal'));window.terminal=terminal;
      await new Promise(resolve=>terminal.write(contents,resolve));
      const bounds=document.querySelector('.xterm').getBoundingClientRect();return {width:Math.ceil(bounds.width)+32,height:Math.ceil(bounds.height)+32};
    },{cols,rows,contents,background,foreground});
    await page.setViewportSize(dimensions);
    await page.screenshot({path:path.join(root,file.replace('-carbon.ansi','.png').replace('.ansi','.png'))});
    console.log(JSON.stringify({scenario:match[1],cols,rows,...dimensions}));await page.close();
  }
  await browser.close();
})().catch(error=>{console.error(error);process.exit(1)});
