<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>ESH KPI 모니터링 v9</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js" defer></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js" defer></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" defer></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#f3f4f6;--sf:#fff;--sf2:#f0f2f5;--bd:#e2e5ea;--tx:#1a1d23;--tx2:#5a6070;--tx3:#9aa0ad;
  --bl:#1059a0;--bll:#e8f1fb;--bld:#0a3f75;
  --gn:#1059a0;--gnl:#e8f1fb;--gnd:#0a3f75;
  --am:#2d7dd2;--aml:#d6eaf8;
  --rd:#c0455e;--rdl:#fde8ec;
  --pu:#7c3aed;--pul:#f5f3ff;--r:10px;--rs:6px}
body{font-family:'Pretendard','Noto Sans KR',system-ui,sans-serif;background:var(--bg);color:var(--tx);font-size:14px}
*{box-sizing:border-box}
.hdr{background:var(--sf);border-bottom:1px solid var(--bd);padding:0 20px;display:flex;align-items:center;gap:10px;height:50px;position:sticky;top:0;z-index:300}
.logo{font-size:14px;font-weight:700;color:var(--bl);white-space:nowrap}.logo span{color:var(--tx);font-weight:400}
.tabs{display:flex;gap:1px;overflow:hidden}
.tab{padding:5px 13px;border-radius:var(--rs);font-size:13px;cursor:pointer;color:var(--tx2);border:none;background:none;font-family:inherit;white-space:nowrap}
.tab.on{background:var(--bll);color:var(--bl);font-weight:600}.tab:hover:not(.on){background:var(--sf2)}
.hdr-r{display:flex;gap:6px;align-items:center;margin-left:auto;flex-shrink:0}
.dot{width:7px;height:7px;border-radius:50%;background:var(--tx3)}.dot.ok{background:var(--gn)}.dot.warn{background:var(--am)}
#stxt{font-size:12px;color:var(--tx2);white-space:nowrap}
.btn{display:inline-flex;align-items:center;gap:4px;padding:5px 12px;border-radius:var(--rs);font-size:13px;font-weight:500;cursor:pointer;border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit;white-space:nowrap}
.btn:hover{background:var(--sf2)}.btn:disabled{opacity:.4;cursor:not-allowed}
.bp{background:var(--bl);color:#fff;border-color:var(--bl)}.bp:hover{background:var(--bld)}
.bg{background:var(--gnl);color:var(--gnd);border-color:var(--gn)}
.brd{color:var(--rd);border-color:var(--rd)}.brd:hover{background:var(--rdl)}
.bsm{padding:3px 9px;font-size:12px}
.page{display:none;padding:16px 20px;max-width:1440px;margin:0 auto;position:relative}.page.on{display:block}
.period-bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:9px 13px;margin-bottom:13px}
.ptype-grp{display:flex;gap:2px;background:var(--sf2);border-radius:var(--rs);padding:3px}
.ptab{padding:4px 11px;border-radius:4px;font-size:12px;font-weight:500;cursor:pointer;border:none;background:none;font-family:inherit;color:var(--tx2);white-space:nowrap}
.ptab.on{background:var(--sf);color:var(--bl);font-weight:700;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.psel{font-size:13px;padding:4px 9px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit}
.psep{width:1px;height:18px;background:var(--bd)}
.cust-row{display:none;align-items:center;gap:6px}
.cust-row.show{display:flex}
.cust-row input[type=date]{font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit}
.krow{display:grid;grid-template-columns:repeat(5,1fr);gap:9px;margin-bottom:13px}
.kc{background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:12px 14px}
.kc[onclick]:hover{background:var(--sf2);border-color:var(--bl);transform:translateY(-1px);transition:all .15s}
.kl{font-size:11px;color:var(--tx2);margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}
.kv{font-size:22px;font-weight:700;line-height:1}.ks{font-size:11px;color:var(--tx3);margin-top:3px}
.kc.hi{border-left:3px solid var(--bl)}.kc.hi .kv{color:var(--bl)}
.kc.danger .kv{color:var(--rd)}.kc.purp .kv{color:var(--pu)}
.dash-row{display:grid;grid-template-columns:240px 1fr;gap:13px;margin-bottom:13px}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:14px 16px}
.cttl{font-size:13px;font-weight:600;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between}
.cttl-sub{font-size:11px;color:var(--tx3);font-weight:400}
.trend-item{display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid var(--bd);font-size:12px}
.trend-item:last-child{border-bottom:none}
.trend-lbl{min-width:60px;color:var(--tx2);flex-shrink:0;font-size:9.4px;line-height:1.3}
.trend-bg{flex:1;height:6px;background:var(--sf2);border-radius:3px;overflow:hidden;position:relative}
.trend-fg{height:100%;border-radius:3px}
.trend-val{min-width:30px;text-align:right;font-weight:600;font-size:11px}
.chw{position:relative;width:100%}
.site-cards-wrap{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px}
.sc{background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:13px 15px;border-left-width:3px}
.sc-hd{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:7px}
.sc-nm{font-weight:700;font-size:14px}.sc-sub{font-size:12px;color:var(--tx2);margin-top:2px}.sc-rt{font-size:22px;font-weight:700}
.bb{height:5px;background:var(--sf2);border-radius:3px;overflow:hidden;margin-bottom:10px}.bf{height:100%;border-radius:3px}
.tree-dept-hd{display:flex;align-items:center;gap:7px;cursor:pointer;padding:3px 5px;border-radius:var(--rs);user-select:none}
.tree-dept-hd:hover{background:var(--sf2)}
.tree-tog{font-size:10px;color:var(--tx3);width:12px;flex-shrink:0}
.tree-dept-nm{font-size:12px;font-weight:600;color:var(--tx2);flex:1}
.tree-dept-cnt{font-size:11px;color:var(--rd);background:var(--rdl);padding:1px 6px;border-radius:10px}
.tree-members{padding-left:19px;display:none}.tree-members.open{display:block}
.tree-member{display:flex;align-items:center;justify-content:space-between;padding:3px 5px;border-radius:4px;margin-bottom:2px;cursor:pointer;font-size:12px}
.tree-member:hover{background:var(--bll)}
.tree-member-cnt{font-size:11px;font-weight:600;padding:1px 6px;border-radius:10px}
.bdg{display:inline-flex;align-items:center;font-size:11px;font-weight:600;padding:2px 7px;border-radius:20px;white-space:nowrap}
.bok{background:#e8f8ee;color:#1e7a40}.bwn{background:#fff3cd;color:#b45309}
.bfl{background:var(--rdl);color:var(--rd)}.bgr{background:var(--sf2);color:var(--tx3)}.bex{background:var(--pul);color:var(--pu)}
.tw{overflow-x:auto;border:1px solid var(--bd);border-radius:var(--r);background:var(--sf)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:var(--sf2);padding:7px 10px;text-align:left;font-size:12px;font-weight:600;color:var(--tx2);white-space:nowrap;border-bottom:1px solid var(--bd);cursor:pointer;user-select:none}
thead th:hover{color:var(--bl)}
tbody td{padding:7px 10px;border-bottom:1px solid var(--bd);vertical-align:middle}
tbody tr:last-child td{border-bottom:none}tbody tr:hover{background:var(--sf2)}
.mb{display:flex;align-items:center;gap:5px}
.mbg{flex:1;height:5px;background:var(--sf2);border-radius:3px;overflow:hidden;min-width:34px}.mbf{height:100%;border-radius:3px}
.row-link{color:var(--bl);cursor:pointer;text-decoration:underline;text-decoration-color:transparent}
.row-link:hover{text-decoration-color:var(--bl)}
.ii{border:1px solid var(--bd);border-radius:4px;padding:3px 6px;font-size:12px;width:100%;font-family:inherit;background:var(--sf);color:var(--tx)}
.ii:focus{outline:2px solid var(--bl)}
td.ed{padding:3px 5px}
.xb{color:var(--tx3);cursor:pointer;font-size:14px;padding:0 3px;background:none;border:none}.xb:hover{color:var(--rd)}
.pagi{display:flex;gap:3px;align-items:center;justify-content:flex-end;margin-top:8px;flex-wrap:wrap}
.pg{padding:3px 9px;border-radius:4px;border:1px solid var(--bd);background:var(--sf);cursor:pointer;font-size:12px;font-family:inherit;color:var(--tx)}
.pg.on{background:var(--bl);color:#fff;border-color:var(--bl)}.pg:hover:not(.on){background:var(--sf2)}
.pi{font-size:12px;color:var(--tx2);margin:0 5px}
.overlay{position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:400;display:flex;align-items:center;justify-content:center}
.overlay.hide{display:none}
.mbox{background:var(--sf);border-radius:var(--r);padding:22px 24px;width:500px;max-width:96vw;border:1px solid var(--bd);max-height:90vh;overflow-y:auto}
.mbox.wide{width:700px}
.mttl{font-size:15px;font-weight:700;margin-bottom:14px;display:flex;align-items:center;justify-content:space-between}
.mclose{cursor:pointer;color:var(--tx3);font-size:18px;background:none;border:none;padding:0 2px}
.mf{margin-bottom:12px}.mf label{display:block;font-size:12px;color:var(--tx2);margin-bottom:4px}
.mf input,.mf textarea,.mf select{width:100%;padding:6px 9px;border-radius:var(--rs);border:1px solid var(--bd);font-size:13px;font-family:inherit;background:var(--sf);color:var(--tx)}
.mf input:focus,.mf textarea:focus{outline:2px solid var(--bl)}
.mf textarea{resize:vertical;min-height:68px;line-height:1.5}
.mhelp{font-size:12px;color:var(--tx2);margin-top:3px;line-height:1.5}
.mft{display:flex;gap:7px;justify-content:flex-end;margin-top:14px}
.person-info{display:flex;gap:11px;align-items:center;margin-bottom:14px;padding-bottom:13px;border-bottom:1px solid var(--bd)}
.avatar{width:42px;height:42px;border-radius:50%;background:var(--bll);display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;color:var(--bl);flex-shrink:0}
.person-meta{flex:1}.person-name{font-size:15px;font-weight:700}
.person-sub{font-size:12px;color:var(--tx2);margin-top:2px}
.person-kpi{text-align:right}.person-rate{font-size:28px;font-weight:800;line-height:1.1}
.person-rate-sub{font-size:11px;color:var(--tx2);margin-top:3px}
.cal-legend{display:flex;gap:10px;margin-bottom:9px;flex-wrap:wrap}
.cleg{display:flex;align-items:center;gap:4px;font-size:12px;color:var(--tx2)}
.cleg-dot{width:10px;height:10px;border-radius:3px}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px;margin-bottom:14px}
.cal-hd{text-align:center;font-size:11px;font-weight:600;color:var(--tx2);padding:3px 0}
.cal-day{text-align:center;font-size:12px;border-radius:5px;padding:4px 2px;min-height:34px;display:flex;flex-direction:column;align-items:center;justify-content:flex-start;gap:1px}
.cal-day .dn{font-weight:500}
.cal-day.ok{background:#d4edda;color:#276740}.cal-day.ok .dn{font-weight:700}
.cal-day.miss{background:#f9c0cc;color:#a0304a;cursor:crosshair;-webkit-user-select:none;user-select:none}
.cal-day.miss:hover{background:#f5a8b8}
.cal-day.miss.cal-drag-sel,.cal-day.miss.cal-sel{background:#bfdbfe!important;color:#1d4ed8!important;outline:2px solid #3b82f6;outline-offset:-2px;border-radius:6px}
.cal-day.locked{background:#f1f5f9!important;color:#94a3b8!important;cursor:not-allowed!important;opacity:.7}
.cal-day.locked .dn{color:#94a3b8!important}
.locked-month-banner{display:flex;align-items:center;gap:8px;padding:6px 12px;background:#f1f5f9;border:1px solid #cbd5e1;border-radius:var(--rs);font-size:12px;color:#64748b;margin-bottom:8px}
/* 관리자 마감 토글 */
.close-month-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:6px}
.cmth-btn{padding:8px 4px;border-radius:8px;border:1.5px solid var(--bd);background:var(--sf);font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;text-align:center;transition:all .15s;line-height:1.3}
.cmth-btn.closed{background:#fee2e2;border-color:#fca5a5;color:#dc2626}
.cmth-btn.closed::after{content:' 🔒';font-size:10px}
.cmth-btn:not(.closed):hover{border-color:var(--bl);background:var(--bll);color:var(--bl)}
.cal-day.exc{background:#d4c5f9;color:#4a1a9e;cursor:pointer}.cal-day.exc:hover{background:#c4b0f5}
.cal-day.wknd{opacity:.55}.cal-day.empty,.cal-day.future{background:none;opacity:.25}
.cal-count{font-size:9px;opacity:.8}
.miss-item{display:flex;align-items:center;justify-content:space-between;padding:6px 10px;border-radius:var(--rs);background:var(--sf2);margin-bottom:4px;font-size:13px}
.miss-date{color:var(--tx2)}
.pw-hint{font-size:12px;color:var(--rd);margin-top:6px}
#toast{position:fixed;bottom:18px;right:18px;background:#1a1d23;color:#fff;padding:9px 15px;border-radius:var(--rs);font-size:13px;z-index:9999;opacity:0;transition:opacity .2s;pointer-events:none}
#toast.show{opacity:1}
#loading-ov{position:fixed;top:80px;left:50%;transform:translateX(-50%);z-index:500;display:flex;align-items:center;gap:16px;background:linear-gradient(135deg,#0a1628 0%,#0d2144 100%);border:1px solid rgba(16,89,160,.6);border-radius:16px;padding:18px 28px;box-shadow:0 0 0 1px rgba(16,89,160,.2),0 8px 40px rgba(10,31,64,.5);transition:opacity .5s,transform .5s;white-space:nowrap;overflow:hidden;min-width:340px}
#loading-ov::before{content:'';position:absolute;inset:0;background:linear-gradient(90deg,transparent 0%,rgba(16,89,160,.08) 50%,transparent 100%);animation:ld-sweep 2s ease-in-out infinite}
@keyframes ld-sweep{0%{transform:translateX(-100%)}100%{transform:translateX(100%)}}
#loading-ov.hide{opacity:0;transform:translateX(-50%) translateY(-16px);pointer-events:none}
.ld-spinner{position:relative;width:32px;height:32px;flex-shrink:0}
.ld-spinner::before,.ld-spinner::after{content:'';position:absolute;inset:0;border-radius:50%;border:3px solid transparent}
.ld-spinner::before{border-top-color:#1059a0;border-right-color:#1059a0;animation:ld-spin .9s linear infinite}
.ld-spinner::after{border-bottom-color:rgba(16,89,160,.3);border-left-color:rgba(16,89,160,.3);animation:ld-spin .9s linear infinite reverse;inset:5px}
@keyframes ld-spin{to{transform:rotate(360deg)}}
.ld-body{flex:1;min-width:0}
.ld-txt{font-size:15px;font-weight:700;color:#e8f1fb;letter-spacing:.04em}
.ld-sub{font-size:11px;color:rgba(168,196,230,.7);margin-top:3px;letter-spacing:.06em;text-transform:uppercase}
.ld-dots::after{content:'';animation:ld-dot 1.2s steps(4,end) infinite}
@keyframes ld-dot{0%{content:''}25%{content:'.'}50%{content:'..'}75%{content:'...'}100%{content:''}}
.ld-progress-wrap{margin-top:10px;position:relative}
.ld-progress-bg{height:4px;background:rgba(16,89,160,.25);border-radius:4px;overflow:hidden}
.ld-progress-fg{height:100%;background:linear-gradient(90deg,#1059a0,#2d9cdb);border-radius:4px;transition:width .5s cubic-bezier(.4,0,.2,1);position:relative;overflow:hidden}
.ld-progress-fg::after{content:'';position:absolute;inset:0;background:linear-gradient(90deg,transparent,rgba(255,255,255,.3),transparent);animation:ld-sweep 1.2s ease-in-out infinite}
.ld-progress-meta{display:flex;justify-content:space-between;margin-top:5px}
.ld-pct{font-size:11px;font-weight:700;color:#2d9cdb;letter-spacing:.04em}
.ld-remain{font-size:11px;color:rgba(168,196,230,.6)}
.sec-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:9px}
.sec-ttl{font-size:13px;font-weight:600}
.fb{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
.fb select,.fb input[type=text]{font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit}
.fb label{font-size:12px;color:var(--tx2)}.fsep{width:1px;height:18px;background:var(--bd)}
.tag{font-size:11px;background:var(--sf2);color:var(--tx2);padding:2px 7px;border-radius:4px;white-space:nowrap}
.ref-banner{background:var(--aml);border:1px solid var(--am);border-radius:var(--r);padding:10px 14px;margin-bottom:12px;font-size:13px;display:flex;align-items:center;gap:10px}
.notice-bar{display:none;background:linear-gradient(135deg,var(--bll) 0%,#f0f7ff 100%);border:1px solid var(--bl);border-left:4px solid var(--bl);border-radius:var(--r);padding:8px 14px;margin-bottom:13px;gap:8px;align-items:center}
.notice-bar.show{display:flex}
.notice-ic{font-size:14px;flex-shrink:0}
.notice-body{flex:1;min-width:0;display:flex;align-items:center;gap:8px;overflow:hidden}
.notice-ttl{font-size:12px;font-weight:700;color:var(--bl);white-space:nowrap;flex-shrink:0}
.notice-sep{font-size:12px;color:var(--bd);flex-shrink:0}
.notice-txt{font-size:13px;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.notice-meta{font-size:11px;color:var(--tx3);white-space:nowrap;flex-shrink:0;margin-left:auto}
.logo{cursor:pointer;user-select:none}.logo:hover{opacity:.8}
.cal-day.pend{background:#fff3cd;color:#856404;cursor:pointer}.cal-day.pend:hover{background:#ffe69c}
.cal-day.rej{background:#ffe0e6;color:#a0304a;cursor:pointer}.cal-day.rej:hover{background:#ffc8d2}
.cleg-dot.pend{background:#fff3cd;border:1px solid #856404}
.cleg-dot.rej{background:#ffe0e6;border:1px solid #a0304a}
.miss-rej-box{background:#fde8ec;border:1px solid #f5b8c4;border-radius:var(--rs);padding:6px 10px;margin-top:4px;font-size:12px;color:#a0304a;line-height:1.5}
.excuse-pend-badge{display:inline-flex;align-items:center;gap:3px;font-size:11px;background:#fff3cd;color:#856404;border:1px solid #ffc107;padding:1px 7px;border-radius:10px;font-weight:600}
.ex-site-block{margin-bottom:20px}
.ex-site-hd{display:flex;align-items:center;gap:14px;margin-bottom:8px;padding-bottom:7px;border-bottom:2px solid var(--bl)}
.ex-site-nm{font-size:17px;font-weight:800;color:var(--tx)}
.ex-site-stat{font-size:13px;font-weight:400;color:var(--tx2)}
.ex-site-stat span{font-weight:700}
.ex-site-stat .s-pend{color:#856404}.ex-site-stat .s-ok{color:var(--gn)}.ex-site-stat .s-rej{color:var(--rd)}
.ex-tbl-wrap{border:1px solid var(--bd);border-radius:var(--rs);overflow:hidden;background:var(--sf)}
.ex-tbl-wrap table{width:100%;border-collapse:collapse;font-size:13px}
.ex-tbl-wrap thead th{background:var(--sf2);padding:7px 10px;text-align:left;font-size:12px;font-weight:600;color:var(--tx2);border-bottom:1px solid var(--bd);white-space:nowrap;vertical-align:middle}
.ex-tbl-wrap thead tr:first-child th[colspan]{border-bottom:1px solid var(--bd)}
.ex-tbl-wrap thead tr:last-child th{padding:4px 10px;font-size:11px}
.ex-tbl-wrap tbody td{padding:7px 10px;border-bottom:1px solid var(--bd);vertical-align:middle}
.ex-tbl-wrap tbody tr:last-child td{border-bottom:none}
.ex-tbl-wrap tbody tr:hover{background:var(--sf2)}
.site-pw-row{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--bd)}
.site-pw-row:last-child{border-bottom:none}
.site-pw-nm{font-size:13px;min-width:160px;font-weight:500}
.site-pw-inp{flex:1;padding:5px 9px;border-radius:var(--rs);border:1px solid var(--bd);font-size:13px;font-family:inherit;background:var(--sf);color:var(--tx)}
/* ★ 소명 모달: 주차별 격자 뷰 */
.exc-week-group{margin-bottom:10px;border:1.5px solid var(--bd);border-radius:10px;overflow:hidden}
.exc-week-hdr{display:flex;align-items:center;justify-content:space-between;padding:7px 12px;background:var(--sf2);font-size:12px;font-weight:700;color:var(--tx2);border-bottom:1px solid var(--bd)}
.exc-week-hdr .wh-miss{font-size:11px;font-weight:700;color:#dc2626;background:#fee2e2;padding:1px 8px;border-radius:10px}
.exc-week-hdr .wh-all{font-size:11px;color:var(--gnd);background:var(--gnl);padding:1px 8px;border-radius:10px}
.exc-week-grid{display:grid;grid-template-columns:repeat(7,1fr)}
.exc-day-cell{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:8px 2px;gap:3px;border-right:1px solid var(--bd);font-size:11px;min-height:62px;position:relative}
.exc-day-cell:last-child{border-right:none}
.exc-day-dow{font-size:10px;font-weight:700;color:var(--tx3)}
.exc-day-num{font-size:15px;font-weight:700;line-height:1}
.exc-day-lbl{font-size:9px;font-weight:600;padding:1px 5px;border-radius:6px;margin-top:1px}
.exc-day-cell.ok{background:#f0faf4}.exc-day-cell.ok .exc-day-num{color:#1e7a40}.exc-day-cell.ok .exc-day-lbl{background:#d4edda;color:#1e7a40}
.exc-day-cell.exc{background:#f5f0ff}.exc-day-cell.exc .exc-day-num{color:#6d28d9}.exc-day-cell.exc .exc-day-lbl{background:#ede9fe;color:#6d28d9}
.exc-day-cell.pend{background:#fffbeb}.exc-day-cell.pend .exc-day-num{color:#b45309}.exc-day-cell.pend .exc-day-lbl{background:#fef3c7;color:#b45309}
.exc-day-cell.rej-d{background:#fff1f2}.exc-day-cell.rej-d .exc-day-num{color:#be123c}.exc-day-cell.rej-d .exc-day-lbl{background:#ffe4e6;color:#be123c}
.exc-day-cell.miss{background:#fff7f7;cursor:pointer;-webkit-user-select:none;user-select:none}.exc-day-cell.miss:hover{background:#fee2e2}
.exc-day-cell.miss .exc-day-num{color:#dc2626}
.exc-day-cell.miss.selected{background:#eff6ff;border-color:transparent}.exc-day-cell.miss.selected .exc-day-num{color:#2563eb}
.exc-day-cell.miss.selected::after{content:'✓';position:absolute;top:4px;right:5px;font-size:10px;font-weight:900;color:#2563eb}
.exc-day-cell.wknd .exc-day-dow{color:#ef4444}.exc-day-cell.wknd .exc-day-num{color:#ef4444}
.exc-day-cell.miss.wknd .exc-day-num{color:#f97316}
.exc-day-cell.future,.exc-day-cell.out{background:#fafafa;opacity:.45;pointer-events:none}
.exc-day-cell.out .exc-day-num{color:var(--tx3);font-weight:400}
/* ★ 소명 사유 버튼 */
.exc-reason-btns{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.exc-rsn-btn{padding:14px 8px;border-radius:12px;border:2px solid var(--bd);background:var(--sf);cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:6px;transition:all .15s;font-family:inherit;position:relative}
.exc-rsn-btn:hover:not(.active){border-color:#93c5fd;background:#eff6ff}
.exc-rsn-btn.active.vacation{border-color:#3b82f6;background:#3b82f6;color:#fff;box-shadow:0 3px 14px rgba(59,130,246,.35);transform:translateY(-2px)}
.exc-rsn-btn.active.sick{border-color:#ef4444;background:#ef4444;color:#fff;box-shadow:0 3px 14px rgba(239,68,68,.35);transform:translateY(-2px)}
.exc-rsn-btn.active.business{border-color:#8b5cf6;background:#8b5cf6;color:#fff;box-shadow:0 3px 14px rgba(139,92,246,.35);transform:translateY(-2px)}
.exc-rsn-icon{font-size:24px;line-height:1}
.exc-rsn-lbl{font-size:13px;font-weight:700}
.exc-rsn-chk{position:absolute;top:7px;right:9px;font-size:11px;font-weight:900;display:none}
.exc-rsn-btn.active .exc-rsn-chk{display:block}
/* ★ 사유확인 사이트별 상태탭 + 스크롤 */
.ex-stat-tabs{display:flex;gap:2px;background:var(--sf2);border-radius:var(--rs);padding:3px;margin-bottom:10px;width:fit-content}
.ex-stat-tab{padding:5px 14px;border-radius:4px;font-size:12px;font-weight:500;cursor:pointer;border:none;background:none;font-family:inherit;color:var(--tx2);display:flex;align-items:center;gap:5px;white-space:nowrap}
.ex-stat-tab.on{background:var(--sf);color:var(--bl);font-weight:700;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.ex-stat-cnt{font-size:10px;font-weight:700;padding:1px 6px;border-radius:8px}
.ex-stat-tab .ex-stat-cnt.all{background:var(--bll);color:var(--bl)}
.ex-stat-tab .ex-stat-cnt.pend{background:#fff3cd;color:#856404}
.ex-stat-tab .ex-stat-cnt.ok{background:#d4edda;color:#1e7a40}
.ex-stat-tab .ex-stat-cnt.rej{background:var(--rdl);color:var(--rd)}
.ex-tbl-scroll{overflow-y:auto;max-height:520px}
.ex-more-bar{text-align:center;padding:8px;font-size:12px;color:var(--tx3);background:var(--sf2);border-top:1px solid var(--bd)}
</style>
</head>
<body oncontextmenu="return false" onselectstart="return false" ondragstart="return false">
<div id="loading-ov">
  <div class="ld-spinner"></div>
  <div class="ld-body">
    <div class="ld-txt" id="ld-txt">데이터 로드 중<span class="ld-dots"></span></div>
    <div class="ld-sub" id="ld-sub">CONNECTING TO ESH SYSTEM</div>
    <div class="ld-progress-wrap">
      <div class="ld-progress-bg"><div class="ld-progress-fg" id="ld-bar" style="width:0%"></div></div>
      <div class="ld-progress-meta"><span class="ld-pct" id="ld-pct">0%</span><span class="ld-remain" id="ld-remain">남은 시간 계산 중...</span></div>
    </div>
  </div>
</div>

<div id="conn-ov" class="overlay hide">
  <div class="mbox">
    <div class="mttl">Google Sheets 연결<button class="mclose" onclick="closeM('conn-ov')">✕</button></div>
    <div class="mf"><label>Apps Script 웹앱 URL</label><input type="text" id="murl" placeholder="https://script.google.com/macros/s/..."><div class="mhelp">배포 → 웹 앱으로 배포 → 액세스: 모든 사용자</div></div>
    <div class="mft"><button class="btn" onclick="closeM('conn-ov')">취소</button><button class="btn bp" onclick="saveConn()">저장 및 로드</button></div>
  </div>
</div>

<div id="pw-ov" class="overlay hide">
  <div class="mbox" style="width:360px">
    <div class="mttl">관리자 인증<button class="mclose" onclick="closeM('pw-ov')">✕</button></div>
    <div class="mf"><label>비밀번호</label><input type="password" id="pw-inp" onkeydown="if(event.key==='Enter')checkPw()"><div class="pw-hint" id="pw-hint"></div></div>
    <div class="mft"><button class="btn" onclick="closeM('pw-ov')">취소</button><button class="btn bp" onclick="checkPw()">확인</button></div>
  </div>
</div>

<!-- ★ 소명 모달: 드래그 날짜 + 버튼형 사유 -->
<div id="excuse-ov" class="overlay hide">
  <div class="mbox" style="width:520px">
    <div class="mttl">소명 입력<button class="mclose" onclick="closeM('excuse-ov')">✕</button></div>
    <div id="exc-person-info" style="background:linear-gradient(135deg,#1e3a8a,#2563eb);border-radius:var(--rs);padding:12px 15px;margin-bottom:14px;color:#fff"></div>
    <div class="mf">
      <label style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <span style="font-size:12px;color:var(--tx2);font-weight:600">📅 날짜 선택 <span style="color:var(--rd)">*</span></span>
        <span style="font-size:11px;color:var(--tx3)">날짜 클릭으로 선택</span>
      </label>
      <div id="exc-date-list" style="max-height:260px;overflow-y:auto;display:flex;flex-direction:column;gap:8px;padding:2px"></div>
      <div class="exc-date-ctrl" style="margin-top:8px">
        <div><button class="btn bsm" style="font-size:11px" onclick="excSelectAll()">전체 선택</button><button class="btn bsm" style="font-size:11px;margin-left:4px" onclick="excClearAll()">전체 해제</button></div>
        <span id="exc-sel-cnt" class="exc-sel-cnt"></span>
      </div>
    </div>
    <div class="mf">
      <label style="font-size:12px;color:var(--tx2);font-weight:600;margin-bottom:8px;display:block">💬 소명 사유 <span style="color:var(--rd)">*</span></label>
      <div class="exc-reason-btns">
        <button class="exc-rsn-btn vacation" onclick="setExcReason('휴가',this,'vacation')"><span class="exc-rsn-chk">✓</span><span class="exc-rsn-icon">🌴</span><span class="exc-rsn-lbl">휴가</span></button>
        <button class="exc-rsn-btn sick" onclick="setExcReason('병가',this,'sick')"><span class="exc-rsn-chk">✓</span><span class="exc-rsn-icon">🏥</span><span class="exc-rsn-lbl">병가</span></button>
        <button class="exc-rsn-btn business" onclick="setExcReason('출장',this,'business')"><span class="exc-rsn-chk">✓</span><span class="exc-rsn-icon">✈️</span><span class="exc-rsn-lbl">출장</span></button>
      </div>
    </div>
    <div class="mf"><label>신청자</label><input type="text" id="exc-regby" placeholder="이름"></div>
    <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:var(--rs);padding:8px 12px;margin-bottom:12px;font-size:12px;color:#856404">⏳ 제출 후 관리자 승인 전까지 KPI에 반영되지 않습니다.</div>
    <div class="mft"><button class="btn" onclick="closeM('excuse-ov')">취소</button><button class="btn bp" onclick="submitExcuse()">소명 제출</button></div>
  </div>
</div>

<div id="person-ov" class="overlay hide">
  <div class="mbox wide">
    <div class="mttl">접속 현황<button class="mclose" onclick="closeM('person-ov')">✕</button></div>
    <div class="person-info">
      <div class="avatar" id="p-av"></div>
      <div class="person-meta"><div class="person-name" id="p-nm"></div><div class="person-sub" id="p-sub"></div></div>
      <div class="person-kpi"><div class="person-rate" id="p-rt"></div><div class="person-rate-sub" id="p-rt-sub"></div></div>
    </div>
    <div class="cal-legend">
      <span class="cleg"><span class="cleg-dot" style="background:#d4edda;border:1px solid #276740"></span>접속</span>
      <span class="cleg"><span class="cleg-dot" style="background:#f9c0cc;border:1px solid #a0304a"></span>미접속(클릭→소명)</span>
      <span class="cleg"><span class="cleg-dot pend"></span>소명대기</span>
      <span class="cleg"><span class="cleg-dot rej"></span>반려됨(재신청가능)</span>
      <span class="cleg"><span class="cleg-dot" style="background:#d4c5f9;border:1px solid #4a1a9e"></span>면제(승인완료)</span>
      <span class="cleg"><span class="cleg-dot" style="background:#f1f5f9;border:1px solid #94a3b8"></span>🔒 마감</span>
    </div>
    <div id="p-cal" onmouseleave="if(G_calDrag.active)_calDragFinish()"></div>
    <div style="font-size:13px;font-weight:600;margin-bottom:7px;color:var(--tx2)">미접속 날짜</div>
    <div id="p-miss"></div>
  </div>
</div>

<div id="ex-detail-ov" class="overlay hide">
  <div class="mbox" style="width:520px">
    <div class="mttl">소명 상세<button class="mclose" onclick="closeM('ex-detail-ov')">✕</button></div>
    <div style="display:flex;align-items:center;gap:12px;padding:12px 14px;background:var(--sf2);border-radius:var(--rs);margin-bottom:14px">
      <div class="avatar" id="exd-av" style="width:36px;height:36px;font-size:13px"></div>
      <div style="flex:1"><div style="font-size:14px;font-weight:700" id="exd-name"></div><div style="font-size:12px;color:var(--tx2);margin-top:2px" id="exd-meta"></div></div>
      <div id="exd-status-badge"></div>
    </div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
      <span style="font-size:12px;color:var(--tx2);min-width:64px">소명 날짜</span>
      <span style="font-size:15px;font-weight:700;color:var(--rd)" id="exd-date"></span>
      <span style="font-size:12px;color:var(--tx3)" id="exd-dow"></span>
    </div>
    <div style="margin-bottom:12px">
      <div style="font-size:12px;color:var(--tx2);margin-bottom:5px;font-weight:600">소명 사유</div>
      <div id="exd-reason" style="background:var(--sf2);border-radius:var(--rs);padding:11px 13px;font-size:13px;line-height:1.7;white-space:pre-wrap;min-height:60px"></div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px">
      <div style="background:var(--sf2);border-radius:var(--rs);padding:9px 12px"><div style="font-size:11px;color:var(--tx3);margin-bottom:3px">신청자</div><div style="font-size:13px;font-weight:600" id="exd-regby"></div></div>
      <div style="background:var(--sf2);border-radius:var(--rs);padding:9px 12px"><div style="font-size:11px;color:var(--tx3);margin-bottom:3px">신청일시</div><div style="font-size:12px;font-weight:500" id="exd-regat"></div></div>
    </div>
    <div id="exd-history" style="margin-bottom:14px"></div>
    <div class="mft" id="exd-action-btns"></div>
  </div>
</div>

<div id="reject-ov" class="overlay hide">
  <div class="mbox" style="width:440px">
    <div class="mttl">반려 처리<button class="mclose" onclick="closeM('reject-ov')">✕</button></div>
    <div id="rej-info" style="background:var(--sf2);border-radius:var(--rs);padding:9px 12px;margin-bottom:13px;font-size:13px"></div>
    <div class="mf"><label>반려 사유 <span style="color:var(--rd)">*</span></label><textarea id="rej-reason" rows="3" placeholder="예) 증빙 자료 미첨부"></textarea></div>
    <div style="background:#fde8ec;border-radius:var(--rs);padding:8px 11px;margin-bottom:12px;font-size:12px;color:#a0304a">⚠ 반려 시 신청자에게 반려 사유가 표시되며 재소명이 가능합니다.</div>
    <div class="mft"><button class="btn" onclick="closeM('reject-ov')">취소</button><button class="btn brd" onclick="submitReject()">반려 확정</button></div>
  </div>
</div>

<header class="hdr">
  <div class="logo" id="app-logo" onclick="goTab('dash')" title="메인으로">ESH<span id="app-title-sub"> KPI</span></div>
  <div class="tabs">
    <button class="tab on" onclick="goTab('dash')">📊 대시보드</button>
    <button class="tab" onclick="goTab('full')">📋 전체 현황</button>
    <button class="tab" onclick="goTab('indiv')">👤 개별 현황</button>
    <button class="tab" onclick="goTab('excuse')">✅ 사유확인</button>
    <button class="tab" onclick="goTab('ref')">👥 대상자 정보</button>
    <button class="tab" id="admin-tab" style="display:none;color:var(--pu)" onclick="goTab('admin')">⚙ 관리자설정</button>
  </div>
  <div class="hdr-r">
    <span class="dot" id="sdot"></span><span id="stxt">미로드</span>
    <div id="hdr-dz" style="display:flex;align-items:center;gap:6px;padding:4px 10px;border:1.5px dashed var(--bd);border-radius:var(--rs);cursor:pointer;font-size:12px;color:var(--tx2)"
      onclick="document.getElementById('fi').click()"
      ondragover="event.preventDefault();this.style.borderColor='var(--bl)'"
      ondragleave="this.style.borderColor='var(--bd)'"
      ondrop="event.preventDefault();this.style.borderColor='var(--bd)';loadF(event.dataTransfer.files[0])">
      📂 <span id="hdr-dz-txt">xlsx 업로드</span>
    </div>
    <button class="btn bsm" id="admin-lock-btn" style="display:none;color:var(--rd);border-color:var(--rd)" onclick="lockAdmin()">🔒 관리자 종료</button>
    <button class="btn bsm" id="admin-login-btn" style="color:var(--tx2)" onclick="openAdminLogin()">🔑</button>
    <input type="file" id="fi" accept=".xlsx,.xls" style="display:none">
  </div>
</header>
<div class="page on" id="page-dash">
  <div class="notice-bar" id="notice-bar"><span class="notice-ic">📢</span><div class="notice-body"><span class="notice-ttl" id="notice-ttl-display"></span><span class="notice-sep" id="notice-sep-display">|</span><span class="notice-txt" id="notice-txt-display"></span><span class="notice-meta" id="notice-meta-display"></span></div></div>
  <div id="dbody">
    <div class="period-bar">
      <div class="ptype-grp">
        <button class="ptab on" data-pt="week" onclick="setPT(this)">주간</button>
        <button class="ptab" data-pt="month" onclick="setPT(this)">월간</button>
        <button class="ptab" data-pt="quarter" onclick="setPT(this)">분기</button>
        <button class="ptab" data-pt="half" onclick="setPT(this)">반기</button>
        <button class="ptab" data-pt="year" onclick="setPT(this)">연간</button>
        <button class="ptab" data-pt="custom" onclick="setPT(this)">📅 기간지정</button>
      </div>
      <div class="psep"></div>
      <select id="psel" class="psel" onchange="if(!G._pselUpdating)rDash()"></select>
      <div class="cust-row" id="cust-row">
        <span style="font-size:12px;color:var(--tx2)">시작일</span><input type="date" id="cf">
        <span style="font-size:13px;color:var(--tx2)">~</span>
        <span style="font-size:12px;color:var(--tx2)">종료일</span><input type="date" id="ct">
        <button class="btn bsm bp" onclick="recomp()">조회</button>
      </div>
      <div class="psep"></div>
      <select id="f-site" class="psel" onchange="rDash()"><option value="">전체 사업장</option></select>
      <span id="p-today-lbl" style="margin-left:auto;font-size:12px;color:var(--tx2);white-space:nowrap"></span>
    </div>
    <div id="krow" class="krow"></div>
    <div class="dash-row">
      <div class="card" style="overflow:hidden"><div class="cttl">기간별 흐름 <span class="cttl-sub" id="tlbl"></span></div><div id="trend-box" style="max-height:360px;overflow-y:auto"></div></div>
      <div class="card"><div class="cttl">사업장 · 부서별 달성률 <span class="cttl-sub" id="slbl"></span></div><div class="chw" id="site-chw"><canvas id="siteC"></canvas></div><div style="border-top:1px solid var(--bd);margin:8px 0"></div><div class="chw" id="dept-chw"><canvas id="deptC"></canvas></div></div>
    </div>
    <div class="sec-hd"><div class="sec-ttl">사업장별 상세</div><button class="btn bsm" id="snap-btn" onclick="snapDash()" title="대시보드 이미지 저장">📸 화면 캡처</button></div>
    <div id="scards" class="site-cards-wrap"></div>
  </div>
</div>

<div class="page" id="page-full">
  <div class="fb">
    <label>사업장</label><select id="fls" onchange="onFlsChange()"><option value="">전체</option></select>
    <label>부서</label><select id="fld" onchange="rFull()"><option value="">전체</option></select>
    <label>기간</label><select id="flp" onchange="rFull()"><option value="">전체</option></select>
    <label>상태</label>
    <select id="flst" onchange="rFull()"><option value="">전원</option><option value="ok">달성</option><option value="fail">미달성</option><option value="zero">미접속</option></select>
    <input type="text" id="flq" placeholder="성명" style="width:90px" oninput="_rFullD()">
    <div class="fsep"></div>
    <button class="btn bsm" onclick="expCSV()">⬇ 미달성 CSV</button>
    <div style="margin-left:auto;text-align:right;line-height:1.4">
      <div id="flsum-rate" style="font-size:16px;font-weight:800;color:var(--bl)">-</div>
      <div id="flsum" style="font-size:11px;color:var(--tx2)">-</div>
    </div>
  </div>
  <div class="tw"><table>
    <thead><tr>
      <th onclick="srt('site')">사업장 ↕</th><th onclick="srt('dept')">부서명 ↕</th><th>사번</th>
      <th onclick="srt('name')">성명 ↕</th><th onclick="srt('period')">기간 ↕</th>
      <th onclick="srt('cnt')">실접속 ↕</th><th onclick="srt('excCnt')">면제 ↕</th>
      <th onclick="srt('total')">합계 ↕</th><th>목표</th><th onclick="srt('ok')">상태 ↕</th><th>상세</th>
    </tr></thead>
    <tbody id="flbody"></tbody>
  </table></div>
  <div class="pagi" id="flpagi"></div>
</div>

<div class="page" id="page-indiv">
  <div class="fb" style="background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:10px 14px;margin-bottom:14px;flex-wrap:wrap;gap:8px">
    <label style="font-size:12px;color:var(--tx2)">사업장</label>
    <select id="iv-site" onchange="rIndivSite()" style="font-size:13px;padding:4px 9px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"><option value="">선택</option></select>
    <label style="font-size:12px;color:var(--tx2)">부서</label>
    <select id="iv-dept" onchange="rIndivDept()" style="font-size:13px;padding:4px 9px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"><option value="">선택</option></select>
    <label style="font-size:12px;color:var(--tx2)">이름</label>
    <select id="iv-name" style="font-size:13px;padding:4px 9px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"><option value="">선택</option></select>
    <div class="fsep"></div>
    <div class="ptype-grp">
      <button class="ptab on" data-ipt="week" onclick="setIPT(this)">주간</button>
      <button class="ptab" data-ipt="month" onclick="setIPT(this)">월간</button>
      <button class="ptab" data-ipt="quarter" onclick="setIPT(this)">분기</button>
      <button class="ptab" data-ipt="half" onclick="setIPT(this)">반기</button>
      <button class="ptab" data-ipt="year" onclick="setIPT(this)">연간</button>
      <button class="ptab" data-ipt="custom" onclick="setIPT(this)">지정</button>
    </div>
    <select id="iv-period" style="font-size:13px;padding:4px 9px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"></select>
    <div id="iv-cust" style="display:none;align-items:center;gap:6px">
      <input type="date" id="iv-cf" style="font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit">
      <span style="color:var(--tx2)">~</span>
      <input type="date" id="iv-ct" style="font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit">
    </div>
    <button class="btn bp bsm" onclick="runIndiv()">조회</button>
  </div>
  <div id="iv-empty" style="text-align:center;padding:60px 20px;color:var(--tx3)"><div style="font-size:32px;margin-bottom:10px;opacity:.3">👤</div><div>사업장 → 부서 → 이름 선택 후 조회하세요</div></div>
  <div id="iv-result" style="display:none">
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:14px 18px;margin-bottom:12px;display:flex;align-items:center;gap:12px">
      <div class="avatar" id="iv-av"></div>
      <div style="flex:1"><div class="person-name" id="iv-nm"></div><div class="person-sub" id="iv-sub"></div></div>
      <div style="text-align:right"><div class="person-rate" id="iv-rt"></div><div class="person-rate-sub" id="iv-rt-sub"></div></div>
    </div>
    <div class="cal-legend" style="margin-bottom:8px">
      <span class="cleg"><span class="cleg-dot" style="background:#d4edda;border:1px solid #276740"></span>접속</span>
      <span class="cleg"><span class="cleg-dot" style="background:#f9c0cc;border:1px solid #a0304a"></span>미접속(클릭→소명)</span>
      <span class="cleg"><span class="cleg-dot pend"></span>소명대기</span>
      <span class="cleg"><span class="cleg-dot rej"></span>반려됨(재신청가능)</span>
      <span class="cleg"><span class="cleg-dot" style="background:#d4c5f9;border:1px solid #4a1a9e"></span>면제(승인완료)</span>
    </div>
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:16px;margin-bottom:12px"><div id="iv-cal"></div></div>
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:14px 16px">
      <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:var(--tx2)">미접속 날짜</div>
      <div id="iv-miss"></div>
    </div>
  </div>
</div>

<div class="page" id="page-excuse">
  <div id="ex-page-lock" style="display:none;position:absolute;inset:0;z-index:200;background:rgba(243,244,246,.97);flex-direction:column;align-items:center;justify-content:center;gap:14px;min-height:340px">
    <div style="font-size:36px">🔒</div>
    <div style="font-size:16px;font-weight:700">사유확인 페이지</div>
    <div style="font-size:13px;color:var(--tx2)">접근 비밀번호를 입력하세요</div>
    <div style="display:flex;gap:8px;align-items:center">
      <input type="password" id="ex-page-pw-inp" placeholder="비밀번호" style="width:160px;padding:7px 11px;border-radius:var(--rs);border:1px solid var(--bd);font-size:14px;font-family:inherit" onkeydown="if(event.key==='Enter')checkExPagePw()">
      <button class="btn bp" onclick="checkExPagePw()">확인</button>
    </div>
    <div id="ex-page-pw-hint" style="font-size:12px;color:var(--rd);height:16px"></div>
  </div>
  <div id="ex-page-content" style="display:none">
    <div class="fb" style="margin-bottom:12px;background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:9px 13px">
      <label>사업장</label><select id="exs" onchange="rExcuse()"><option value="">전체</option></select>
      <div class="psep"></div>
      <label>기간</label>
      <select id="ex-period-type" onchange="onExPeriodTypeChange()" style="font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit">
        <option value="">전체</option><option value="week">주간</option><option value="month">월간</option><option value="custom">직접입력</option>
      </select>
      <select id="ex-period-sel" style="display:none;font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit" onchange="rExcuse()"></select>
      <div id="ex-period-custom" style="display:none;align-items:center;gap:5px">
        <input type="date" id="ex-date-from" style="font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit">
        <span style="color:var(--tx2)">~</span>
        <input type="date" id="ex-date-to" style="font-size:13px;padding:4px 8px;border-radius:var(--rs);border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit">
        <button class="btn bsm bp" onclick="rExcuse()">조회</button>
      </div>
      <div class="psep"></div>
      <input type="text" id="exq" placeholder="성명·사번" style="width:120px" oninput="_rExcD()">
      <span class="tag" id="exsum" style="margin-left:auto">-</span>
    </div>
    <div id="ex-site-sections"></div>
  </div>
</div>

<div class="page" id="page-ref">
  <div class="ref-banner" id="ref-banner"><span style="font-size:16px">🔒</span><div>대상자 정보는 <strong>읽기 전용</strong>입니다.<button class="btn bsm" style="margin-left:10px" onclick="openM('pw-ov')">관리자 인증</button></div></div>
  <div class="fb" id="ref-edit-tb" style="display:none;margin-bottom:10px">
    <button class="btn bp bsm" onclick="addRow()">+ 행 추가</button>
    <button class="btn bg bsm" onclick="push2Sheets()">☁ Sheets 저장</button>
    <button class="btn bsm" onclick="pullRef()">↓ Sheets 불러오기</button>
    <button class="btn bsm brd" onclick="lockRef()">🔒 편집 종료</button>
  </div>
  <div class="fb">
    <input type="text" id="mgq" placeholder="검색" style="width:120px" oninput="_rRefD()">
    <select id="mgsite" onchange="onMgsiteChange()" style="font-size:13px;padding:4px 8px;border-radius:6px;border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"><option value="">전체</option></select>
    <select id="mgdept" onchange="rRef()" style="font-size:13px;padding:4px 8px;border-radius:6px;border:1px solid var(--bd);background:var(--sf);color:var(--tx);font-family:inherit"><option value="">전체 부서</option></select>
    <span class="tag" id="mgcnt">0명</span>
    <div id="syncinfo" style="font-size:12px;color:var(--tx2);margin-left:auto;display:flex;align-items:center;gap:5px"></div>
  </div>
  <div class="tw"><table>
    <thead><tr>
      <th style="width:32px">#</th>
      <th onclick="srtRef('site')" style="cursor:pointer">사업장 ↕</th>
      <th onclick="srtRef('dept')" style="cursor:pointer">부서명 ↕</th>
      <th onclick="srtRef('empno')" style="cursor:pointer">사번 ↕</th>
      <th onclick="srtRef('name')" style="cursor:pointer">성명 ↕</th>
      <th>비고</th><th id="del-th" style="width:34px;display:none"></th>
    </tr></thead>
    <tbody id="mgbody"></tbody>
  </table></div>
  <div class="pagi" id="mgpagi"></div>
</div>

<div class="page" id="page-admin">
  <div style="max-width:800px;margin:0 auto">
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:20px 24px;margin-bottom:14px">
      <div style="font-size:15px;font-weight:700;margin-bottom:18px;color:var(--bl)">⚙ 관리자 설정</div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">📋 대시보드 타이틀</div>
        <div class="mf" style="margin-bottom:10px"><label>메인 타이틀</label><input type="text" id="adm-title-main" placeholder="예) ESH"></div>
        <div class="mf" style="margin-bottom:12px"><label>서브 타이틀</label><input type="text" id="adm-title-sub" placeholder="예)  KPI"></div>
        <div style="font-size:12px;color:var(--tx3);margin-bottom:12px">미리보기: <strong id="adm-title-preview" style="color:var(--bl)"></strong></div>
        <button class="btn bp bsm" onclick="admSaveTitle()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">🎯 KPI 목표 설정</div>
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
          <label style="font-size:12px;color:var(--tx2);min-width:120px">주당 목표 접속 횟수</label>
          <input type="number" id="adm-kpi-week" min="1" max="20" value="4" style="width:80px;padding:5px 9px;border-radius:var(--rs);border:1px solid var(--bd);font-size:13px;font-family:inherit;background:var(--sf);color:var(--tx)">
          <span style="font-size:12px;color:var(--tx2)">회 / 주</span>
        </div>
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
          <label style="font-size:12px;color:var(--tx2);min-width:120px">KPI 적용 시작일</label>
          <input type="date" id="adm-kpi-start" style="padding:5px 9px;border-radius:var(--rs);border:1px solid var(--bd);font-size:13px;font-family:inherit;background:var(--sf);color:var(--tx)">
          <button class="btn bsm brd" onclick="document.getElementById('adm-kpi-start').value='';admSaveKPI();">✕ 초기화</button>
        </div>
        <div style="font-size:12px;color:var(--tx3);margin-bottom:12px">시작일 이전 접속 데이터는 집계에서 제외됩니다.</div>
        <button class="btn bp bsm" onclick="admSaveKPI()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">🏭 사업장 표시 순서</div>
        <div class="mf" style="margin-bottom:12px"><label>사업장명 (한 줄에 하나씩)</label><textarea id="adm-site-order" rows="8"></textarea></div>
        <button class="btn bp bsm" onclick="admSaveSiteOrder()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">🏷 부서 분류 키워드</div>
        <div class="mf" style="margin-bottom:12px"><label>부서 유형 키워드 (한 줄에 하나씩)</label><textarea id="adm-dept-order" rows="5"></textarea></div>
        <button class="btn bp bsm" onclick="admSaveDeptOrder()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">☁ Google Sheets 연결</div>
        <div class="mf" style="margin-bottom:12px"><label>Apps Script 웹앱 URL</label><input type="text" id="adm-url"><div class="mhelp">배포 → 웹 앱으로 배포 → 액세스: 모든 사용자</div></div>
        <div style="display:flex;gap:7px"><button class="btn bp bsm" onclick="admSaveUrl()">저장 및 연결</button><button class="btn bsm" onclick="admTestUrl()">연결 테스트</button></div>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:4px">🔐 사유확인 페이지 접근 비밀번호</div>
        <div style="font-size:12px;color:var(--tx3);margin-bottom:12px">설정 시 사유확인 탭 진입 시 비밀번호 입력 필요.</div>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
          <input type="password" id="adm-ex-page-pw" placeholder="비워두면 잠금 없음" style="flex:1;padding:6px 9px;border-radius:var(--rs);border:1px solid var(--bd);font-size:13px;font-family:inherit;background:var(--sf);color:var(--tx)">
          <button class="btn bsm" onclick="document.getElementById('adm-ex-page-pw').type=document.getElementById('adm-ex-page-pw').type==='password'?'text':'password'">👁</button>
        </div>
        <button class="btn bp bsm" onclick="admSaveExPagePw()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:4px">🔑 사업장별 소명 비밀번호</div>
        <div style="font-size:12px;color:var(--tx3);margin-bottom:12px">비밀번호를 설정한 사업장은 소명 페이지에서 인증 후 승인/반려 가능합니다.</div>
        <div id="adm-site-pw-list"></div>
        <button class="btn bsm bp" style="margin-top:8px" onclick="admSaveSitePws()">저장</button>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">📢 공지사항</div>
        <div class="mf" style="margin-bottom:10px"><label>제목</label><input type="text" id="adm-notice-ttl"></div>
        <div class="mf" style="margin-bottom:10px"><label>내용</label><textarea id="adm-notice-txt" rows="4"></textarea></div>
        <div style="display:flex;gap:7px;align-items:center">
          <button class="btn bp bsm" onclick="admSaveNotice()">공지 게시</button>
          <button class="btn bsm brd" onclick="admClearNotice()">공지 삭제</button>
          <span id="adm-notice-status" style="font-size:12px;color:var(--tx3)"></span>
        </div>
      </div>
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px">
      <div style="border:1px solid var(--bd);border-radius:var(--rs);padding:16px 18px;margin-bottom:14px">
        <div style="font-size:13px;font-weight:700;margin-bottom:4px">📅 월별 소명 마감 처리</div>
        <div style="font-size:12px;color:var(--tx3);margin-bottom:12px">마감된 달은 소명 입력이 차단됩니다. 버튼을 클릭해 마감/해제를 토글하세요.</div>
        <div id="adm-close-month-grid" class="close-month-grid"></div>
        <div style="display:flex;gap:8px;margin-top:12px;align-items:center">
          <button class="btn bp bsm" onclick="admSaveClosedMonths()">저장</button>
          <button class="btn bsm brd" onclick="admClearClosedMonths()">전체 해제</button>
          <span id="adm-close-status" style="font-size:12px;color:var(--tx3)"></span>
        </div>
      </div>
        <div style="font-size:13px;font-weight:700;margin-bottom:12px">🔐 관리자 비밀번호 변경</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
          <div class="mf" style="margin-bottom:0"><label>새 비밀번호</label><input type="password" id="adm-pw1"></div>
          <div class="mf" style="margin-bottom:0"><label>비밀번호 확인</label><input type="password" id="adm-pw2"></div>
        </div>
        <button class="btn bsm" style="color:var(--rd);border-color:var(--rd)" onclick="admChangePw()">비밀번호 변경</button>
      </div>
    </div>
  </div>
</div>

<div id="toast"></div>
<script>
let KPI_WEEK=4,ADMIN_PW='1234',KPI_START_DATE='';
let CLOSED_MONTHS=new Set(); // 마감된 월 Set<'YYYY-MM'>
function isMonthClosed(dt){return CLOSED_MONTHS.has(String(dt).slice(0,7));}
let SITE_ORDER=['중부권역-평택','중부권역-아산','동부권역-경산','동부권역-영동','청양공장','상하공장','광주공장','MIC'];
let DEPT_ORDER=['생산','품질','물류','공무','지원'];

/* ── 소명 모달 상태 ── */
let G_exc={selectedDates:new Set(),reason:'',isDragging:false,dragStartEl:null,dragMode:'add'};
function setExcReason(reason,btn){G_exc.reason=reason;document.querySelectorAll('.exc-rsn-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');}
function excDragStart(e,el){e.preventDefault();G_exc.isDragging=true;G_exc.dragStartEl=el;G_exc.dragMode=G_exc.selectedDates.has(el.dataset.date)?'remove':'add';excToggleWeekCell(el,G_exc.dragMode==='add');}
function excDragEnter(el){if(!G_exc.isDragging||!el.dataset.date)return;excToggleWeekCell(el,G_exc.dragMode==='add');}
function excDragEnd(){G_exc.isDragging=false;}
function excToggleWeekCell(el,forceState){const date=el.dataset.date;if(!date)return;const should=forceState!==undefined?forceState:!G_exc.selectedDates.has(date);if(should){G_exc.selectedDates.add(date);el.classList.add('selected');}else{G_exc.selectedDates.delete(date);el.classList.remove('selected');}updateExcSelCnt();}
function excToggleDate(el,forceState){excToggleWeekCell(el,forceState);}
function excSelectAll(){document.querySelectorAll('#exc-date-list .exc-day-cell.miss').forEach(el=>excToggleWeekCell(el,true));}
function excClearAll(){document.querySelectorAll('#exc-date-list .exc-day-cell.miss').forEach(el=>excToggleWeekCell(el,false));}
function updateExcSelCnt(){const cnt=G_exc.selectedDates.size;const el=document.getElementById('exc-sel-cnt');if(!el)return;if(cnt>0){el.textContent=cnt+'일 선택됨';el.style.display='inline-flex';}else el.style.display='none';}

function openExcuseM(empno,date,name,site,dept,weekCnt,preSelectedDates,dateFrom,dateTo){
  G.excuseTarget={empno,date,name,site,dept};
  const ld=new Set([...G.rawLog.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const ed=new Set([...G.excuses.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const pd=new Set([...G.pendingExcuses.keys()].filter(k=>k.startsWith(empno+'|')&&G.pendingExcuses.get(k)._status==='pending').map(k=>k.split('|')[1]));
  const rejMap=new Map([...G.pendingExcuses.entries()].filter(([k,v])=>k.startsWith(empno+'|')&&v._status==='rejected').map(([k,v])=>[k.split('|')[1],v]));
  /* 기간 범위: 명시적으로 넘어온 경우 그것만, 아니면 전체 선택 기간 */
  const allD=new Set();
  if(dateFrom&&dateTo){
    let d=new Date(dateFrom);while(fmtD(d)<=dateTo){allD.add(fmtD(d));d.setDate(d.getDate()+1);}
  }else{
    const aps=getAPs();
    for(const p of aps){let d=new Date(p.from);while(fmtD(d)<=p.to){allD.add(fmtD(d));d.setDate(d.getDate()+1);}}
  }
  const today=fmtD(new Date());
  let missDates=[...allD].filter(dt=>dt<=today&&!ld.has(dt)&&!ed.has(dt)&&!pd.has(dt)).sort();
  /* ★ 기간 범위 내 반려된 날짜만 추가 */
  const rejDates=[...rejMap.keys()].filter(rd=>allD.has(rd));
  for(const rd of rejDates){if(!missDates.includes(rd))missDates.push(rd);}
  /* ★ date 파라미터도 allD 내일 때만 추가 */
  if(allD.has(date)&&!missDates.includes(date))missDates.push(date);
  missDates=missDates.filter((v,i,a)=>a.indexOf(v)===i).sort();
  /* 마감월 날짜 제거 */
  const blockedByClose=missDates.filter(d=>isMonthClosed(d));
  missDates=missDates.filter(d=>!isMonthClosed(d));
  const _missSet=new Set(missDates);
  /* ★ preSelectedDates도 missDate 범위 내만 허용 */
  G_exc.selectedDates=preSelectedDates?new Set([...preSelectedDates].filter(d=>_missSet.has(d)&&!isMonthClosed(d))):new Set(missDates.length?[date].filter(d=>_missSet.has(d)&&!isMonthClosed(d)):[]);
  /* 마감월 배너 */
  const _prevBanner=document.getElementById('exc-close-banner');if(_prevBanner)_prevBanner.remove();
  if(blockedByClose.length){const _b=document.createElement('div');_b.id='exc-close-banner';_b.className='locked-month-banner';_b.innerHTML='🔒 <strong>마감된 달</strong>('+[...new Set(blockedByClose.map(d=>d.slice(0,7)))].join(', ')+')의 날짜는 소명 입력이 차단됩니다.';document.getElementById('exc-person-info').insertAdjacentElement('afterend',_b);}
  /* 초기화 */
  G_exc.reason='';G_exc.isDragging=false;G_exc.dragStartEl=null;
  document.getElementById('exc-person-info').innerHTML=
    '<div style="display:flex;align-items:center;gap:10px">'+
    '<div style="width:36px;height:36px;border-radius:50%;background:rgba(255,255,255,.2);display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:800;color:#fff;flex-shrink:0">'+esc(name[0])+'</div>'+
    '<div><div style="font-size:14px;font-weight:700;color:#fff">'+esc(name)+'</div>'+
    '<div style="font-size:12px;color:rgba(255,255,255,.7);margin-top:2px">'+esc(site)+' · '+esc(dept)+' · '+empno+'</div></div></div>';
  const DOW=['일','월','화','수','목','금','토'];
  const listEl=document.getElementById('exc-date-list');
  if(!missDates.length){listEl.innerHTML='<div style="color:var(--tx3);text-align:center;padding:14px;font-size:13px">소명 가능한 날짜가 없습니다</div>';}
  else{
    /* 미접속 날짜가 포함된 주차 수집 */
    const weekMap=new Map();
    for(const dt of missDates){
      const ws=wkStart(new Date(dt));const wk=isoWk(ws);
      if(!weekMap.has(wk)){const we=new Date(ws);we.setDate(ws.getDate()+6);weekMap.set(wk,{ws:fmtD(ws),we:fmtD(we),missDates:[]});}
      weekMap.get(wk).missDates.push(dt);
    }
    const today=fmtD(new Date());
    listEl.innerHTML=[...weekMap.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([wk,{ws,we,missDates:wMiss}])=>{
      /* 주 레이블 */
      const wsD=new Date(ws);const mon=wsD.getMonth()+1,day=wsD.getDate();
      const weD=new Date(we);const eDay=weD.getDate();
      const allDone=wMiss.every(d=>G_exc.selectedDates.has(d));
      const hdrBadge=wMiss.length===0?`<span class="wh-all">완료</span>`:`<span class="wh-miss">미접속 ${wMiss.length}일</span>`;
      /* 7일 셀 생성 */
      const cells=[];
      for(let i=0;i<7;i++){
        const d=new Date(wsD);d.setDate(wsD.getDate()+i);const dt=fmtD(d);
        const dow=d.getDay();const isWk=[0,6].includes(dow);const isFut=dt>today;
        const isLog=ld.has(dt),isExc=ed.has(dt)&&!isLog,isPend=pd.has(dt)&&!isLog&&!isExc,isRej=rejDates.includes(dt)&&!isLog&&!isExc&&!isPend;
        const isMiss=missDates.includes(dt);const isSel=G_exc.selectedDates.has(dt);
        let cls='exc-day-cell'+(isWk?' wknd':'');
        let lbl='',extra='';
        if(isFut){cls+=' future';lbl='';}
        else if(isLog){cls+=' ok';lbl='접속';}
        else if(isExc){cls+=' exc';lbl='면제';}
        else if(isPend){cls+=' pend';lbl='대기';}
        else if(isRej){cls+=' rej-d';lbl='반려';extra=`onclick="excToggleWeekCell(this,'${dt}')"`;if(isSel)cls+=' selected';}
        else if(isMiss){if(isMonthClosed(dt)){cls+=' miss locked';lbl='🔒마감';}else{cls+=' miss'+(isSel?' selected':'');extra=`onmousedown="excDragStart(event,this)" onmouseenter="excDragEnter(this)"`;extra+=` data-date="${dt}"`;lbl='미접속';}}
        else{cls+=' out';}
        cells.push(`<div class="${cls}" ${extra} title="${dt}"><span class="exc-day-dow">${DOW[dow]}</span><span class="exc-day-num">${d.getDate()}</span>${lbl?`<span class="exc-day-lbl">${lbl}</span>`:''}</div>`);
      }
      return `<div class="exc-week-group">
        <div class="exc-week-hdr"><span>${mon}/${day} ~ ${eDay}</span>${hdrBadge}</div>
        <div class="exc-week-grid" onmouseleave="excDragEnd()" onmouseup="excDragEnd()">${cells.join('')}</div>
      </div>`;
    }).join('');
  }
  document.querySelectorAll('.exc-rsn-btn').forEach(b=>b.classList.remove('active'));
  const exEntry=G.pendingExcuses.get(empno+'|'+date)||G.excuses.get(empno+'|'+date);
  if(exEntry&&exEntry.reason){const rmap={휴가:'vacation',병가:'sick',출장:'business'};if(rmap[exEntry.reason]){G_exc.reason=exEntry.reason;document.querySelectorAll('.exc-rsn-btn').forEach(b=>{if(b.classList.contains(rmap[exEntry.reason]))b.classList.add('active');});}}
  document.getElementById('exc-regby').value=exEntry?(exEntry.regBy||''):(G.lastRegBy||'');
  updateExcSelCnt();closeM('person-ov');openM('excuse-ov');
}

async function submitExcuse(){
  if(G_exc.selectedDates.size===0){toast('날짜를 선택해주세요');return;}
  if(!G_exc.reason){toast('소명 사유를 선택해주세요');return;}
  const closedDates=[...G_exc.selectedDates].filter(d=>isMonthClosed(d));
  if(closedDates.length){toast('⚠ 마감된 달의 날짜는 제출할 수 없습니다: '+closedDates.join(', '));return;}
  const regBy=document.getElementById('exc-regby').value.trim();
  const t=G.excuseTarget;G.lastRegBy=regBy;
  const dates=[...G_exc.selectedDates].sort();
  const now=new Date().toLocaleString('ko-KR');let fail=false;
  for(const date of dates){
    const key=t.empno+'|'+date;
    const obj={key,empno:t.empno,date,name:t.name,site:t.site,dept:t.dept,reason:G_exc.reason,regBy,regAt:now,_status:'pending'};
    G.pendingExcuses.set(key,obj);
    if(G.url){const res=await api(null,{action:'saveExcuse',data:obj});if(!res||res.error)fail=true;}
  }
  closeM('excuse-ov');
  toast(fail?'✓ '+dates.length+'일 제출 (Sheets 일부 실패)':'✓ '+dates.length+'일 소명 제출 완료 — 관리자 승인 대기 중');
  if(G.drill&&G.drill._iv){runIndiv();}else if(G.drill){openDrill(G.drill.empno);}
}

async function delExcuse(key){
  if(!confirm('소명을 삭제할까요?'))return;
  G.excuses.delete(key);G.pendingExcuses.delete(key);
  if(G.url){const d=await api(null,{action:'delExcuse',key});toast(d&&!d.error?'✓ 삭제 완료':'⚠ Sheets 삭제 실패');}else{toast('✓ 삭제');}
  recomp();if(G.drill)openDrill(G.drill.empno);rExcuse();
}

function normalizeDate(s){if(!s)return '';s=String(s).trim();if(/^\d{4}-\d{2}-\d{2}$/.test(s))return s;try{const d=new Date(s);if(!isNaN(d.getTime()))return fmtD(d);}catch(e){}return '';}
function fmtDateKo(d){if(!d)return'-';const s=normalizeDate(String(d));if(!s)return String(d);const dow=['일','월','화','수','목','금','토'][new Date(s).getDay()];return s+' ('+dow+'요일)';}
function getKPI(pt,from,to){if(pt==='week')return KPI_WEEK;if(pt==='month'){const days=(new Date(to)-new Date(from))/864e5+1;return Math.round(days/7)*KPI_WEEK;}if(pt==='quarter')return 13*KPI_WEEK;if(pt==='half')return 26*KPI_WEEK;if(pt==='year')return 52*KPI_WEEK;const days=(new Date(to)-new Date(from))/864e5+1;return Math.max(KPI_WEEK,Math.round(days/7)*KPI_WEEK);}
function deptType(d){for(const t of DEPT_ORDER)if(d.includes(t))return t;return '기타';}
function siteIdx(s){const i=SITE_ORDER.indexOf(s);return i<0?99:i;}
function deptIdx(d){const i=DEPT_ORDER.indexOf(deptType(d));return i<0?99:i;}

let G={ref:[],excuses:new Map(),pendingExcuses:new Map(),rawLog:new Map(),periodType:'week',allPeriods:[],result:[],fullSort:{k:'cnt',a:true},pages:{fl:1,mg:1},ps:50,charts:{},url:'https://script.google.com/macros/s/AKfycbwbSeb1s_mXKzcLqbtJHG-T_EcPIXCRagW5aTEQGCyZmkeZcMLeOQ0DiVi3hV1XWyG0WA/exec',sync:'',logCache:null,drill:null,excuseTarget:null,adminMode:false};
// 사유확인 탭 상태 관리
let G_exTab={}; // site -> 'all'|'pending'|'approved'|'rejected'

function goTab(t){
  document.querySelectorAll('.tab').forEach(btn=>{const fn=btn.getAttribute('onclick')||'';const m=fn.match(/goTab\('(\w+)'\)/);if(m)btn.classList.toggle('on',m[1]===t);});
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('on'));
  document.getElementById('page-'+t).classList.add('on');
  if(t==='full')rFull();if(t==='excuse')initExcusePage();if(t==='ref')rRef();if(t==='indiv')initIndiv();if(t==='admin')initAdminPage();if(t==='dash'&&G.rawLog.size)rDash();
}
function openM(id){document.getElementById(id).classList.remove('hide');}
function closeM(id){document.getElementById(id).classList.add('hide');}
document.addEventListener('keydown',e=>{if(e.key==='Escape')e.stopImmediatePropagation();},{capture:true});

/* ══ 보안: 우클릭 차단 + 개발자도구 단축키 차단 ══ */
document.addEventListener('contextmenu',e=>{e.preventDefault();e.stopPropagation();return false;},{capture:true});
document.oncontextmenu=()=>false;
document.addEventListener('keydown',e=>{
  // F12
  if(e.key==='F12'){e.preventDefault();return false;}
  // Ctrl+Shift+I / Ctrl+Shift+J / Ctrl+Shift+C (개발자도구)
  if(e.ctrlKey&&e.shiftKey&&['I','i','J','j','C','c'].includes(e.key)){e.preventDefault();return false;}
  // Ctrl+U (소스보기)
  if(e.ctrlKey&&(e.key==='u'||e.key==='U')){e.preventDefault();return false;}
  // Ctrl+S (저장)
  if(e.ctrlKey&&(e.key==='s'||e.key==='S')){e.preventDefault();return false;}
},{capture:true});
/* 개발자도구 감지 제거 (false positive로 인한 오작동 방지) */

/* ══ 비밀번호 잠금 (3회 실패 → 30분 잠금) ══ */
const PW_LOCK={
  MAX:3, LOCK_MS:30*60*1000,
  key(id){return 'esh_pwlock_'+id;},
  getState(id){try{const s=JSON.parse(localStorage.getItem(this.key(id))||'{}');return s;}catch(e){return {};}},
  fail(id){
    const s=this.getState(id);
    const attempts=(s.attempts||0)+1;
    const lockedUntil=attempts>=this.MAX?Date.now()+this.LOCK_MS:0;
    localStorage.setItem(this.key(id),JSON.stringify({attempts,lockedUntil}));
    return{attempts,lockedUntil};
  },
  success(id){localStorage.removeItem(this.key(id));},
  isLocked(id){
    const s=this.getState(id);
    if(s.lockedUntil&&Date.now()<s.lockedUntil)return s.lockedUntil;
    if(s.lockedUntil&&Date.now()>=s.lockedUntil)localStorage.removeItem(this.key(id));
    return false;
  },
  remainMsg(until){const s=Math.round((until-Date.now())/1000);return s>60?Math.ceil(s/60)+'분':''+s+'초';}
};
function applyTitle(main,sub){document.getElementById('app-logo').childNodes[0].textContent=main;document.getElementById('app-title-sub').textContent=sub;document.title=main+(sub||'');}

function checkPw(){
  const locked=PW_LOCK.isLocked('admin');
  if(locked){document.getElementById('pw-hint').textContent='🔒 잠금 중 — '+PW_LOCK.remainMsg(locked)+' 후 재시도';return;}
  const v=document.getElementById('pw-inp').value;
  if(v===ADMIN_PW){
    PW_LOCK.success('admin');
    G.adminMode=true;closeM('pw-ov');document.getElementById('pw-inp').value='';document.getElementById('pw-hint').textContent='';
    document.getElementById('ref-edit-tb').style.display='flex';document.getElementById('ref-banner').style.display='none';
    document.getElementById('del-th').style.display='';document.getElementById('admin-tab').style.display='';
    document.getElementById('admin-lock-btn').style.display='inline-flex';document.getElementById('admin-login-btn').style.display='none';
    exPageUnlocked=true;toast('✓ 관리자 모드 활성화');rRef();
  }else{
    const{attempts,lockedUntil}=PW_LOCK.fail('admin');
    const remain=PW_LOCK.MAX-attempts;
    if(lockedUntil){document.getElementById('pw-hint').textContent='🔒 3회 실패 — 30분간 잠금됩니다';}
    else{document.getElementById('pw-hint').textContent='비밀번호 오류 (남은 시도: '+remain+'회)';}
    document.getElementById('pw-inp').value='';
  }
}
function lockAdmin(){
  G.adminMode=false;document.getElementById('ref-edit-tb').style.display='none';document.getElementById('ref-banner').style.display='flex';
  document.getElementById('del-th').style.display='none';document.getElementById('admin-tab').style.display='none';
  document.getElementById('admin-lock-btn').style.display='none';document.getElementById('admin-login-btn').style.display='inline-flex';
  exPageUnlocked=false;if(document.getElementById('page-admin').classList.contains('on'))goTab('dash');
  toast('🔒 관리자 모드 종료');rRef();
}
function lockRef(){lockAdmin();}
function openAdminLogin(){openM('pw-ov');G._pwCallback=null;}

async function api(params,body){
  if(!G.url)return null;
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),15000); // 15초 타임아웃
  try{
    let r;
    if(body){r=await fetch(G.url,{method:'POST',body:JSON.stringify(body),signal:controller.signal});}
    else{r=await fetch(G.url+'?'+new URLSearchParams(params),{signal:controller.signal});}
    clearTimeout(timer);
    return await r.json();
  }catch(e){
    clearTimeout(timer);
    if(e.name==='AbortError'){console.warn('API 타임아웃');toast('⚠ 연결 시간 초과 — 재시도 중...');}
    else{console.error('API Error:',e.message);}
    return null;
  }
}
function saveConn(){const u=document.getElementById('murl').value.trim();if(!u){toast('URL 입력');return;}G.url=u;closeM('conn-ov');pullAll();}

/* ══ localStorage 영구 캐시 ══
   키: esh_log_cache  값: {ts, from, kpiStart, keys:[]}
   TTL: 6시간 / xlsx 업로드 or KPI 시작일 변경 시 무효화                */
const LC_KEY='esh_log_cache';const LC_TTL=6*60*60*1000;
function lcSave(keys,fromDate){
  try{localStorage.setItem(LC_KEY,JSON.stringify({ts:Date.now(),from:fromDate,kpiStart:KPI_START_DATE,keys}));}catch(e){console.warn('lcSave failed',e.message);}
}
function lcLoad(fromDate){
  try{
    const raw=localStorage.getItem(LC_KEY);if(!raw)return null;
    const c=JSON.parse(raw);
    if(!c||!Array.isArray(c.keys)){lcClear();return null;} // 손상된 캐시 자동 삭제
    if(c.from!==fromDate||c.kpiStart!==KPI_START_DATE)return null;
    if(Date.now()-c.ts>LC_TTL)return null;
    return{keys:c.keys,ts:c.ts};
  }catch(e){lcClear();return null;}
}
function lcClear(){try{localStorage.removeItem(LC_KEY);}catch(e){}}
function lcAge(ts){const s=Math.round((Date.now()-ts)/1000);return s<60?s+'초':s<3600?Math.round(s/60)+'분':Math.round(s/3600)+'시간';}

async function pullAll(){
  if(!G.url){openM('conn-ov');return;}
  const fromDate=fmtD(new Date(new Date().setMonth(new Date().getMonth()-13)));
  const cached=lcLoad(fromDate);
  if(cached&&cached.keys.length){G.rawLog=new Map();for(const k of cached.keys)G.rawLog.set(k,true);}
  setSt('warn','로드 중...');
  try{
    if(cached&&cached.keys.length){
      const[,rd,ed]=await Promise.all([loadSettingsFromSheets(),api({action:'getRef'}),api({action:'getExcuses'})]);
      if(rd&&!rd.error)_applyRef(rd);
      if(ed&&!ed.error)_applyExcuses(ed);
    }else{
      const[,rd,ed,ld]=await Promise.all([loadSettingsFromSheets(),api({action:'getRef'}),api({action:'getExcuses'}),api({action:'getLogs',from:fromDate})]);
      if(rd&&!rd.error)_applyRef(rd);
      if(ed&&!ed.error)_applyExcuses(ed);
      if(ld&&Array.isArray(ld)&&ld.length){_applyLogs(ld);lcSave([...G.rawLog.keys()],fromDate);}
    }
    if(G.rawLog.size&&G.ref.length)recomp();
    if(cached&&cached.keys.length)_bgRefreshLog(fromDate,cached.ts);
  }catch(err){
    console.error('pullAll error:',err);
  }finally{
    /* 어떤 상황에도 반드시 로딩 닫기 */
    setSt('ok',G.ref.length+'명');
    document.getElementById('loading-ov').classList.add('hide');
    console.log('[pullAll] done, ref:',G.ref.length,'log:',G.rawLog.size);
  }
}

function _applySettings(d){
  if(!d||d.error)return;
  const kw=d.esh_kpi_week;if(kw&&!isNaN(Number(kw)))KPI_WEEK=Number(kw);
  const ks=d.esh_kpi_start;if(ks!==undefined){const n=normalizeDate(ks);if(n!==KPI_START_DATE){lcClear();G.logCache=null;}KPI_START_DATE=n;}
  const so=d.esh_site_order;if(so){try{SITE_ORDER=JSON.parse(so);}catch(e){}}
  const doo=d.esh_dept_order;if(doo){try{DEPT_ORDER=JSON.parse(doo);}catch(e){}}
  const ap=d.esh_admin_pw;if(ap)ADMIN_PW=ap;
  const ep=d.esh_ex_page_pw;if(ep!==undefined)EXCUSE_PAGE_PW=ep;
  const sp=d.esh_site_pws;if(sp){try{SITE_PWS=JSON.parse(sp);}catch(e){}}
  const tm=d.esh_title_main,ts=d.esh_title_sub;
  if(tm)G.titleMain=tm;if(ts!==undefined)G.titleSub=ts;
  if(tm||ts)applyTitle(G.titleMain||'ESH',G.titleSub!=null?G.titleSub:' KPI');
  const cm=d.esh_closed_months;if(cm){try{CLOSED_MONTHS=new Set(JSON.parse(cm));}catch(e){}}
  const notice=d.esh_notice;if(notice){try{const n=JSON.parse(notice);if(n.txt){G.notice=n;loadNotice();}}catch(e){}}else G.notice=null;
}
function _applyRef(rd){if(!rd||rd.error)return;G.ref=rd.map((r,i)=>({...r,id:r.empno+'_'+i}));}
function _applyExcuses(ed){
  if(!ed||!Array.isArray(ed))return;
  G.excuses=new Map();G.pendingExcuses=new Map();
  for(const e of ed){const n=normalizeDate(String(e.date||''));const ne={...e,date:n||e.date};if(ne._status==='approved')G.excuses.set(ne.key,ne);else G.pendingExcuses.set(ne.key,ne);}
}
function _applyLogs(logRes){
  G.rawLog=new Map();for(const r of logRes)G.rawLog.set(r.empno+'|'+r.date,true);
}

async function _fetchFullLog(fromDate){
  const logRes=await api({action:'getLogs',from:fromDate});
  if(logRes&&Array.isArray(logRes)&&logRes.length){
    _applyLogs(logRes);lcSave([...G.rawLog.keys()],fromDate);
    const dz=document.getElementById('hdr-dz-txt');if(dz)dz.textContent='✓ '+G.rawLog.size+'건';
    if(G.ref.length)recomp();
    setSt('ok',G.ref.length+'명');
  }else{
    setSt('ok',G.ref.length?G.ref.length+'명 (로그 없음)':'연결 오류');
    toast('⚠ 접속로그를 불러오지 못했습니다');
  }
}

async function _bgRefreshLog(fromDate,cachedTs){
  /* 마지막 캐시 이후 날짜만 요청 */
  const sinceDate=fmtD(new Date(cachedTs-24*60*60*1000));/* 하루 여유 */
  try{
    const logRes=await api({action:'getLogs',from:sinceDate});
    if(!logRes||!Array.isArray(logRes)||!logRes.length)return;
    let added=0;
    for(const r of logRes){const k=r.empno+'|'+r.date;if(!G.rawLog.has(k)){G.rawLog.set(k,true);added++;}}
    if(added>0){
      lcSave([...G.rawLog.keys()],fromDate);
      setSt('ok',G.ref.length+'명 · 갱신 +'+added+'건');
      recomp();
      const dz=document.getElementById('hdr-dz-txt');if(dz)dz.textContent='✓ '+G.rawLog.size+'건 (갱신됨)';
      toast('✓ 신규 접속 '+added+'건 갱신');
    }else{
      lcSave([...G.rawLog.keys()],fromDate);/* TTL 갱신 */
      setSt('ok',G.ref.length+'명');
    }
  }catch(e){console.warn('bg refresh failed',e);}
}
async function pullRef(){const d=await api({action:'getRef'});if(!d||d.error){toast('⚠ 실패');return;}G.ref=d.map((r,i)=>({...r,id:r.empno+'_'+i}));if(G.rawLog.size)recomp();rRef();toast('✓ '+G.ref.length+'명');}
async function push2Sheets(){if(!G.url){openM('conn-ov');return;}if(!confirm('대상자 정보 '+G.ref.length+'명을 Sheets에 저장?'))return;const d=await api(null,{action:'saveRef',data:G.ref});if(!d||d.error){toast('⚠ 실패');return;}G.sync=new Date().toLocaleString('ko-KR');toast('✓ '+d.count+'명 저장');rRef();}
async function saveKPI(){if(!G.url){toast('Sheets 연결 필요');return;}const records=buildKPIRecords();if(!records.length){toast('데이터 없음');return;}const d=await api(null,{action:'appendKPI',data:records});if(!d||d.error){toast('⚠ 실패');return;}localStorage.setItem('esh_last_kpi_wk',isoWk(new Date()));toast('✓ KPI 이력 '+records.length+'건 저장');}

/* ── 📸 대시보드 이미지 저장 ── */
async function snapDash(){
  if(typeof html2canvas==='undefined'){toast('⚠ html2canvas 로드 중, 잠시 후 다시 시도하세요');return;}
  const btn=document.getElementById('snap-btn');
  if(btn){btn.textContent='⏳ 캡처 중...';btn.disabled=true;}
  try{
    const sf=document.getElementById('f-site').value||'전체';
    const aps=getAPs();const pl=aps.length===1?aps[0].label:aps.length+'기간';
    const hdr=document.querySelector('.hdr');
    const dash=document.getElementById('page-dash');
    if(!hdr||!dash){toast('화면 로드 후 시도하세요');return;}

    /* 캔버스 → img 변환 후 클론 */
    function cloneWithCanvas(src){
      const clone=src.cloneNode(true);
      const srcCanvases=src.querySelectorAll('canvas');
      const clnCanvases=clone.querySelectorAll('canvas');
      srcCanvases.forEach((cv,i)=>{
        try{
          const img=document.createElement('img');
          img.src=cv.toDataURL('image/png');
          img.style.cssText=`width:${cv.offsetWidth}px;height:${cv.offsetHeight}px;display:block`;
          clnCanvases[i]?.parentNode?.replaceChild(img,clnCanvases[i]);
        }catch(e){}
      });
      return clone;
    }

    const W=document.documentElement.clientWidth;
    const w=document.createElement('div');
    w.style.cssText=`position:fixed;left:-${W*2}px;top:0;width:${W}px;background:#f3f4f6;z-index:-1`;
    w.appendChild(cloneWithCanvas(hdr));
    w.appendChild(cloneWithCanvas(dash));
    document.body.appendChild(w);

    const canvas=await html2canvas(w,{scale:2,useCORS:true,backgroundColor:'#f3f4f6',logging:false,width:W});
    document.body.removeChild(w);
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png');
    a.download='ESH_KPI_'+sf+'_'+pl+'_'+fmtD(new Date())+'.png';
    a.click();
    toast('✓ 이미지 저장 완료');
  }catch(e){toast('⚠ 이미지 저장 실패: '+e.message);}
  finally{if(btn){btn.textContent='📸 이미지 저장';btn.disabled=false;}}
}

/* ── 주별 자동 KPI 저장 ── */
async function autoSaveKPIIfNewWeek(){
  if(!G.url||!G.result.length)return;
  const thisWk=isoWk(new Date());
  const lastWk=localStorage.getItem('esh_last_kpi_wk')||'';
  if(thisWk===lastWk)return; // 이미 이번 주에 저장함
  // 전주 데이터가 있을 때만 저장 (현재 주차 제외)
  const prevWks=G.allPeriods.filter(p=>p.key<thisWk);
  if(!prevWks.length)return;
  const records=buildKPIRecords().filter(r=>r.periodLabel!==G.allPeriods[G.allPeriods.length-1]?.label);
  if(!records.length)return;
  const d=await api(null,{action:'appendKPI',data:records});
  if(d&&!d.error){localStorage.setItem('esh_last_kpi_wk',thisWk);toast('✓ 주간 KPI 이력 자동 저장 완료');}
}

document.addEventListener('dragover',e=>{if(document.getElementById('page-dash').classList.contains('on'))e.preventDefault();});
document.addEventListener('drop',e=>{if(document.getElementById('page-dash').classList.contains('on')){e.preventDefault();loadF(e.dataTransfer.files[0]);}});
document.getElementById('fi').addEventListener('change',e=>loadF(e.target.files[0]));
function loadF(file){if(!file)return;lcClear();G.logCache=null;const r=new FileReader();r.onload=e=>{const wb=XLSX.read(e.target.result,{type:'array',cellDates:true});parseXL(wb);};r.readAsArrayBuffer(file);}

function parseXL(wb){
  const ls=wb.Sheets['사원별 접속 현황'];if(!ls){toast('⚠ "사원별 접속 현황" 시트 없음');return;}
  G.logCache=null;const lr=XLSX.utils.sheet_to_json(ls,{header:1,defval:'',cellDates:true});
  let ds=1;for(let i=0;i<Math.min(lr.length,10);i++){if(String(lr[i][0]).trim()==='No'){ds=i+1;break;}}
  const rawRows=[];G.rawLog=new Map();
  for(let i=ds;i<lr.length;i++){const r=lr[i];if(!r[3]&&!r[4])continue;const emp=String(r[3]).trim().padStart(7,'0');if(!/^\d{7}$/.test(emp))continue;let d=r[5] instanceof Date?r[5]:new Date(String(r[5]));if(isNaN(d.getTime()))continue;const dateStr=fmtD(d);G.rawLog.set(emp+'|'+dateStr,true);rawRows.push({empno:emp,date:dateStr,time:r[6]?String(r[6]).slice(0,8):'',siteName:String(r[1]||'').trim(),deptName:String(r[2]||'').trim(),name:String(r[4]||'').trim()});}
  if(!G.rawLog.size){toast('⚠ 접속 데이터를 읽지 못했습니다.');return;}
  toast('✓ 접속로그 '+G.rawLog.size+'건 읽기 완료');G._pendingSync=G.url?rawRows:null;recomp();
}

async function syncLogToSheets(rawRows){
  setSt('warn','Sheets 동기화 중...');
  try{
    const existKeys=new Set(G.rawLog.keys());const newRows=[];
    for(const r of rawRows){const key=r.empno+'|'+r.date;if(!existKeys.has(key)){existKeys.add(key);newRows.push(r);}}
    let logMsg=!newRows.length?'신규 없음':``;
    if(newRows.length){const BATCH=500;let inserted=0;for(let i=0;i<newRows.length;i+=BATCH){const res=await api(null,{action:'appendLog',rows:newRows.slice(i,i+BATCH)});if(res&&res.inserted)inserted+=res.inserted;}G.logCache=null;logMsg='신규 '+inserted+'건';}
    const kpiRecords=buildKPIRecords();let kpiMsg='';
    if(kpiRecords.length){const d=await api(null,{action:'appendKPI',data:kpiRecords});kpiMsg=d&&!d.error?'KPI '+kpiRecords.length+'건':'';}
    setSt('ok',G.ref.length+'명 · 동기화 완료');toast('✓ Sheets 저장 — '+logMsg+(kpiMsg?' | '+kpiMsg:''));
  }catch(e){setSt('ok',G.ref.length+'명 로드');toast('⚠ Sheets 저장 실패: '+e.message);}
}

function buildKPIRecords(){if(!G.result.length||!G.allPeriods.length)return[];const records=[];for(const p of G.allPeriods){for(const s of sitesSorted()){const sr=G.result.filter(r=>r.site===s&&r.periodKey===p.key);if(!sr.length)continue;const ok=sr.filter(r=>r.ok).length;records.push({periodType:G.periodType,periodLabel:p.label,site:s,total:sr.length,achieved:ok,rate:Math.round(ok/sr.length*100)});}}return records;}

/* ── 이진탐색 헬퍼 ── */
function bisectLeft(arr,v){let lo=0,hi=arr.length;while(lo<hi){const m=(lo+hi)>>>1;if(arr[m]<v)lo=m+1;else hi=m;}return lo;}
function bisectRight(arr,v){let lo=0,hi=arr.length;while(lo<hi){const m=(lo+hi)>>>1;if(arr[m]<=v)lo=m+1;else hi=m;}return lo;}
function countRange(sortedArr,from,to){return bisectRight(sortedArr,to)-bisectLeft(sortedArr,from);}
/* 디바운스 */
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms);};}
const _rFullD=debounce(()=>rFull(),120);
const _rExcD=debounce(()=>rExcuse(),120);
const _rRefD=debounce(()=>rRef(),120);

function recomp(){
  if(!G.rawLog.size||!G.ref.length)return;
  const t0=performance.now();
  const dates=[...G.rawLog.keys()].map(k=>k.split('|')[1]).sort();
  const minD=new Date(dates[0]),maxD=new Date(dates[dates.length-1]);
  G.allPeriods=buildPeriods(minD,maxD);buildPSelUI();

  /* ── 1단계: 직원별 정렬 배열 구축 (Set→Array+sort) ── */
  const pLog=new Map(),pExcArr=new Map(),pLogSet=new Map();
  for(const p of G.ref){pLog.set(p.empno,[]);pExcArr.set(p.empno,[]);pLogSet.set(p.empno,new Set());}
  for(const k of G.rawLog.keys()){
    const i=k.indexOf('|');const e=k.slice(0,i),d=k.slice(i+1);
    if(KPI_START_DATE&&d<KPI_START_DATE)continue;
    if(pLog.has(e)){pLog.get(e).push(d);pLogSet.get(e).add(d);}
  }
  for(const[k] of G.excuses){
    const i=k.indexOf('|');const e=k.slice(0,i),d=k.slice(i+1);
    if(KPI_START_DATE&&d<KPI_START_DATE)continue;
    if(pExcArr.has(e))pExcArr.get(e).push(d);
  }
  /* 한 번만 정렬 */
  for(const[,arr] of pLog)arr.sort();
  for(const[,arr] of pExcArr)arr.sort();

  /* ── 2단계: 기간×직원 계산 — 이진탐색으로 O(log D) ── */
  G.result=[];
  const periods=G.allPeriods,refLen=G.ref.length,pLen=periods.length;
  for(let pi=0;pi<pLen;pi++){
    const p=periods[pi];const kpiTarget=getKPI(G.periodType,p.from,p.to);
    for(let ri=0;ri<refLen;ri++){
      const per=G.ref[ri];
      const lg=pLog.get(per.empno)||[];
      const ex=pExcArr.get(per.empno)||[];
      const ls=pLogSet.get(per.empno)||new Set();
      const cnt=countRange(lg,p.from,p.to);
      /* excuse 중 log 미포함만 카운트 */
      const exSlice=ex.slice(bisectLeft(ex,p.from),bisectRight(ex,p.to));
      let excCnt=0;for(const d of exSlice)if(!ls.has(d))excCnt++;
      G.result.push({site:per.site,dept:per.dept,empno:per.empno,name:per.name,periodKey:p.key,period:p.label,cnt,excCnt,total:cnt+excCnt,kpi:kpiTarget,ok:cnt+excCnt>=kpiTarget});
    }
  }

  /* ── 3단계: 빠른 필터용 인덱스 ── */
  G._idx={bySite:new Map(),byPeriod:new Map(),bySitePeriod:new Map()};
  for(const r of G.result){
    if(!G._idx.bySite.has(r.site))G._idx.bySite.set(r.site,[]);G._idx.bySite.get(r.site).push(r);
    if(!G._idx.byPeriod.has(r.periodKey))G._idx.byPeriod.set(r.periodKey,[]);G._idx.byPeriod.get(r.periodKey).push(r);
    const spk=r.site+'|'+r.periodKey;if(!G._idx.bySitePeriod.has(spk))G._idx.bySitePeriod.set(spk,[]);G._idx.bySitePeriod.get(spk).push(r);
  }
  console.log('[recomp]',Math.round(performance.now()-t0)+'ms',G.result.length+'rows');
  const sites=sitesSorted();
  ['f-site','fls','exs'].forEach(id=>{const el=document.getElementById(id);if(!el)return;const cur=el.value;el.innerHTML='<option value="">전체</option>'+sites.map(s=>`<option${s===cur?' selected':''}>${s}</option>`).join('');});
  document.getElementById('fld').innerHTML='<option value="">전체</option>';
  setSt('ok',G.ref.length+'명 · '+G.allPeriods.length+'기간');
  setTimeout(autoSaveKPIIfNewWeek, 3000); // 렌더 완료 후 3초 뒤 체크
  const dz=document.getElementById('hdr-dz-txt');if(dz&&G.rawLog.size)dz.textContent='✓ '+G.rawLog.size+'건';
  G.pages.fl=1;rDash();
  if(G._pendingSync){const rows=G._pendingSync;G._pendingSync=null;syncLogToSheets(rows);}
}

function buildPeriods(minD,maxD){
  const pt=G.periodType,ps=[];
  const MN=['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
  if(pt==='week'){let d=wkStart(minD);while(d<=maxD){const e=new Date(d);e.setDate(e.getDate()+6);const wk=isoWk(d);const thu=new Date(d);thu.setDate(thu.getDate()+3);const tm=thu.getMonth(),ty=thu.getFullYear();const fd=new Date(ty,tm,1);const fthu=new Date(fd);const fdow=fd.getDay()||7;fthu.setDate(fd.getDate()+(fdow<=4?4-fdow:11-fdow));const fwm=new Date(fthu);fwm.setDate(fthu.getDate()-3);const wn=Math.floor((d-fwm)/(7*864e5))+1;ps.push({key:wk,label:MN[tm]+' '+wn+'주차',from:fmtD(d),to:fmtD(e)});d.setDate(d.getDate()+7);}}
  else if(pt==='month'){let y=minD.getFullYear(),m=minD.getMonth();while(new Date(y,m,1)<=maxD){ps.push({key:y+'-'+pad(m+1),label:y+'년 '+MN[m],from:y+'-'+pad(m+1)+'-01',to:fmtD(new Date(y,m+1,0))});m++;if(m>11){m=0;y++;}}}
  else if(pt==='quarter'){let y=minD.getFullYear(),q=Math.floor(minD.getMonth()/3);while(new Date(y,q*3,1)<=maxD){const sm=q*3;ps.push({key:y+'-Q'+(q+1),label:y+'년 '+(q+1)+'분기',from:y+'-'+pad(sm+1)+'-01',to:fmtD(new Date(y,sm+3,0))});q++;if(q>3){q=0;y++;}}}
  else if(pt==='half'){let y=minD.getFullYear(),h=minD.getMonth()<6?0:1;while(new Date(y,h*6,1)<=maxD){const sm=h*6;ps.push({key:y+'-H'+(h+1),label:y+'년 '+(h===0?'상':'하')+'반기',from:y+'-'+pad(sm+1)+'-01',to:fmtD(new Date(y,sm+6,0))});h++;if(h>1){h=0;y++;}}}
  else if(pt==='year'){let y=minD.getFullYear();while(y<=maxD.getFullYear()){ps.push({key:String(y),label:y+'년',from:y+'-01-01',to:y+'-12-31'});y++;}}
  else if(pt==='custom'){const f=document.getElementById('cf').value,t=document.getElementById('ct').value;if(f&&t)ps.push({key:'custom',label:f+'~'+t,from:f,to:t});}
  return ps;
}
function buildPSelUI(){const el=document.getElementById('psel');const cur=el.value;if(G.periodType==='custom'){el.innerHTML='<option value="custom">지정기간</option>';el.style.display='none';return;}el.style.display='';G._pselUpdating=true;el.innerHTML='<option value="">전체</option>'+G.allPeriods.map(p=>`<option value="${p.key}"${p.key===cur?' selected':''}>${p.label}</option>`).join('');const vk=new Set(G.allPeriods.map(p=>p.key));if(!cur||!vk.has(cur)){/* 기본값: 지난주(마지막-1), 없으면 마지막 */const def=G.allPeriods.length>=2?G.allPeriods[G.allPeriods.length-2].key:G.allPeriods.length?G.allPeriods[G.allPeriods.length-1].key:'';el.value=def;}G._pselUpdating=false;}
function getAPs(){const sel=document.getElementById('psel').value;if(!sel||G.periodType==='custom')return G.allPeriods;return G.allPeriods.filter(p=>p.key===sel);}
function setPT(btn){document.querySelectorAll('.ptab').forEach(b=>b.classList.remove('on'));btn.classList.add('on');G.periodType=btn.dataset.pt;document.getElementById('cust-row').classList.toggle('show',G.periodType==='custom');if(G.url)pullAll();else if(G.rawLog.size&&G.ref.length)recomp();}
function sitesSorted(){return[...new Set(G.ref.map(r=>r.site))].sort((a,b)=>siteIdx(a)-siteIdx(b));}

function goFull(opts={}){
  document.querySelectorAll('.tab').forEach(btn=>{const fn=btn.getAttribute('onclick')||'';const m=fn.match(/goTab\('(\w+)'\)/);if(m)btn.classList.toggle('on',m[1]==='full');});
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('on'));document.getElementById('page-full').classList.add('on');
  G._jumpOpts={period:opts.period||'',site:opts.site||'',dept:opts.dept||'',status:opts.status||''};
  const flsSel=document.getElementById('fls');flsSel.innerHTML='<option value="">전체</option>'+sitesSorted().map(s=>`<option${s===(opts.site||'')?' selected':''}>${s}</option>`).join('');flsSel.value=opts.site||'';
  const fldSel=document.getElementById('fld');const depts=[...new Set(G.result.filter(r=>!opts.site||r.site===opts.site).map(r=>r.dept))].sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});
  fldSel.innerHTML='<option value="">전체</option>'+depts.map(d=>`<option${d===(opts.dept||'')?' selected':''}>${d}</option>`).join('');fldSel.value=opts.dept||'';
  document.getElementById('flst').value=opts.status||'';document.getElementById('flq').value='';G.pages.fl=1;rFull();G._jumpOpts=null;
}

function updateTodayLbl(){
  const el=document.getElementById('p-today-lbl');if(!el)return;
  const now=new Date();
  const y=now.getFullYear(),m=now.getMonth()+1;
  const MN=['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
  const hl=(txt)=>`<span style="font-weight:800;color:var(--bl);background:var(--bll);padding:1px 7px;border-radius:5px">${txt}</span>`;
  let prefix='',hlPart='';
  switch(G.periodType){
    case 'week':{
      const wkN=Math.ceil((now.getDate()-(now.getDay()||7)+8)/7);
      prefix='금일은 ';hlPart=`${m}월 ${wkN}주차`;break;}
    case 'month':
      prefix='금일은 ';hlPart=`${y}년 ${MN[m-1]}`;break;
    case 'quarter':{
      const q=Math.ceil(m/3);
      prefix='금일은 ';hlPart=`${y}년 ${q}분기`;break;}
    case 'half':{
      const h=m<=6?'상반기':'하반기';
      prefix='금일은 ';hlPart=`${y}년 ${h}`;break;}
    case 'year':
      prefix='금일은 ';hlPart=`${y}년`;break;
    default:
      el.innerHTML='';return;
  }
  el.innerHTML=prefix+hl(hlPart)+' 입니다.';
}

function rDash(){
  updateTodayLbl();
  const sf=document.getElementById('f-site').value;const aps=getAPs();
  const apKeys=new Set(aps.map(p=>p.key));
  let base;
  if(sf&&aps.length===1){const k=sf+'|'+aps[0].key;base=G._idx?.bySitePeriod?.get(k)||[];}
  else if(sf){base=(G._idx?.bySite?.get(sf)||[]).filter(r=>apKeys.has(r.periodKey));}
  else if(aps.length===1){base=G._idx?.byPeriod?.get(aps[0].key)||[];}
  else{base=G.result.filter(r=>apKeys.has(r.periodKey));}
  if(sf&&aps.length!==1)base=base.filter(r=>apKeys.has(r.periodKey));
  const tot=base.length,ok=base.filter(r=>r.ok).length,mis=tot-ok;const rt=tot?Math.round(ok/tot*100):0;
  const exTot=base.reduce((a,r)=>a+r.excCnt,0),ppl=new Set(base.map(r=>r.empno)).size;
  const kpiTarget=aps.length?getKPI(G.periodType,aps[0].from,aps[0].to):KPI_WEEK;
  const cs=document.getElementById('f-site').value,cp=aps.length===1?aps[0].key:'';
  document.getElementById('krow').innerHTML=`
    <div class="kc hi"><div class="kl">KPI 달성률</div><div class="kv">${rt}%</div><div class="ks">목표 ${kpiTarget}회 이상</div></div>
    <div class="kc" style="cursor:pointer" onclick="goFull({status:'ok',site:'${cs}',period:'${cp}'})"><div class="kl">달성 인원 ↗</div><div class="kv">${ok.toLocaleString()} <span style="font-size:13px;font-weight:400;color:var(--tx2)">(${tot?Math.round(ok/tot*100):0}%)</span></div><div class="ks">/ ${tot.toLocaleString()} 명</div></div>
    <div class="kc danger" style="cursor:pointer" onclick="goFull({status:'fail',site:'${cs}',period:'${cp}'})"><div class="kl">미달성 인원 ↗</div><div class="kv">${mis.toLocaleString()} <span style="font-size:13px;font-weight:400;color:#2d7dd2">(${tot?Math.round(mis/tot*100):0}%)</span></div><div class="ks">follow-up 필요</div></div>
    <div class="kc purp" style="cursor:pointer" onclick="goTab('excuse')"><div class="kl">사유소명 ↗</div><div class="kv">${exTot.toLocaleString()}</div><div class="ks">접속 인정 처리</div></div>
    <div class="kc" style="cursor:pointer" onclick="goTab('ref')"><div class="kl">대상 인원 ↗</div><div class="kv">${ppl}</div><div class="ks">${aps.length}기간</div></div>`;
  const pLbl=aps.length===1?aps[0].label:aps.length+'기간';
  document.getElementById('slbl').textContent=pLbl+(sf?' · '+sf:'');document.getElementById('tlbl').textContent=sf||'전체';
  rTrend(sf);rDualChart(base,sf);requestAnimationFrame(()=>rCards(sf,aps));
}

function rTrend(sf){
  const tb=G.result.filter(r=>(!sf||r.site===sf));const periods=G.allPeriods;
  if(!periods.length){document.getElementById('trend-box').innerHTML='<div style="color:var(--tx3);font-size:13px;padding:8px 0">데이터 없음</div>';return;}
  const data=periods.map(p=>{const wr=tb.filter(r=>r.periodKey===p.key);const rt=wr.length?Math.round(wr.filter(r=>r.ok).length/wr.length*100):0;return{label:p.label,key:p.key,rt};});
  const selKey=document.getElementById('psel').value;
  document.getElementById('trend-box').innerHTML=`<div style="position:relative" id="trend-wrap">${data.map((d,i)=>{const col=d.rt>=80?'var(--gn)':d.rt>=50?'var(--am)':'var(--rd)';const isSel=d.key===selKey;return `<div class="trend-item" style="${isSel?'background:var(--bll);border-radius:4px;':''}cursor:pointer" onclick="goFull({period:'${d.key}',site:'${sf||''}'})" title="${d.label}"><span class="trend-lbl" style="${isSel?'font-weight:700;color:var(--bl)':''}">${d.label}</span><div class="trend-bg" data-idx="${i}"><div class="trend-fg" id="tfg_${i}" style="width:${d.rt}%;background:${col}"></div></div><span class="trend-val" style="color:${col}">${d.rt}%</span></div>`;}).join('')}<svg id="trend-svg" style="position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;overflow:visible"></svg></div>`;
  requestAnimationFrame(()=>{const wrap=document.getElementById('trend-wrap');const svg=document.getElementById('trend-svg');if(!wrap||!svg)return;const wRect=wrap.getBoundingClientRect();const pts=data.map((d,i)=>{const fg=document.getElementById('tfg_'+i);if(!fg)return null;const bg=fg.parentElement;return{x:fg.getBoundingClientRect().right-wRect.left,y:bg.getBoundingClientRect().top-wRect.top+bg.getBoundingClientRect().height/2,rt:d.rt};}).filter(Boolean);if(pts.length<2){svg.innerHTML='';return;}svg.innerHTML=`<polyline points="${pts.map(p=>p.x+','+p.y).join(' ')}" fill="none" stroke="rgba(180,188,200,0.35)" stroke-width="1.2"/>${pts.map(p=>`<circle cx="${p.x}" cy="${p.y}" r="2" fill="${p.rt>=80?'#1059a0':p.rt>=50?'#2d7dd2':'#c0455e'}" opacity="0.35"/>`).join('')}`;});
}

function rDualChart(base,sf){
  const allSites=sitesSorted();const dSites=sf?[sf]:allSites;
  const TOP=['중부권역-평택','중부권역-아산','동부권역-경산','동부권역-영동'];
  const topSites=dSites.filter(s=>TOP.includes(s));const btmSites=dSites.filter(s=>!TOP.includes(s));
  ['siteC','deptC'].forEach(id=>{const c=Chart.getChart(id);if(c)c.destroy();});
  if(G.charts.s){try{G.charts.s.destroy();}catch(e){}}if(G.charts.d){try{G.charts.d.destroy();}catch(e){}}
  function h2r(hex,a){const r=parseInt(hex.slice(1,3),16),g=parseInt(hex.slice(3,5),16),b=parseInt(hex.slice(5,7),16);return `rgba(${r},${g},${b},${a})`;}
  const siteClr=v=>v>=80?'#0a3f75':v>=50?'#1059a0':'#2d7dd2';
  if(!Chart.registry.plugins.get('eshLbl')){Chart.register({id:'eshLbl',afterDatasetsDraw(chart){const{ctx,scales:{y}}=chart;const yTop=y?y.top:0;const placed=[];const mg=16;chart.data.datasets.forEach((ds,di)=>{const meta=chart.getDatasetMeta(di);if(meta.hidden)return;meta.data.forEach((bar,i)=>{const val=ds.data[i];if(val===null||val===undefined)return;const isSite=!!ds._isSite;const fs=isSite?11:9;const lx=bar.x;let ly=Math.max(bar.y-(isSite?7:5),yTop+fs+2);for(const p of placed){if(Math.abs(p.x-lx)<mg&&Math.abs(p.y-ly)<fs+2){ly=Math.min(p.y,ly)-(fs+3);}}ly=Math.max(ly,yTop+fs+1);placed.push({x:lx,y:ly});ctx.save();ctx.font=`${isSite?'700':'500'} ${fs}px 'Pretendard','Noto Sans KR',system-ui`;ctx.fillStyle=isSite?(Array.isArray(ds.backgroundColor)?ds.backgroundColor[i]:ds.backgroundColor)||'#1a56db':'#6b7280';ctx.textAlign='center';ctx.textBaseline='bottom';ctx.fillText(val+'%',lx,ly);ctx.restore();});});}});}
  function buildGroupChart(sites,canvasId,wrapId){
    if(!sites.length){document.getElementById(wrapId).style.height='0';document.getElementById(canvasId).style.display='none';return null;}
    const siteDepts={};for(const s of sites){siteDepts[s]=[...new Set(base.filter(r=>r.site===s).map(r=>r.dept))].sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});}
    const labels=[],siteIdxSet=new Set(),sepIdxSet=new Set();const siteData=[],deptData=[],siteBg=[],deptBg=[];
    for(let si=0;si<sites.length;si++){const s=sites[si];const sr=base.filter(r=>r.site===s);const sv=sr.length?Math.round(sr.filter(r=>r.ok).length/sr.length*100):0;const sc=siteClr(sv);if(si>0){const sepIdx=labels.length;labels.push('');sepIdxSet.add(sepIdx);siteData.push(null);deptData.push(null);siteBg.push('transparent');deptBg.push('transparent');}siteIdxSet.add(labels.length);labels.push(s);siteData.push(sv);deptData.push(null);siteBg.push(sc);deptBg.push('transparent');siteDepts[s].forEach(dept=>{const dr=base.filter(r=>r.site===s&&r.dept===dept);const dv=dr.length?Math.round(dr.filter(r=>r.ok).length/dr.length*100):0;labels.push(dept);siteData.push(null);deptData.push(dv);siteBg.push('transparent');deptBg.push(h2r(sc,0.38));});}
    document.getElementById(wrapId).style.height='200px';document.getElementById(canvasId).style.display='';document.getElementById(canvasId).style.cursor='pointer';
    /* ★ 기존 차트 재사용 (data update) */
    const existing=Chart.getChart(canvasId);
    if(existing&&existing.data.labels.length===labels.length){
      existing.data.labels=labels;existing.data.datasets[0].data=siteData;existing.data.datasets[0].backgroundColor=siteBg;existing.data.datasets[1].data=deptData;existing.data.datasets[1].backgroundColor=deptBg;existing.update('none');return existing;
    }
    if(existing)existing.destroy();
    const sepLinePlugin={id:'sep_'+canvasId,afterDraw(chart){if(!sepIdxSet.size)return;const{ctx,scales:{x,y}}=chart;ctx.save();ctx.strokeStyle='rgba(160,168,180,0.45)';ctx.lineWidth=1;ctx.setLineDash([4,4]);for(const si of sepIdxSet){const meta=chart.getDatasetMeta(0);if(meta&&meta.data[si]){const cx=meta.data[si].x;ctx.beginPath();ctx.moveTo(cx,y.top-10);ctx.lineTo(cx,y.bottom+4);ctx.stroke();}}ctx.restore();}};
    return new Chart(document.getElementById(canvasId),{type:'bar',plugins:[sepLinePlugin],data:{labels,datasets:[{data:siteData,backgroundColor:siteBg,borderRadius:6,borderSkipped:false,barThickness:24,_isSite:true,order:1},{data:deptData,backgroundColor:deptBg,borderRadius:4,borderSkipped:false,barThickness:13,_isSite:false,order:2}]},options:{responsive:true,maintainAspectRatio:false,animation:{duration:350},clip:false,onClick(evt,elements){const aps=getAPs();const pk=aps.length===1?aps[0].key:'';let idx=-1;if(elements.length){idx=elements[0].index;}else{const xScale=this.scales.x;if(!xScale)return;let minD=Infinity;for(let i=0;i<labels.length;i++){const d=Math.abs(evt.x-xScale.getPixelForTick(i));if(d<minD){minD=d;idx=i;}}if(minD>30||idx<0)return;}const lbl=labels[idx];if(!lbl||sepIdxSet.has(idx))return;if(siteIdxSet.has(idx)){goFull({site:lbl,period:pk});}else{const cs=sites.find(s=>siteDepts[s]&&siteDepts[s].includes(lbl));if(cs)goFull({site:cs,dept:lbl,period:pk});}},plugins:{legend:{display:false},tooltip:{callbacks:{title(ctx){return ctx[0]?.label||'';},label(ctx){return ctx.raw===null?'':' '+ctx.raw+'%';}},filter:item=>item.raw!==null}},scales:{y:{min:0,max:130,ticks:{callback:v=>v<=100?v+'%':'',font:{size:9},color:'#b0b8c4',stepSize:25,maxTicksLimit:5},grid:{color:'rgba(0,0,0,.04)'},border:{display:false},afterFit(a){a.width=36;}},x:{ticks:{font(ctx){return{size:siteIdxSet.has(ctx.index)?11:9,weight:siteIdxSet.has(ctx.index)?'700':'400',family:"'Pretendard','Noto Sans KR',system-ui"};},color(ctx){if(sepIdxSet.has(ctx.index))return 'transparent';return siteIdxSet.has(ctx.index)?'#1e3a5f':'#9aa0ad';},maxRotation:40,minRotation:0,autoSkip:false,padding:2},grid:{display:false},border:{display:false},offset:true}},layout:{padding:{top:24,right:16,bottom:0,left:16}}}});
  }
  G.charts.s=buildGroupChart(topSites,'siteC','site-chw');G.charts.d=buildGroupChart(btmSites,'deptC','dept-chw');
}

function rCards(sf,aps){
  const dSites=sf?[sf]:sitesSorted();const cRows=G.result.filter(r=>(!sf||r.site===sf)&&aps.some(p=>p.key===r.periodKey));
  document.getElementById('scards').innerHTML=dSites.map((s,si)=>{
    const sr=cRows.filter(r=>r.site===s);const t=sr.length,o=sr.filter(r=>r.ok).length,m=t-o;const rt=t?Math.round(o/t*100):0;const col=rt>=80?'#0a3f75':rt>=50?'#1059a0':'#2d7dd2';
    if(t===0)return `<div class="sc" style="border-left-color:#b0bec5"><div class="sc-hd"><div><div class="sc-nm">${s}</div><div class="sc-sub" style="color:var(--tx3)">해당 기간 데이터 없음</div></div><div class="sc-rt" style="color:var(--tx3)">-</div></div></div>`;
    const kpiTarget=sr[0]?.kpi||KPI_WEEK;
    const missRows=sr.filter(r=>!r.ok);const dg={};for(const r of missRows){if(!dg[r.dept])dg[r.dept]=[];dg[r.dept].push(r);}
    const dkeys=Object.keys(dg).sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});
    const missTree=dkeys.map((dept,di)=>{const uid='tr'+si+'_'+di;const mbs=dg[dept].sort((a,b)=>a.total-b.total);return `<div class="tree-dept"><div class="tree-dept-hd" onclick="toggleT('${uid}')"><span class="tree-tog" id="t_${uid}">▶</span><span class="tree-dept-nm">${dept}</span><span class="tree-dept-cnt" style="background:#fef9c3;color:#92400e">${mbs.length}명</span></div><div class="tree-members" id="${uid}">${mbs.map(r=>`<div class="tree-member" onclick="openDrill('${r.empno}')"><span class="tree-member-nm">${r.name}</span><div style="display:flex;align-items:center;gap:6px"><span style="font-size:10px;color:${r.total===0?'#c0455e':'#92400e'};background:${r.total===0?'#fde8ec':'#fef9c3'};padding:1px 6px;border-radius:10px;font-weight:600">${r.total===0?'미접속':'미달성'}</span><span class="tree-member-cnt" style="background:${r.total===0?'#fde8ec':'#fef9c3'};color:${r.total===0?'#c0455e':'#92400e'}">${r.total}/${kpiTarget}회</span></div></div>`).join('')}</div></div>`;}).join('');
    const okRows=sr.filter(r=>r.ok);const okDg={};for(const r of okRows){if(!okDg[r.dept])okDg[r.dept]=[];okDg[r.dept].push(r);}
    const okKeys=Object.keys(okDg).sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});
    const okTree=okKeys.map((dept,di)=>{const uid='ok'+si+'_'+di;const mbs=okDg[dept].sort((a,b)=>b.total-a.total);return `<div class="tree-dept"><div class="tree-dept-hd" onclick="toggleT('${uid}')"><span class="tree-tog" id="t_${uid}">▶</span><span class="tree-dept-nm">${dept}</span><span class="tree-dept-cnt" style="background:#d4edda;color:#276740">${mbs.length}명</span></div><div class="tree-members" id="${uid}">${mbs.map(r=>`<div class="tree-member" onclick="openDrill('${r.empno}')"><span class="tree-member-nm">${r.name}</span><span class="tree-member-cnt" style="background:#d4edda;color:#276740">${r.total}/${kpiTarget}회</span></div>`).join('')}</div></div>`;}).join('');
    return `<div class="sc" style="border-left-color:${col};border-left-width:4px"><div class="sc-hd"><div><div class="sc-nm">${s}</div><div class="sc-sub">${t}명 · 달성 ${o} / 미달성 ${m} · 목표 ${kpiTarget}회</div></div><div class="sc-rt" style="color:${col}">${rt}%</div></div><div class="bb"><div class="bf" style="width:${rt}%;background:${col}"></div></div>${m>0?`<div style="font-size:11px;font-weight:700;color:#2d7dd2;margin:8px 0 5px">미달성 ${m}명</div>${missTree}`:''}${o>0?`<div style="font-size:11px;font-weight:700;color:#0a3f75;margin:${m>0?'10px':'8px'} 0 5px">달성 ${o}명</div>${okTree}`:''}</div>`;
  }).join('');
}

function toggleT(id){const el=document.getElementById(id),t=document.getElementById('t_'+id);const o=el.classList.toggle('open');t.textContent=o?'▼':'▶';}

function openDrill(empno,periodKeyOverride){
  const person=G.ref.find(r=>r.empno===empno);if(!person)return;G.drill=person;
  const ld=new Set([...G.rawLog.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const ed=new Set([...G.excuses.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const pd=new Set([...G.pendingExcuses.keys()].filter(k=>k.startsWith(empno+'|')&&G.pendingExcuses.get(k)._status==='pending').map(k=>k.split('|')[1]));
  const rd=new Map([...G.pendingExcuses.entries()].filter(([k,v])=>k.startsWith(empno+'|')&&v._status==='rejected').map(([k,v])=>[k.split('|')[1],v]));
  const aps=periodKeyOverride?G.allPeriods.filter(p=>p.key===periodKeyOverride):getAPs();
  const logMonths=new Set([...ld].map(d=>d.slice(0,7)));const apMonths=new Set();for(const p of aps){let d=new Date(p.from);while(fmtD(d)<=p.to){apMonths.add(fmtD(d).slice(0,7));d.setDate(d.getDate()+1);}}
  const months=new Set([...logMonths,...apMonths]);const allD=new Set();for(const p of aps){let d=new Date(p.from);while(fmtD(d)<=p.to){allD.add(fmtD(d));d.setDate(d.getDate()+1);}}
  let tC=0,tE=0,okP=0;for(const p of aps){const kpiT=getKPI(G.periodType,p.from,p.to);let c=0,e=0;for(const d of allD){if(d<p.from||d>p.to)continue;if(ld.has(d))c++;else if(ed.has(d))e++;}tC+=c;tE+=e;if(c+e>=kpiT)okP++;}
  const kpiTarget=aps.length?getKPI(G.periodType,aps[0].from,aps[0].to):KPI_WEEK;const rate=aps.length?Math.round(okP/aps.length*100):0;
  document.getElementById('p-av').textContent=person.name[0];document.getElementById('p-nm').textContent=person.name;
  document.getElementById('p-sub').textContent=person.site+' · '+person.dept+' · '+person.empno;
  document.getElementById('p-rt').textContent=rate+'%';document.getElementById('p-rt').style.color=rate>=100?'#0a3f75':rate>=50?'#1059a0':'#2d7dd2';
  document.getElementById('p-rt-sub').textContent=okP+'/'+aps.length+'기간 달성 (접속 '+tC+'일+면제 '+tE+'일 / 목표 '+kpiTarget+'회)';
  const today=fmtD(new Date());let calH='';
  for(const ym of[...months].sort()){
    const[y,m]=ym.split('-').map(Number);const fd=new Date(y,m-1,1).getDay(),ld2=new Date(y,m,0).getDate();
    calH+=`<div style="margin-bottom:14px"><div style="font-size:13px;font-weight:600;margin-bottom:6px">${ym}</div><div class="cal-grid">${['일','월','화','수','목','금','토'].map(d=>`<div class="cal-hd">${d}</div>`).join('')}${Array(fd).fill('<div class="cal-day empty"></div>').join('')}`;
    for(let dd=1;dd<=ld2;dd++){
      const dt=y+'-'+pad(m)+'-'+pad(dd);const dow=new Date(y,m-1,dd).getDay(),isWk=[0,6].includes(dow),isFut=dt>today;
      const isLog=ld.has(dt),isExc=ed.has(dt)&&!isLog,isPend=pd.has(dt)&&!isLog&&!isExc,isRej=rd.has(dt)&&!isLog&&!isExc&&!isPend;
      const wkS=wkStart(new Date(dt)),wkE=new Date(wkS);wkE.setDate(wkE.getDate()+6);
      const wkSStr=fmtD(wkS),wkEStr=fmtD(wkE);
      const weekLogCnt=[...ld].filter(d=>d>=wkSStr&&d<=wkEStr).length+[...ed].filter(d=>d>=wkSStr&&d<=wkEStr).length;
      let cls='cal-day',extra='';
      if(isFut)cls+=' future';
      else if(isExc){cls+=` exc${isWk?' wknd':''}`;extra=`onclick="delExcuse('${empno}|${dt}')" title="면제 취소"`;}
      else if(isPend){cls+=` pend${isWk?' wknd':''}`;extra=`onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt})" title="소명 대기 중"`;}
      else if(isRej){cls+=` rej${isWk?' wknd':''}`;extra=`onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt})" title="반려됨"`;}
      else if(isLog){cls+=` ok${isWk?' wknd':''}`;}
      else if(!isFut){if(isMonthClosed(dt)){cls+=` miss locked${isWk?' wknd':''}`;extra=`title="마감된 달 — 소명 불가"`;}else{cls+=` miss${isWk?' wknd':''}`;extra=`onmousedown="calDragStart(event,'${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt})" onmouseenter="calDragEnter(this,'${dt}')" title="드래그로 여러날 선택 가능"`;}}
      calH+=`<div class="${cls}" ${extra}><span class="dn">${dd}</span>${isExc?'<span class="cal-count">면제</span>':isPend?'<span class="cal-count">대기</span>':isRej?'<span class="cal-count">반려</span>':isLog?'<span class="cal-count">✓</span>':isFut?'':isMonthClosed(dt)?'<span class="cal-count">🔒</span>':''}</div>`;
    }calH+='</div></div>';
  }
  document.getElementById('p-cal').innerHTML=calH;
  const minLogDate=[...ld].sort()[0];const missAllD=new Set();
  if(minLogDate){let d=new Date(minLogDate);while(fmtD(d)<=today){const dt=fmtD(d);if(!ld.has(dt)&&!ed.has(dt))missAllD.add(dt);d.setDate(d.getDate()+1);}}
  const mD=[...missAllD].filter(dt=>{const[y,m]=dt.split('-');return months.has(y+'-'+m);}).sort();
  document.getElementById('p-miss').innerHTML=mD.length?mD.map(dt=>{
    const dn=['일','월','화','수','목','금','토'][new Date(dt).getDay()];
    const wkS=wkStart(new Date(dt)),wkE=new Date(wkS);wkE.setDate(wkE.getDate()+6);
    const weekLogCnt=[...ld].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length+[...ed].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length;
    const isPend=pd.has(dt);const rejObj=rd.get(dt);const achieved=weekLogCnt>=KPI_WEEK;
    let btnHtml='';
    if(isPend){btnHtml=`<span class="excuse-pend-badge">⏳ 승인 대기</span>`;}
    else if(rejObj){btnHtml=`<div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px"><span style="font-size:11px;background:var(--rdl);color:var(--rd);padding:2px 8px;border-radius:8px;font-weight:600">✕ 반려됨</span><button class="btn bsm" style="color:var(--pu);border-color:var(--pu);font-size:11px" onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt})">재소명</button></div>`;return `<div class="miss-item" style="flex-direction:column;align-items:stretch;gap:6px"><div style="display:flex;align-items:center;justify-content:space-between"><span class="miss-date">${dt} (${dn})</span>${btnHtml}</div><div class="miss-rej-box">반려 사유: ${esc(rejObj.rejectReason||'(미입력)')} <span style="font-size:10px;opacity:.7">${rejObj.rejectedAt||''}</span></div></div>`;}
    else if(achieved){btnHtml=`<span style="font-size:11px;color:var(--tx3);background:var(--sf2);padding:2px 8px;border-radius:8px">주간 달성</span>`;}
    else{btnHtml=`<button class="btn bsm" style="color:var(--pu);border-color:var(--pu)" onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt})">소명 입력</button>`;}
    return `<div class="miss-item"><span class="miss-date">${dt} (${dn})</span>${btnHtml}</div>`;
  }).join(''):'<div style="color:#0a3f75;font-size:13px;padding:8px 0">미접속 날짜 없음 🎉</div>';
  openM('person-ov');
}

/* ── 달력 드래그 선택 ── */
let G_calDrag={active:false,dates:new Set(),empno:'',name:'',site:'',dept:'',weekCnt:0,startDt:'',dateFrom:'',dateTo:''};
function calDragStart(e,empno,dt,name,site,dept,weekCnt,dateFrom,dateTo){
  e.preventDefault();e.stopPropagation();
  G_calDrag={active:true,dates:new Set([dt]),empno,name,site,dept,weekCnt,startDt:dt,dateFrom:dateFrom||'',dateTo:dateTo||''};
  e.currentTarget.classList.add('cal-drag-sel');
  document.body.style.userSelect='none';
  console.log('[drag] start',dt);
}
function calDragEnter(el,dt){
  if(!G_calDrag.active)return;
  G_calDrag.dates.add(dt);
  el.classList.add('cal-drag-sel');
  console.log('[drag] enter',dt,'total',G_calDrag.dates.size);
}
function _calDragFinish(){
  if(!G_calDrag.active)return;
  G_calDrag.active=false;
  document.body.style.userSelect='';
  document.querySelectorAll('.cal-drag-sel').forEach(el=>el.classList.remove('cal-drag-sel'));
  console.log('[drag] finish',G_calDrag.dates.size,'dates',[...G_calDrag.dates]);
  if(G_calDrag.dates.size>0){
    openExcuseM(G_calDrag.empno,G_calDrag.startDt,G_calDrag.name,G_calDrag.site,G_calDrag.dept,G_calDrag.weekCnt,new Set(G_calDrag.dates),G_calDrag.dateFrom||undefined,G_calDrag.dateTo||undefined);
  }
}
document.addEventListener('mouseup',_calDragFinish,{capture:true});

function initIndiv(){const sites=sitesSorted();const el=document.getElementById('iv-site');el.innerHTML='<option value="">선택</option>'+sites.map(s=>`<option>${s}</option>`).join('');ivBuildPeriodSel();}
function rIndivSite(){const sf=document.getElementById('iv-site').value;const depts=[...new Set(G.ref.filter(r=>r.site===sf).map(r=>r.dept))].sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});document.getElementById('iv-dept').innerHTML='<option value="">선택</option>'+depts.map(d=>`<option>${d}</option>`).join('');document.getElementById('iv-name').innerHTML='<option value="">선택</option>';}
function rIndivDept(){const sf=document.getElementById('iv-site').value;const df=document.getElementById('iv-dept').value;const persons=G.ref.filter(r=>r.site===sf&&r.dept===df).sort((a,b)=>a.name.localeCompare(b.name,'ko'));document.getElementById('iv-name').innerHTML='<option value="">선택</option>'+persons.map(p=>`<option value="${p.empno}">${p.name}</option>`).join('');}
function setIPT(btn){document.querySelectorAll('[data-ipt]').forEach(b=>b.classList.remove('on'));btn.classList.add('on');G_iv.periodType=btn.dataset.ipt;const isC=G_iv.periodType==='custom';document.getElementById('iv-cust').style.display=isC?'flex':'none';document.getElementById('iv-period').style.display=isC?'none':'';ivBuildPeriodSel();}
function ivBuildPeriodSel(){if(!G.allPeriods.length)return;const pt=G_iv.periodType;if(pt==='custom'){document.getElementById('iv-period').style.display='none';return;}const dates=[...G.rawLog.keys()].map(k=>k.split('|')[1]).sort();if(!dates.length)return;const origPT=G.periodType;G.periodType=pt;const ps=buildPeriods(new Date(dates[0]),new Date(dates[dates.length-1]));G.periodType=origPT;const el=document.getElementById('iv-period');el.style.display='';el.innerHTML=ps.map(p=>`<option value="${p.key}" data-from="${p.from}" data-to="${p.to}">${p.label}</option>`).join('');if(ps.length)el.selectedIndex=ps.length-1;el._ps=ps;}
function runIndiv(){
  const empno=document.getElementById('iv-name').value;if(!empno){toast('이름을 선택하세요');return;}
  const person=G.ref.find(r=>r.empno===empno);if(!person){toast('대상자 정보에서 찾을 수 없습니다');return;}
  let from,to,periodType=G_iv.periodType,periodLabel='';
  if(periodType==='custom'){from=document.getElementById('iv-cf').value;to=document.getElementById('iv-ct').value;if(!from||!to){toast('기간을 선택하세요');return;}periodLabel=from+' ~ '+to;}
  else{const sel=document.getElementById('iv-period');const opt=sel.options[sel.selectedIndex];if(!opt||!opt.dataset.from){toast('기간을 선택하세요');return;}from=opt.dataset.from;to=opt.dataset.to;periodLabel=opt.textContent;}
  const kpiTarget=getKPI(periodType,from,to);
  const ld=new Set([...G.rawLog.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const ed=new Set([...G.excuses.keys()].filter(k=>k.startsWith(empno+'|')).map(k=>k.split('|')[1]));
  const pd=new Set([...G.pendingExcuses.keys()].filter(k=>k.startsWith(empno+'|')&&G.pendingExcuses.get(k)._status==='pending').map(k=>k.split('|')[1]));
  const rd=new Map([...G.pendingExcuses.entries()].filter(([k,v])=>k.startsWith(empno+'|')&&v._status==='rejected').map(([k,v])=>[k.split('|')[1],v]));
  const allD=new Set();let d=new Date(from);while(fmtD(d)<=to){allD.add(fmtD(d));d.setDate(d.getDate()+1);}
  let cnt=0,excCnt=0;for(const dt of allD){if(ld.has(dt))cnt++;else if(ed.has(dt))excCnt++;}
  const total=cnt+excCnt,ok=total>=kpiTarget,pct=kpiTarget?Math.round(total/kpiTarget*100):0;
  document.getElementById('iv-av').textContent=person.name[0];document.getElementById('iv-nm').textContent=person.name;
  document.getElementById('iv-sub').textContent=person.site+' · '+person.dept+' · '+person.empno+' · '+periodLabel;
  document.getElementById('iv-rt').textContent=pct+'%';document.getElementById('iv-rt').style.color=ok?'#0a3f75':pct>=50?'#1059a0':'#2d7dd2';
  document.getElementById('iv-rt-sub').textContent='접속 '+cnt+'일 + 면제 '+excCnt+'일 / 목표 '+kpiTarget+'회 ('+(ok?'달성':'미달성')+')';
  const months=new Set();for(const dt of allD){const[y,m]=dt.split('-');months.add(y+'-'+m);}
  const today=fmtD(new Date());let calH='';
  const MNK=['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
  for(const ym of[...months].sort()){
    const[y,m]=ym.split('-').map(Number);const fd=new Date(y,m-1,1).getDay(),ld2=new Date(y,m,0).getDate();
    calH+=`<div style="margin-bottom:14px"><div style="font-size:13px;font-weight:600;margin-bottom:6px">${y}년 ${MNK[m-1]}</div><div class="cal-grid">${['일','월','화','수','목','금','토'].map(v=>`<div class="cal-hd">${v}</div>`).join('')}${Array(fd).fill('<div class="cal-day empty"></div>').join('')}`;
    for(let dd=1;dd<=ld2;dd++){
      const dt=y+'-'+pad(m)+'-'+pad(dd);const dow=new Date(y,m-1,dd).getDay(),isWk=[0,6].includes(dow),isFut=dt>today;
      const isLog=ld.has(dt),isExc=ed.has(dt)&&!isLog,isPend=pd.has(dt)&&!isLog&&!isExc,isRej=rd.has(dt)&&!isLog&&!isExc&&!isPend;
      const wkS=wkStart(new Date(dt)),wkE=new Date(wkS);wkE.setDate(wkE.getDate()+6);
      const weekLogCnt=[...ld].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length+[...ed].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length;
      let cls='cal-day',extra='';
      if(isFut)cls+=' future';
      else if(isExc){cls+=` exc${isWk?' wknd':''}`;extra=`onclick="delExcuse('${empno}|${dt}');runIndiv()" title="면제 취소"`;}
      else if(isPend){cls+=` pend${isWk?' wknd':''}`;extra=`onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt},undefined,'${from}','${to}');G.drill={empno:'${empno}',_iv:true}" title="소명 대기 중"`;}
      else if(isRej){cls+=` rej${isWk?' wknd':''}`;extra=`onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt},undefined,'${from}','${to}');G.drill={empno:'${empno}',_iv:true}" title="반려됨"`;}
      else if(isLog){cls+=` ok${isWk?' wknd':''}`;}
      else if(!isFut){if(isMonthClosed(dt)){cls+=` miss locked${isWk?' wknd':''}`;extra=`title="마감된 달 — 소명 불가"`;}else{cls+=` miss${isWk?' wknd':''}`;extra=`onmousedown="calDragStart(event,'${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt},'${from}','${to}');G.drill={empno:'${empno}',_iv:true}" onmouseenter="calDragEnter(this,'${dt}')" title="드래그로 여러날 선택 가능"`;}}
      calH+=`<div class="${cls}" ${extra}><span class="dn">${dd}</span>${isExc?'<span class="cal-count">면제</span>':isPend?'<span class="cal-count">대기</span>':isRej?'<span class="cal-count">반려</span>':isLog?'<span class="cal-count">✓</span>':isFut?'':isMonthClosed(dt)?'<span class="cal-count">🔒</span>':''}</div>`;
    }calH+='</div></div>';
  }
  document.getElementById('iv-cal').innerHTML=calH;
  const mD=[...allD].filter(dt=>!ld.has(dt)&&!ed.has(dt)).sort();
  document.getElementById('iv-miss').innerHTML=mD.length?mD.map(dt=>{
    const dn=['일','월','화','수','목','금','토'][new Date(dt).getDay()];
    const wkS=wkStart(new Date(dt)),wkE=new Date(wkS);wkE.setDate(wkE.getDate()+6);
    const weekLogCnt=[...ld].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length+[...ed].filter(d=>d>=fmtD(wkS)&&d<=fmtD(wkE)).length;
    const isPend=pd.has(dt);const rejObj=rd.get(dt);const achieved=weekLogCnt>=KPI_WEEK;
    let btnHtml='';
    if(isPend){btnHtml=`<span class="excuse-pend-badge">⏳ 승인 대기</span>`;}
    else if(rejObj){return `<div class="miss-item" style="flex-direction:column;align-items:stretch;gap:6px"><div style="display:flex;align-items:center;justify-content:space-between"><span class="miss-date">${dt} (${dn})</span><div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px"><span style="font-size:11px;background:var(--rdl);color:var(--rd);padding:2px 8px;border-radius:8px;font-weight:600">✕ 반려됨</span><button class="btn bsm" style="color:var(--pu);border-color:var(--pu);font-size:11px" onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt},undefined,'${from}','${to}');G.drill={empno:'${empno}',_iv:true}">재소명</button></div></div><div class="miss-rej-box">반려 사유: ${esc(rejObj.rejectReason||'(미입력)')} <span style="font-size:10px;opacity:.7">${rejObj.rejectedAt||''}</span></div></div>`;}
    else if(achieved){btnHtml=`<span style="font-size:11px;color:var(--tx3);background:var(--sf2);padding:2px 8px;border-radius:8px">주간 달성</span>`;}
    else{btnHtml=`<button class="btn bsm" style="color:var(--pu);border-color:var(--pu)" onclick="openExcuseM('${empno}','${dt}','${esc(person.name)}','${esc(person.site)}','${esc(person.dept)}',${weekLogCnt},undefined,'${from}','${to}');G.drill={empno:'${empno}',_iv:true}">소명 입력</button>`;}
    return `<div class="miss-item"><span class="miss-date">${dt} (${dn})</span>${btnHtml}</div>`;
  }).join(''):'<div style="color:#0a3f75;font-size:13px;padding:8px 0">미접속 날짜 없음 🎉</div>';
  document.getElementById('iv-empty').style.display='none';document.getElementById('iv-result').style.display='block';
}

function srt(k){G.fullSort=G.fullSort.k===k?{k,a:!G.fullSort.a}:{k,a:true};G.pages.fl=1;rFull();}
function onFlsChange(){const sf=document.getElementById('fls').value;const depts=[...new Set(G.result.filter(r=>!sf||r.site===sf).map(r=>r.dept))].sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});const fld=document.getElementById('fld');const cur=fld.value;fld.innerHTML='<option value="">전체</option>'+depts.map(d=>`<option${d===cur?' selected':''}>${d}</option>`).join('');if(!depts.includes(cur))fld.value='';rFull();}
function rFull(){
  const sf=document.getElementById('fls').value,df=document.getElementById('fld').value,pf=document.getElementById('flp').value,stf=document.getElementById('flst').value,q=document.getElementById('flq').value.toLowerCase();
  const fp=document.getElementById('flp');const fpc=G._jumpOpts?.period||fp.value;
  const periodMap=new Map();G.result.forEach(r=>{if(!periodMap.has(r.periodKey))periodMap.set(r.periodKey,r.period);});
  const orderedKeys=[...G.allPeriods.map(p=>p.key),...[...periodMap.keys()].filter(k=>!G.allPeriods.some(p=>p.key===k))];
  fp.innerHTML='<option value="">전체</option>'+orderedKeys.filter(k=>periodMap.has(k)).map(k=>`<option value="${k}"${k===fpc?' selected':''}>${periodMap.get(k)}</option>`).join('');fp.value=fpc;
  const fld=document.getElementById('fld');const fdc=fld.value;const depts=[...new Set(G.result.filter(r=>!sf||r.site===sf).map(r=>r.dept))].sort((a,b)=>{const ia=deptIdx(a),ib=deptIdx(b);return ia!==ib?ia-ib:a.localeCompare(b,'ko');});
  fld.innerHTML='<option value="">전체</option>'+depts.map(d=>`<option${d===fdc?' selected':''}>${d}</option>`).join('');
  let rows=G.result.filter(r=>{if(sf&&r.site!==sf)return false;if(df&&r.dept!==df)return false;if(pf&&r.periodKey!==pf)return false;if(stf==='ok'&&!r.ok)return false;if(stf==='fail'&&(r.ok||r.total===0))return false;if(stf==='zero'&&r.total!==0)return false;if(q&&!r.name.includes(q)&&!r.dept.toLowerCase().includes(q)&&!r.empno.includes(q))return false;return true;});
  const{k,a}=G.fullSort;rows.sort((x,y)=>{let v;if(k==='ok')v=Number(x.ok)-Number(y.ok);else if(['cnt','excCnt','total'].includes(k))v=x[k]-y[k];else v=String(x[k]||'').localeCompare(String(y[k]||''),undefined,{numeric:true});return a?v:-v;});
  const tot=rows.length,ok=rows.filter(r=>r.ok).length,rate=tot?Math.round(ok/tot*100):0;
  document.getElementById('flsum-rate').textContent='달성률 '+rate+'%';document.getElementById('flsum-rate').style.color=rate>=80?'#0a3f75':rate>=50?'var(--bl)':'var(--rd)';
  document.getElementById('flsum').textContent=tot.toLocaleString()+'명 (달성 '+ok+' / 미달성 '+(tot-ok)+')';
  const pg=G.pages.fl,ps=G.ps,pd=rows.slice((pg-1)*ps,pg*ps);
  document.getElementById('flbody').innerHTML=pd.length?pd.map(r=>{const col=r.ok?'#1e7a40':r.total>=1?'#b45309':'var(--rd)';const pct=Math.round(r.total/(r.kpi||KPI_WEEK)*100);const b=r.ok?'<span class="bdg bok">달성</span>':r.total>=1?'<span class="bdg bwn">미달성</span>':'<span class="bdg bfl">미접속</span>';return `<tr><td>${r.site}</td><td>${r.dept}</td><td style="color:var(--tx2);font-size:12px">${r.empno}</td><td><span class="row-link" onclick="openDrill('${r.empno}')">${r.name}</span></td><td style="color:var(--tx2)">${r.period}</td><td style="font-weight:600;color:var(--tx2)">${r.cnt}</td><td>${r.excCnt?`<span class="bdg bex">${r.excCnt}</span>`:'-'}</td><td><div class="mb"><span style="min-width:20px;text-align:right;font-weight:700;color:${col}">${r.total}</span><div class="mbg"><div class="mbf" style="width:${Math.min(100,pct)}%;background:${col}"></div></div></div></td><td style="color:var(--tx3)">${r.kpi||KPI_WEEK}</td><td>${b}</td><td><button class="btn bsm" style="font-size:11px;padding:2px 7px" onclick="openDrill('${r.empno}','${r.periodKey}')">상세</button></td></tr>`;}).join(''):'<tr><td colspan="11" style="text-align:center;padding:26px;color:var(--tx3)">데이터 없음</td></tr>';
  pagi('flpagi',rows.length,ps,pg,p=>{G.pages.fl=p;rFull();});
}
function expCSV(){const sf=document.getElementById('fls').value,pf=document.getElementById('flp').value;const rows=G.result.filter(r=>!r.ok&&(!sf||r.site===sf)&&(!pf||r.periodKey===pf));if(!rows.length){toast('미달성자 없음');return;}const bom='\uFEFF',h='사업장,부서명,사번,성명,기간,실접속,면제,합계,미달성\n';const b=rows.map(r=>`${r.site},${r.dept},${r.empno},${r.name},${r.period},${r.cnt},${r.excCnt},${r.total},${(r.kpi||KPI_WEEK)-r.total}`).join('\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([bom+h+b],{type:'text/csv;charset=utf-8'}));a.download='ESH_미달성_'+(pf||'전체')+'_'+(sf||'전사')+'.csv';a.click();toast('CSV 다운로드');}

/* ── 사유확인 페이지 ── */
let SITE_PWS={},exSiteUnlocked=new Set(),EXCUSE_PAGE_PW='',exPageUnlocked=false;
function initExcusePage(){if(!EXCUSE_PAGE_PW||exPageUnlocked||G.adminMode){showExcuseContent();}else{showExcuseLock();}}
function showExcuseLock(){document.getElementById('ex-page-lock').style.display='flex';document.getElementById('ex-page-content').style.display='none';document.getElementById('ex-page-pw-hint').textContent='';setTimeout(()=>document.getElementById('ex-page-pw-inp').focus(),100);}
function showExcuseContent(){document.getElementById('ex-page-lock').style.display='none';document.getElementById('ex-page-content').style.display='block';initExPeriodSel();rExcuse();}
function checkExPagePw(){
  const locked=PW_LOCK.isLocked('expage');
  if(locked){document.getElementById('ex-page-pw-hint').textContent='🔒 잠금 중 — '+PW_LOCK.remainMsg(locked)+' 후 재시도';return;}
  const v=document.getElementById('ex-page-pw-inp').value;
  if(v===EXCUSE_PAGE_PW){
    PW_LOCK.success('expage');
    exPageUnlocked=true;document.getElementById('ex-page-pw-inp').value='';showExcuseContent();toast('✓ 사유확인 페이지 접근 허용');
  }else{
    const{attempts,lockedUntil}=PW_LOCK.fail('expage');
    const remain=PW_LOCK.MAX-attempts;
    if(lockedUntil){document.getElementById('ex-page-pw-hint').textContent='🔒 3회 실패 — 30분간 잠금됩니다';}
    else{document.getElementById('ex-page-pw-hint').textContent='비밀번호 오류 (남은 시도: '+remain+'회)';}
    document.getElementById('ex-page-pw-inp').value='';document.getElementById('ex-page-pw-inp').focus();
  }
}
function admSaveExPagePw(){const v=document.getElementById('adm-ex-page-pw').value;EXCUSE_PAGE_PW=v;exPageUnlocked=false;saveSettingsToSheets();toast(v?'✓ 비밀번호 저장됨':'✓ 잠금 해제');}

function initExPeriodSel(){const type=document.getElementById('ex-period-type').value;const sel=document.getElementById('ex-period-sel');const cust=document.getElementById('ex-period-custom');if(!type){sel.style.display='none';cust.style.display='none';return;}if(type==='custom'){sel.style.display='none';cust.style.display='flex';return;}cust.style.display='none';const dates=[...G.rawLog.keys()].map(k=>k.split('|')[1]).sort();if(!dates.length){sel.innerHTML='<option>데이터 없음</option>';sel.style.display='';return;}const origPT=G.periodType;G.periodType=type;const ps=buildPeriods(new Date(dates[0]),new Date(dates[dates.length-1]));G.periodType=origPT;const cur=sel.value;sel.innerHTML=ps.map(p=>`<option value="${p.from}|${p.to}">${p.label}</option>`).join('');if(ps.length)sel.selectedIndex=ps.length-1;sel.style.display='';}
function onExPeriodTypeChange(){initExPeriodSel();rExcuse();}

function admRenderSitePwList(){const sites=sitesSorted().length?sitesSorted():[...new Set(G.ref.map(r=>r.site))].sort();const el=document.getElementById('adm-site-pw-list');if(!el)return;if(!sites.length){el.innerHTML='<div style="color:var(--tx3);font-size:12px">대상자 정보를 먼저 로드하세요.</div>';return;}el.innerHTML=sites.map(s=>`<div class="site-pw-row"><span class="site-pw-nm">${esc(s)}</span><input class="site-pw-inp" type="password" id="spw_${esc(s)}" placeholder="비밀번호 없음" value="${esc(SITE_PWS[s]||'')}"><button class="btn bsm" onclick="togglePwVis('spw_${esc(s)}')">👁</button></div>`).join('');}
function togglePwVis(id){const el=document.getElementById(id);if(el)el.type=el.type==='password'?'text':'password';}
function admSaveSitePws(){const sites=sitesSorted().length?sitesSorted():[...new Set(G.ref.map(r=>r.site))].sort();const obj={};sites.forEach(s=>{const v=(document.getElementById('spw_'+s)||{}).value||'';if(v)obj[s]=v;});SITE_PWS=obj;exSiteUnlocked=new Set();saveSettingsToSheets();toast('✓ 사업장 비밀번호 저장됨');rExcuse();}

/* ── ★ rExcuse: 상태탭 + 최대20건 스크롤 ── */
function setExTab(site,tab){G_exTab[site]=tab;rExcuse();}

function rExcuse(){
  const sfFilter=document.getElementById('exs').value;const q=document.getElementById('exq').value.toLowerCase();
  const periodType=document.getElementById('ex-period-type').value;let dateFrom='',dateTo='';
  if(periodType==='custom'){dateFrom=document.getElementById('ex-date-from').value;dateTo=document.getElementById('ex-date-to').value;}
  else if(periodType==='week'||periodType==='month'){const sel=document.getElementById('ex-period-sel');if(sel.value){const[f,t]=sel.value.split('|');dateFrom=f;dateTo=t;}}
  const exsSel=document.getElementById('exs');const curEx=exsSel.value;
  const allSites=sitesSorted().length?sitesSorted():[...new Set(G.ref.map(r=>r.site))].sort((a,b)=>siteIdx(a)-siteIdx(b));
  exsSel.innerHTML='<option value="">전체</option>'+allSites.map(s=>`<option${s===curEx?' selected':''}>${s}</option>`).join('');
  // 전체 소명 목록
  const allRows=[...[...G.excuses.values()].map(r=>({...r,_status:r._status||'approved'})),...[...G.pendingExcuses.values()].filter(r=>r._status==='pending'||r._status==='rejected')].filter(r=>{
    if(sfFilter&&r.site!==sfFilter)return false;
    if(q&&!r.name.includes(q)&&!r.empno.includes(q))return false;
    if(dateFrom&&r.date<dateFrom)return false;if(dateTo&&r.date>dateTo)return false;return true;
  });
  const pending=allRows.filter(r=>r._status==='pending').length;const approved=allRows.filter(r=>r._status==='approved').length;const rejected=allRows.filter(r=>r._status==='rejected').length;
  document.getElementById('exsum').textContent='전체 '+allRows.length+'건 · 대기 '+pending+' · 승인 '+approved+' · 반려 '+rejected;
  const targetSites=sfFilter?[sfFilter]:allSites;
  const container=document.getElementById('ex-site-sections');
  if(!targetSites.length){container.innerHTML='<div style="text-align:center;padding:40px;color:var(--tx3)">데이터 없음</div>';return;}

  const MAX_VISIBLE=20;
  container.innerHTML=targetSites.map(site=>{
    const siteAllRows=allRows.filter(r=>r.site===site).sort((a,b)=>b.date.localeCompare(a.date));
    const sp=siteAllRows.filter(r=>r._status==='pending').length;
    const sa=siteAllRows.filter(r=>r._status==='approved').length;
    const sr=siteAllRows.filter(r=>r._status==='rejected').length;
    const currentTab=G_exTab[site]||'all';
    const tabRows=currentTab==='all'?siteAllRows:siteAllRows.filter(r=>r._status===(currentTab==='approved'?'approved':currentTab==='pending'?'pending':'rejected'));
    const visibleRows=tabRows.slice(0,MAX_VISIBLE);
    const hasMore=tabRows.length>MAX_VISIBLE;
    const locked=SITE_PWS[site]&&!exSiteUnlocked.has(site)&&!G.adminMode;
    const canApprove=G.adminMode||(SITE_PWS[site]&&exSiteUnlocked.has(site));

    const tableRows=visibleRows.map(r=>{
      const statusBadge=r._status==='pending'?`<span class="excuse-pend-badge">⏳ 대기</span>`:r._status==='rejected'?`<span style="font-size:11px;background:var(--rdl);color:var(--rd);padding:1px 7px;border-radius:10px;font-weight:600">✕ 반려</span>`:`<span style="font-size:11px;background:var(--gnl);color:var(--gnd);padding:1px 7px;border-radius:10px;font-weight:600">✓ 승인</span>`;
      let approveBtn='',rejectBtn='';
      if(r._status==='pending'&&canApprove){approveBtn=`<button class="btn bp bsm" onclick="exApprove('${r.key}')">승인</button>`;rejectBtn=`<button class="btn bsm brd" onclick="exReject('${r.key}')">반려</button>`;}
      else if(r._status==='approved'&&canApprove){approveBtn=`<span style="font-size:11px;color:var(--tx3)">-</span>`;rejectBtn=`<button class="btn bsm brd" onclick="delExcuse('${r.key}')">취소</button>`;}
      else if(r._status==='rejected'&&canApprove){approveBtn=`<button class="btn bp bsm" onclick="exApprove('${r.key}')">승인</button>`;rejectBtn=`<span style="font-size:11px;color:var(--tx3)">-</span>`;}
      else{approveBtn=`<span style="font-size:11px;color:var(--tx3)">-</span>`;rejectBtn=`<span style="font-size:11px;color:var(--tx3)">-</span>`;}
      const reasonIcon=r.reason==='휴가'?'🌴':r.reason==='병가'?'🏥':r.reason==='출장'?'✈️':'📝';
      return `<tr><td style="font-size:12px;color:var(--tx2)">${esc(r.dept||'-')}</td><td style="font-size:12px;color:var(--tx2)">${r.empno}</td><td><span class="row-link" onclick="openDrill('${r.empno}')">${esc(r.name)}</span></td><td style="font-weight:600;white-space:nowrap">${fmtDateKo(r.date)}</td><td style="font-size:12px">${reasonIcon} ${esc(r.reason)}</td><td style="font-size:12px;color:var(--tx2)">${esc(r.regBy||'-')}</td><td style="font-size:11px;color:var(--tx3)">${r.regAt||''}</td><td>${statusBadge}</td><td style="text-align:center;background:#f7faff"><button class="btn bsm" style="font-size:11px;padding:2px 8px;color:var(--bl);border-color:var(--bl)" onclick="openExDetail('${r.key}')">확인</button></td><td style="text-align:center;background:#f7fff9">${approveBtn}</td><td style="text-align:center;background:#f7fff9">${rejectBtn}</td></tr>`;
    }).join('');

    const lockedMsg=`<div style="text-align:center;padding:16px;color:var(--tx3);font-size:13px">🔒 비밀번호 인증 후 열람 가능</div>`;
    const tableHtml=`<div class="ex-tbl-wrap"><div class="ex-tbl-scroll"><table>
      <thead><tr>
        <th rowspan="2">부서명</th><th rowspan="2">사번</th><th rowspan="2">성명</th><th rowspan="2">날짜</th>
        <th rowspan="2">사유</th><th rowspan="2">신청자</th><th rowspan="2">등록일시</th><th rowspan="2">상태</th>
        <th colspan="1" style="text-align:center;background:#eef4fb;color:var(--bl);border-bottom:1px solid var(--bd)">확인</th>
        <th colspan="2" style="text-align:center;background:#f0faf4;color:var(--gnd);border-bottom:1px solid var(--bd)">처리</th>
      </tr><tr>
        <th style="text-align:center;background:#eef4fb;font-size:11px;color:var(--bl)">상세</th>
        <th style="text-align:center;background:#f0faf4;font-size:11px;color:var(--gnd)">승인</th>
        <th style="text-align:center;background:#f0faf4;font-size:11px;color:var(--rd)">반려/취소</th>
      </tr></thead>
      <tbody>${tableRows||'<tr><td colspan="11" style="text-align:center;padding:16px;color:var(--tx3)">해당 건 없음</td></tr>'}</tbody>
    </table></div>${hasMore?`<div class="ex-more-bar">📋 총 ${tabRows.length}건 중 ${MAX_VISIBLE}건 표시 · 스크롤하여 확인하세요</div>`:''}</div>`;

    const statTabs=`<div class="ex-stat-tabs">
      <button class="ex-stat-tab${currentTab==='all'?' on':''}" onclick="setExTab('${esc(site)}','all')">전체 <span class="ex-stat-cnt all">${siteAllRows.length}</span></button>
      <button class="ex-stat-tab${currentTab==='pending'?' on':''}" onclick="setExTab('${esc(site)}','pending')">⏳ 대기 <span class="ex-stat-cnt pend">${sp}</span></button>
      <button class="ex-stat-tab${currentTab==='approved'?' on':''}" onclick="setExTab('${esc(site)}','approved')">✓ 승인 <span class="ex-stat-cnt ok">${sa}</span></button>
      <button class="ex-stat-tab${currentTab==='rejected'?' on':''}" onclick="setExTab('${esc(site)}','rejected')">✕ 반려 <span class="ex-stat-cnt rej">${sr}</span></button>
    </div>`;

    return `<div class="ex-site-block">
      <div class="ex-site-hd">
        <span class="ex-site-nm">${esc(site)}</span>
        <span class="ex-site-stat">신청: <span>${sp+sa+sr}건</span> &nbsp; 승인: <span class="s-ok">${sa}건</span> &nbsp; 대기: <span class="s-pend">${sp}건</span> &nbsp; 반려: <span class="s-rej">${sr}건</span></span>
        ${SITE_PWS[site]&&!G.adminMode&&!exSiteUnlocked.has(site)?`<button class="btn bsm" style="margin-left:auto;font-size:11px" onclick="exUnlockSite('${esc(site)}')">🔓 인증</button>`:''}
      </div>
      ${locked?lockedMsg:statTabs+tableHtml}
    </div>`;
  }).join('');
}

function exUnlockSite(site){
  const lockId='site_'+site;
  const locked=PW_LOCK.isLocked(lockId);
  if(locked){toast('🔒 '+site+' 잠금 중 — '+PW_LOCK.remainMsg(locked)+' 후 재시도');return;}
  const pw=prompt('['+site+'] 비밀번호를 입력하세요');if(pw===null)return;
  if(SITE_PWS[site]&&pw===SITE_PWS[site]){
    PW_LOCK.success(lockId);exSiteUnlocked.add(site);rExcuse();toast('✓ '+site+' 인증 완료');
  }else{
    const{attempts,lockedUntil}=PW_LOCK.fail(lockId);
    if(lockedUntil){toast('🔒 '+site+' 3회 실패 — 30분간 잠금됩니다');}
    else{toast('⚠ 비밀번호 오류 (남은 시도: '+(PW_LOCK.MAX-attempts)+'회)');}
  }
}

function openExDetail(key){
  const r=G.pendingExcuses.get(key)||G.excuses.get(key);if(!r){toast('소명 데이터를 찾을 수 없습니다');return;}
  document.getElementById('exd-av').textContent=(r.name||'?')[0];document.getElementById('exd-name').textContent=r.name||'-';document.getElementById('exd-meta').textContent=(r.empno||'')+' · '+(r.site||'')+' · '+(r.dept||'');
  const badgeMap={pending:`<span class="excuse-pend-badge" style="font-size:12px;padding:3px 10px">⏳ 승인 대기</span>`,approved:`<span style="font-size:12px;background:var(--gnl);color:var(--gnd);padding:3px 10px;border-radius:20px;font-weight:700">✓ 승인완료</span>`,rejected:`<span style="font-size:12px;background:var(--rdl);color:var(--rd);padding:3px 10px;border-radius:20px;font-weight:700">✕ 반려</span>`};
  document.getElementById('exd-status-badge').innerHTML=badgeMap[r._status]||'';
  document.getElementById('exd-date').textContent=normalizeDate(String(r.date||''))||r.date||'-';
  const _nd=normalizeDate(String(r.date||''));const dow=['일','월','화','수','목','금','토'][new Date(_nd).getDay()];document.getElementById('exd-dow').textContent=_nd?'('+dow+'요일)':'';
  document.getElementById('exd-reason').textContent=r.reason||'(내용 없음)';document.getElementById('exd-regby').textContent=r.regBy||'-';document.getElementById('exd-regat').textContent=r.regAt||'-';
  let histHtml='';if(r.approvedAt)histHtml+=`<div style="font-size:12px;color:var(--gnd);background:var(--gnl);border-radius:var(--rs);padding:7px 11px;margin-bottom:6px">✓ 승인 처리: ${r.approvedAt}</div>`;if(r.rejectedAt)histHtml+=`<div style="font-size:12px;color:var(--rd);background:var(--rdl);border-radius:var(--rs);padding:7px 11px;margin-bottom:6px">✕ 반려: ${r.rejectedAt}${r.rejectReason?'<div style="margin-top:5px;font-weight:600">반려 사유: '+esc(r.rejectReason)+'</div>':''}</div>`;
  document.getElementById('exd-history').innerHTML=histHtml;
  const canApprove=G.adminMode||(SITE_PWS[r.site]&&exSiteUnlocked.has(r.site));
  const delBtn=`<button class="btn bsm" style="color:#888;border-color:#ccc;margin-right:auto" onclick="if(confirm('이 소명을 삭제할까요?\\n삭제 후 복구할 수 없습니다.')){closeM('ex-detail-ov');delExcuse('${key}');}">🗑 삭제</button>`;
  let actionHtml=`<button class="btn" onclick="closeM('ex-detail-ov')">닫기</button>`;
  if(canApprove){if(r._status==='pending'){actionHtml=`${delBtn}<button class="btn" onclick="closeM('ex-detail-ov')">닫기</button><button class="btn bsm brd" onclick="closeM('ex-detail-ov');exReject('${key}')">✕ 반려</button><button class="btn bp" onclick="closeM('ex-detail-ov');exApprove('${key}')">✓ 승인</button>`;}else if(r._status==='approved'){actionHtml=`${delBtn}<button class="btn" onclick="closeM('ex-detail-ov')">닫기</button><button class="btn bsm brd" onclick="closeM('ex-detail-ov');delExcuse('${key}')">승인 취소</button>`;}else if(r._status==='rejected'){actionHtml=`${delBtn}<button class="btn" onclick="closeM('ex-detail-ov')">닫기</button><button class="btn bp" onclick="closeM('ex-detail-ov');exApprove('${key}')">✓ 승인으로 전환</button>`;}}
  document.getElementById('exd-action-btns').innerHTML=actionHtml;openM('ex-detail-ov');
}

async function exApprove(key){
  const pend=G.pendingExcuses.get(key);if(!pend){toast('항목을 찾을 수 없습니다');return;}
  const approved={...pend,_status:'approved',approvedAt:new Date().toLocaleString('ko-KR')};
  G.excuses.set(key,approved);G.pendingExcuses.delete(key);
  if(G.url){const res=await api(null,{action:'saveExcuse',data:approved});toast(res&&!res.error?'✓ 승인 완료 — '+pend.name+' '+pend.date:'⚠ Sheets 저장 실패');}else{toast('✓ 승인 — '+pend.name+' '+pend.date);}
  recomp();rExcuse();
}
async function exReject(key){const pend=G.pendingExcuses.get(key);if(!pend){toast('항목을 찾을 수 없습니다');return;}G._rejectTarget=key;document.getElementById('rej-info').innerHTML=`<strong>${esc(pend.name)}</strong> (${pend.empno}) · ${esc(pend.site)} · ${esc(pend.dept)}<br><strong style="color:var(--rd)">${pend.date}</strong> · ${esc(pend.reason)}`;document.getElementById('rej-reason').value='';openM('reject-ov');}
async function submitReject(){const reason=document.getElementById('rej-reason').value.trim();if(!reason){toast('반려 사유를 입력하세요');return;}const key=G._rejectTarget;const pend=G.pendingExcuses.get(key);if(!pend){toast('항목 없음');closeM('reject-ov');return;}const rejected={...pend,_status:'rejected',rejectReason:reason,rejectedAt:new Date().toLocaleString('ko-KR')};G.pendingExcuses.set(key,rejected);closeM('reject-ov');if(G.url){const res=await api(null,{action:'saveExcuse',data:rejected});toast(res&&!res.error?'반려 처리 완료 — '+pend.name+' '+pend.date:'⚠ Sheets 저장 실패');}else{toast('반려 처리 — '+pend.name+' '+pend.date);}rExcuse();if(G.drill)openDrill(G.drill.empno);}

let G_refSort={k:'site',a:true};
function onMgsiteChange(){const sf=document.getElementById('mgsite').value;const depts=[...new Set(G.ref.filter(r=>!sf||r.site===sf).map(r=>r.dept))].sort((a,b)=>deptIdx(a)-deptIdx(b)||a.localeCompare(b,'ko'));const md=document.getElementById('mgdept');const cur=md.value;md.innerHTML='<option value="">전체 부서</option>'+depts.map(d=>`<option${d===cur?' selected':''}>${d}</option>`).join('');if(!depts.includes(cur))md.value='';rRef();}
function srtRef(k){G_refSort=G_refSort.k===k?{k,a:!G_refSort.a}:{k,a:true};rRef();}
function rRef(){
  const q=document.getElementById('mgq').value.toLowerCase();const sf=document.getElementById('mgsite').value;const df=document.getElementById('mgdept')?.value||'';
  let flt=G.ref.filter(r=>(!sf||r.site===sf)&&(!df||r.dept===df)&&(!q||r.name.includes(q)||r.empno.includes(q)||r.dept.toLowerCase().includes(q)));
  const{k,a}=G_refSort;flt=[...flt].sort((x,y)=>{const v=k==='site'?siteIdx(x.site)-siteIdx(y.site)||x.site.localeCompare(y.site,'ko'):k==='dept'?deptIdx(x.dept)-deptIdx(y.dept)||x.dept.localeCompare(y.dept,'ko'):String(x[k]||'').localeCompare(String(y[k]||''),undefined,{numeric:true});return a?v:-v;});
  document.getElementById('mgcnt').textContent=(sf||df)?flt.length+'명 / '+G.ref.length+'명':G.ref.length+'명';
  const sites=[...new Set(G.ref.map(r=>r.site))].sort((a,b)=>siteIdx(a)-siteIdx(b));
  const ms=document.getElementById('mgsite');const cur=ms.value;ms.innerHTML='<option value="">전체</option>'+sites.map(s=>`<option${s===cur?' selected':''}>${s}</option>`).join('');
  const depts=[...new Set(G.ref.filter(r=>!sf||r.site===sf).map(r=>r.dept))].sort((a,b)=>deptIdx(a)-deptIdx(b)||a.localeCompare(b,'ko'));
  const md=document.getElementById('mgdept');if(md){const dc=md.value;md.innerHTML='<option value="">전체 부서</option>'+depts.map(d=>`<option${d===dc?' selected':''}>${d}</option>`).join('');}
  const pg=G.pages.mg,ps=G.ps,pdd=flt.slice((pg-1)*ps,pg*ps),off=(pg-1)*ps;const admin=G.adminMode;
  document.getElementById('mgbody').innerHTML=pdd.map((r,i)=>`<tr><td style="color:var(--tx3);font-size:12px">${off+i+1}</td>${admin?`<td class="ed"><input class="ii" value="${esc(r.site)}" onchange="upd('${r.id}','site',this.value)"></td><td class="ed"><input class="ii" value="${esc(r.dept)}" onchange="upd('${r.id}','dept',this.value)"></td><td class="ed"><input class="ii" value="${esc(r.empno)}" onchange="upd('${r.id}','empno',this.value)"></td><td class="ed"><input class="ii" value="${esc(r.name)}" onchange="upd('${r.id}','name',this.value)"></td><td class="ed"><input class="ii" value="${esc(r.note||'')}" onchange="upd('${r.id}','note',this.value)"></td><td><button class="xb" onclick="del('${r.id}')">✕</button></td>`:`<td>${esc(r.site)}</td><td>${esc(r.dept)}</td><td style="color:var(--tx2)">${esc(r.empno)}</td><td>${esc(r.name)}</td><td style="color:var(--tx2)">${esc(r.note||'-')}</td>`}</tr>`).join('');
  document.getElementById('syncinfo').innerHTML=G.sync?`<span class="dot ok"></span>동기화: ${G.sync}`:'<span class="dot"></span>미동기화';
  pagi('mgpagi',flt.length,ps,pg,p=>{G.pages.mg=p;rRef();});
}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function upd(id,f,v){const r=G.ref.find(r=>String(r.id)===String(id));if(r)r[f]=v.trim();}
function del(id){if(!confirm('삭제?'))return;G.ref=G.ref.filter(r=>String(r.id)!==String(id));rRef();toast('삭제됨');}
function addRow(){G.ref.push({id:'n'+Date.now(),site:'',dept:'',empno:'',name:'',note:''});G.pages.mg=Math.ceil(G.ref.length/G.ps);rRef();}
function fmtD(d){return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());}
function pad(n){return String(n).padStart(2,'0');}
function wkStart(d){const t=new Date(d);t.setDate(t.getDate()-(t.getDay()||7)+1);t.setHours(0,0,0,0);return t;}
function isoWk(d){const t=new Date(d);t.setHours(0,0,0,0);t.setDate(t.getDate()+3-(t.getDay()||7)-3);const y=t.getFullYear(),j=new Date(y,0,1);return y+'-W'+pad(Math.ceil(((t-j)/864e5+1)/7));}

const LD={startTime:0,steps:{'설정 로드 중...':{pct:15,sub:'LOADING CONFIGURATION'},'대상자 · 사유 데이터 로드 중...':{pct:35,sub:'FETCHING MEMBER & EXCUSE DATA'},'접속로그 로드 중...':{pct:85,sub:'SYNCING ACCESS LOGS'},'Sheets 동기화 중...':{pct:92,sub:'SYNCING TO SHEETS'}},cur:0,timer:null};
function ldSetProgress(pct,remainTxt){const bar=document.getElementById('ld-bar');const pctEl=document.getElementById('ld-pct');const remEl=document.getElementById('ld-remain');if(bar)bar.style.width=pct+'%';if(pctEl)pctEl.textContent=Math.round(pct)+'%';if(remEl)remEl.textContent=remainTxt||'';}
function ldStartTick(targetPct){if(LD.timer)clearInterval(LD.timer);const stepStart=Date.now();LD.timer=setInterval(()=>{const elapsed=Date.now()-stepStart;const prog=LD.cur+(targetPct-LD.cur)*(1-Math.exp(-elapsed/2000));const totalElapsed=(Date.now()-LD.startTime)/1000;const speed=(prog/100)/totalElapsed;const remain=speed>0?Math.max(0,Math.round((1-prog/100)/speed)):null;ldSetProgress(prog,remain===null?'계산 중...':remain===0?'거의 완료...':remain<60?'약 '+remain+'초 남음':'약 '+Math.round(remain/60)+'분 남음');},100);}
function setSt(t,txt){
  const d=document.getElementById('sdot');if(d)d.className='dot'+(t==='ok'?' ok':t==='warn'?' warn':'');const stEl=document.getElementById('stxt');if(stEl)stEl.textContent=txt;
  const ov=document.getElementById('loading-ov');if(!ov)return;
  if(t==='warn'){const step=LD.steps[txt];const ldTxt=document.getElementById('ld-txt');const ldSub=document.getElementById('ld-sub');if(ldTxt)ldTxt.childNodes[0].textContent=txt||'데이터 로드 중';if(ldSub)ldSub.textContent=step?step.sub:'CONNECTING TO ESH SYSTEM';if(!LD.startTime)LD.startTime=Date.now();const targetPct=step?step.pct:50;LD.cur=Math.max(LD.cur,0);ldStartTick(targetPct);ov.classList.remove('hide');}
  else{if(LD.timer)clearInterval(LD.timer);LD.timer=null;ov.classList.add('hide');LD.startTime=0;LD.cur=0;ldSetProgress(0,'');}
}
function toast(msg){const el=document.getElementById('toast');el.textContent=msg;el.classList.add('show');clearTimeout(el._t);el._t=setTimeout(()=>el.classList.remove('show'),2600);}
function pagi(id,tot,ps,cur,cb){const pages=Math.ceil(tot/ps),el=document.getElementById(id);if(pages<=1){el.innerHTML='';return;}const s=Math.max(1,cur-2),en=Math.min(pages,cur+2);let h=`<span class="pi">${tot.toLocaleString()}건</span>`;if(s>1)h+=`<button class="pg" onclick="(${cb})(1)">1</button>${s>2?'<span class="pi">…</span>':''}`;for(let p=s;p<=en;p++)h+=`<button class="pg${p===cur?' on':''}" onclick="(${cb})(${p})">${p}</button>`;if(en<pages)h+=`${en<pages-1?'<span class="pi">…</span>':''}<button class="pg" onclick="(${cb})(${pages})">${pages}</button>`;el.innerHTML=h;}

function loadNotice(){const obj=G.notice||null;const bar=document.getElementById('notice-bar');if(!obj||!obj.txt){bar.classList.remove('show');return;}const hasTtl=!!obj.ttl;document.getElementById('notice-ttl-display').textContent=obj.ttl||'';document.getElementById('notice-ttl-display').style.display=hasTtl?'':'none';document.getElementById('notice-sep-display').style.display=hasTtl?'':'none';document.getElementById('notice-txt-display').textContent=obj.txt;document.getElementById('notice-meta-display').textContent=obj.regAt||'';bar.classList.add('show');}
function admSaveNotice(){const ttl=document.getElementById('adm-notice-ttl').value.trim();const txt=document.getElementById('adm-notice-txt').value.trim();if(!txt){toast('내용을 입력하세요');return;}const regAt=new Date().toLocaleString('ko-KR');G.notice={ttl,txt,regAt};loadNotice();document.getElementById('adm-notice-status').textContent='게시됨 · '+regAt;saveSettingsToSheets();toast('✓ 공지사항 게시 완료');}
function admClearNotice(){if(!confirm('공지사항을 삭제할까요?'))return;G.notice=null;document.getElementById('notice-bar').classList.remove('show');document.getElementById('adm-notice-ttl').value='';document.getElementById('adm-notice-txt').value='';document.getElementById('adm-notice-status').textContent='';saveSettingsToSheets();toast('✓ 공지사항 삭제됨');}

function initAdminPage(){const tm=G.titleMain||'ESH',ts=G.titleSub||' KPI';document.getElementById('adm-title-main').value=tm;document.getElementById('adm-title-sub').value=ts;document.getElementById('adm-title-preview').textContent=tm+ts;document.getElementById('adm-title-main').oninput=admPreviewTitle;document.getElementById('adm-title-sub').oninput=admPreviewTitle;document.getElementById('adm-kpi-week').value=KPI_WEEK;document.getElementById('adm-kpi-start').value=KPI_START_DATE||'';document.getElementById('adm-site-order').value=SITE_ORDER.join('\n');document.getElementById('adm-dept-order').value=DEPT_ORDER.join('\n');document.getElementById('adm-url').value=G.url||'';document.getElementById('adm-ex-page-pw').value=EXCUSE_PAGE_PW||'';document.getElementById('adm-pw1').value='';document.getElementById('adm-pw2').value='';admRenderSitePwList();admRenderCloseMonthGrid();const n=G.notice;if(n){document.getElementById('adm-notice-ttl').value=n.ttl||'';document.getElementById('adm-notice-txt').value=n.txt||'';document.getElementById('adm-notice-status').textContent=n.regAt?'게시 중 · '+n.regAt:'';}else{document.getElementById('adm-notice-ttl').value='';document.getElementById('adm-notice-txt').value='';document.getElementById('adm-notice-status').textContent='';}}
function admPreviewTitle(){const m=document.getElementById('adm-title-main').value||'ESH';const s=document.getElementById('adm-title-sub').value||' KPI';document.getElementById('adm-title-preview').textContent=m+s;}

async function saveSettingsToSheets(){if(!G.url)return;const data={esh_title_main:G.titleMain||'ESH',esh_title_sub:G.titleSub||' KPI',esh_kpi_week:String(KPI_WEEK),esh_kpi_start:KPI_START_DATE||'',esh_site_order:JSON.stringify(SITE_ORDER),esh_dept_order:JSON.stringify(DEPT_ORDER),esh_admin_pw:ADMIN_PW||'1234',esh_ex_page_pw:EXCUSE_PAGE_PW||'',esh_site_pws:JSON.stringify(SITE_PWS||{}),esh_notice:G.notice?JSON.stringify(G.notice):'',esh_closed_months:JSON.stringify([...CLOSED_MONTHS])};await api(null,{action:'saveSettings',data});}
async function loadSettingsFromSheets(){if(!G.url)return;const d=await api({action:'getSettings'});_applySettings(d);}

function admSaveTitle(){const main=document.getElementById('adm-title-main').value.trim()||'ESH';const sub=document.getElementById('adm-title-sub').value||' KPI';G.titleMain=main;G.titleSub=sub;applyTitle(main,sub);saveSettingsToSheets();toast('✓ 타이틀 저장됨');}
async function admSaveKPI(){const v=parseInt(document.getElementById('adm-kpi-week').value,10);if(isNaN(v)||v<1){toast('⚠ 올바른 숫자 입력');return;}KPI_WEEK=v;const rawDate=document.getElementById('adm-kpi-start').value||'';const startDate=normalizeDate(rawDate);KPI_START_DATE=startDate;lcClear();G.logCache=null;await saveSettingsToSheets();toast('✓ KPI 설정 저장 — '+v+'회/주'+(startDate?' · 시작일 '+startDate:''));await pullAll();toast('✓ 완료');}
function admSaveSiteOrder(){const lines=document.getElementById('adm-site-order').value.split('\n').map(l=>l.trim()).filter(Boolean);if(!lines.length){toast('⚠ 내용 없음');return;}SITE_ORDER=lines;if(G.rawLog.size&&G.ref.length)recomp();saveSettingsToSheets();toast('✓ 사업장 순서 저장됨 ('+lines.length+'개)');}
function admSaveDeptOrder(){const lines=document.getElementById('adm-dept-order').value.split('\n').map(l=>l.trim()).filter(Boolean);if(!lines.length){toast('⚠ 내용 없음');return;}DEPT_ORDER=lines;if(G.rawLog.size&&G.ref.length)recomp();saveSettingsToSheets();toast('✓ 부서 분류 저장됨 ('+lines.length+'개)');}
function admSaveUrl(){const u=document.getElementById('adm-url').value.trim();if(!u){toast('URL 입력');return;}G.url=u;pullAll();toast('✓ URL 저장 — 데이터 로드 중...');}
async function admTestUrl(){const u=document.getElementById('adm-url').value.trim();if(!u){toast('URL 먼저 입력');return;}toast('연결 테스트 중...');try{const r=await fetch(u+'?action=getRef');const d=await r.json();if(Array.isArray(d))toast('✓ 연결 성공 — 대상자 정보 '+d.length+'명 확인');else toast('⚠ 응답 형식 오류');}catch(e){toast('⚠ 연결 실패: '+e.message);}}
function admChangePw(){const p1=document.getElementById('adm-pw1').value;const p2=document.getElementById('adm-pw2').value;if(!p1){toast('새 비밀번호 입력');return;}if(p1!==p2){toast('⚠ 비밀번호 불일치');return;}ADMIN_PW=p1;document.getElementById('adm-pw1').value='';document.getElementById('adm-pw2').value='';saveSettingsToSheets();toast('✓ 비밀번호 변경됨');}

/* ── 월별 마감 ── */
function admRenderCloseMonthGrid(){
  const el=document.getElementById('adm-close-month-grid');if(!el)return;
  const now=new Date();const months=[];
  /* 12개월 전부터 오늘 달까지 */
  for(let i=12;i>=0;i--){const d=new Date(now.getFullYear(),now.getMonth()-i,1);months.push(d.getFullYear()+'-'+pad(d.getMonth()+1));}
  el.innerHTML=months.map(ym=>{
    const[y,m]=ym.split('-');const MN=['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
    const isClosed=CLOSED_MONTHS.has(ym);
    return `<button class="cmth-btn${isClosed?' closed':''}" id="cmb_${ym}" onclick="toggleCloseMonth('${ym}')" title="${ym}">${y.slice(2)}년 ${MN[parseInt(m)-1]}</button>`;
  }).join('');
  document.getElementById('adm-close-status').textContent=CLOSED_MONTHS.size?'마감: '+[...CLOSED_MONTHS].sort().join(', '):'마감된 달 없음';
}
function toggleCloseMonth(ym){
  if(CLOSED_MONTHS.has(ym))CLOSED_MONTHS.delete(ym);else CLOSED_MONTHS.add(ym);
  admRenderCloseMonthGrid();
}
async function admSaveClosedMonths(){
  await saveSettingsToSheets();
  toast('✓ 마감 설정 저장 — '+([...CLOSED_MONTHS].length?[...CLOSED_MONTHS].sort().join(', '):'없음'));
}
function admClearClosedMonths(){CLOSED_MONTHS=new Set();admRenderCloseMonthGrid();}


(function init(){
  document.getElementById('cf').value=fmtD(new Date(new Date().setDate(1)));
  document.getElementById('ct').value=fmtD(new Date());
  document.getElementById('iv-cf').value=fmtD(new Date(new Date().setDate(1)));
  document.getElementById('iv-ct').value=fmtD(new Date());
  document.querySelectorAll('.overlay').forEach(el=>{el.addEventListener('click',e=>{e.stopPropagation();});});
  window.addEventListener('mouseup',()=>{G_exc.isDragging=false;});
  if(G.url)pullAll();
  document.getElementById('psel').addEventListener('change',()=>{if(!G._pselUpdating)rDash();});
})();
</script>
</body>
</html>
