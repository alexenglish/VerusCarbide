# Explorer reference

[Return to the README](../README.md)

## Controls

Function keys open workspaces and investigation tools. F5 refreshes the current view wherever manual refresh is available. Plain letters act on the current view. Shift commonly selects a related action, reverses traversal, or toggles a display setting. The footer shows the bindings for the visible interface.

- `F2` opens Sources. Use `e` to enable or disable the selected daemon or API, `m` to toggle daemon monitoring, `s` to save states, and `Enter` to choose the current chain. Press `z` on a local daemon to open ZMQ Setup. Recent activity follows the selected source. Press `a` for history, `f` to switch between selected and all sources, and `Enter` for the event timestamp and details.
- `F4` in a transaction opens Witnesses. Enter, Right, or `l` starts the investigation or opens the focused signer or evidence output. Use `j/k` to select, `a` to switch sections, `[`/`]` to focus and select evidence, Tab for JSON, and `r` to refresh. Back restores the prior selection.
- `F3` in a transaction: select an explicit notarization, evidence, finalization, or import/export reference and follow it to its owning system and output; reverse references include related transactions retained in the persistent cache
- `F5` on Dashboard: request an explicit refresh (including a consented API snapshot when the daemon is unavailable)
- `F7` on a transaction/block: open stored inclusion history/child branches; `j/k` selects, `Enter` opens a block, `r` reloads cached observations, `[`/`]` pages, and `Esc` closes
- `F8` in a transaction: select a reserve-transfer, main currency-import or fee-pool output, then press `Enter` to inspect its spender, export batch, matching import, and result outputs. The panel checks the selected transfer's batch position and the import's declared generated-output region. For supported batches, it replays each member through the selected transfer: native P2PKH results require exact satoshis and serialized script; plain reserve results require exact reserve amounts and decoded destination. Structural and result-candidate edges persist with source and block context for later opening. Ordinary reserve/pool buys and sells and reserve-to-reserve conversions can produce native or reserve results. Pool sells use the supplied pool currency for pending lookup and import routing (`IMPORT_TO_SOURCE`), with canonical local and cross-system sell flags supported. A captured SUPERVRSC sell verifies the exact native receipt. Supported gateway forwarding matches principal, converted fee budget, destination and next system, then follows the exact continuation automatically (up to eight legs). Each leg retains its own import evidence and destination-height search. Each leg also summarizes instruction principal, exact replayed receipts or forwarding, explicit fee budgets, conversion fees and pool context. `c` lists its currencies; `p` opens the pool on its executing system at the import height. The summary shows net execution amounts, effective rates and the import batch's named conversion prices. `v` explicitly loads the linked previous pool notarization and compares its supply/reserves with the import state. Changes cover the pool between notarizations and can include other transfers and fees. Supported gateway conversions reproduce converted principal, the separate fee budget and native custody before continuing. Same-system settled and failed-launch refunds receive a distinct Refunded result with the full original principal. Captured native/reserve refunds and Bots’ five-asset preconversion batch now exercise the actual replay and terminal UI. Native identity refunds require the standard version-3 one-of-one script; reserve identity refunds require exact decoded values and the standard identity condition. This includes cancelled gateway legs with a compatible P2PKH primary or auxiliary destination; unused forwarding fees remain separate from principal. Unknown amounts remain unresolved, and forwarding budgets are not totaled across legs. Narrow cross-system returns now verify local refund receipts without mistaking CROSS_SYSTEM for forwarding. Ordinary same-system prelaunch token imports replay customer preconversions and, for nonfractional tokens, their preceding currency-identity proceeds. This proves receipts rather than eventual launch success. Plain fractional transfers can verify local gateway fallback when a supported PBaaS next-system definition proves an insufficient unchanged forwarding budget; the recovered budget and any native principal appear separately. Main import roots open backward export references and generated outputs. Empty same-system nonfractional clear-launch batches can verify their complete ordered explicit preallocation vector. Fee-pool roots compare adjacent parent-linked coinbase checkpoints without searching for a spender, showing observed deltas separately from unproven fee inflow or producer debit. General capped/final-launch replay, ETH gas-dependent fallback, exporter-dependent allocations and serialized/nested destination paths remain unresolved. For a historical cross-system import, `g` jumps to a destination-chain height, `[`/`]` search adjacent 1,000-block windows, and `s` scans up to 32 older windows with visible progress; repeat `s` to continue. `j/k` selects a hop and keeps it visible, `Enter` opens it, `r` refreshes, and `Esc` closes. A missing import in searched windows is not proof that none exists.
- `F6`: open Bookmarks; `Shift-B` from a supported detail view bookmarks that location, `n` adds a search target, `e` edits its description, `x` removes it with confirmation, and `Enter` opens it
- `F9`: open Charts at the viewed block, a transaction’s containing block, or the observed tip; `1/2/3/4/5` selects intervals/mining/staking/PoW Ratio/fee pool, `j`/Down selects older and `k`/Up newer samples, `-`/`+` adjusts the time span, PageUp/`[` and PageDown/`]` load older/newer ranges, Home/End selects boundaries, Enter opens its block, and F5 refreshes. Press `g` to seek by block height, Unix seconds, or plain-text date/time, with a preview before submission. Dates default to UTC and date-only input means midnight. Targets in the loaded range select immediately. Other targets load a centered window using the current chart and span. `height:` and `unix:` provide optional overrides. A future target offers `l` to open the latest block.
- `F10`: enter a future height or date in the estimator (`estimate:` search).
- `Shift-I`: toggle resolved identity/currency names and raw i-addresses in data views, including JSON. The `identityaddress` field stays raw. Friendly names omit the assumed `.VRSC` root; the explicit fully qualified field retains it.
- `c`: switch between an identity and its associated currency from Overview or JSON. In a DeFi trail, `c` lists its currencies. In ordinary browsing, `c` opens the currency browser.
- `Shift-D`: resolve supported heights, confirmation counts and Unix timestamps in block/transaction/identity data, including JSON. Future heights show `est.` dates; confirmations show the recorded inclusion date when known. Relative unlock delays remain counts. Dates follow the UTC/local display setting in Help. Status, breadcrumbs, navigation controls and chart axes stay numeric; copied and exported values remain raw.
- **Shift-M** switches between mouse capture (scroll and left-click to copy values) and native terminal selection and middle-click paste. Each vertical wheel step acts like Up or Down. In Overview, a click copies the displayed field value, including full hashes behind shortened labels. In JSON, it copies the underlying value like `y`. Hold Shift to bypass capture in supporting terminals. tmux may require its own mouse override if its mouse mode is enabled.
- JSON navigation retains the viewport when reversing from either document end; the cursor moves toward the middle before the text follows. This applies to arrows, j/k and the wheel.
- `Up/Down` or `j/k`: move through selectable items, including underlined output badges and labeled protocol references. `Enter` opens the selected link. A Reserve Transfer badge opens F8 at that exact output; currency badges open the currency viewer on the observing chain. Transfer destinations and source-export/evidence outpoints retain their explicit chain, with normal Back navigation. External destinations remain readable in JSON.
- `Enter`, `Right`, or `l`: open the selected block/transaction, follow a JSON link, or expand a JSON node
- `Left` or `h`: traverse back through history
- `[`/`p` and `]`/`n`: open the previous or next block/identity version
- `v` in identity Overview: enter the ending height for a bounded 1,000-block historical reverse lookup of known authorities and primary addresses. It can establish reverse-index support for an enabled source; unsupported sources are skipped. Evidence links persist with the identity version and source block, and an orphaned block invalidates their reuse.
- `d`: open Dashboard as the new navigation root
- On Dashboard, recent blocks are on the left and recent confirmed transactions are on the right. Tab or Shift-Tab switches lists. In narrow terminals, Dashboard shows the focused list. Select **Stream Blocks** with Home in the block list to keep new blocks in view. Enter opens the selected block or transaction.
- `m`: open Mempool as the new navigation root. In RPC polling mode, the first load requests decoded transactions in bulk when supported. With a ZMQ transaction feed, Mempool listens for events. Otherwise it polls RPC every two seconds. Membership and metadata come from the daemon. A single follow-up check after a ZMQ transaction burst can catch a late admission. Press `F5` for an immediate refresh.
- `o`: open Marketplace as the new navigation root from any view
- `c` or `/currencies`: open the currency browser for the observing system, showing liquidity pools by default (Fractional option required), with known failed launches excluded. **Shift-F** opens filters on the option flags page. Tab or Shift-Tab switches to launch-status filters for Launched, Prelaunch, Failed launch (refunding), and Unknown; Space/Enter toggles inclusion, `l`/`p`/`f`/`u` chooses a single status, `a` includes all, and `d` restores the default. On the option flags page, `j/k` selects a flag, Space/Enter cycles Any → Show → Hide, `a` allows any option flags, `p` restores liquidity pools, and Esc/Shift-F returns to the list. Matches have all Show flags and none of the Hide flags. Launch-status and text filters also apply. Flag changes are local, preserve on Back/refresh, and reset the list to the top. Summary tiles show names, i-addresses, option tags, supply and reserve counts from the list observation. `f` filters names, IDs or options; `j/k`, arrows and paging select; Enter opens; `r` refreshes. Back preserves the filter and selection. Retained lists and cached definitions remain usable offline and are labeled as observations. On an ID with the active-currency flag, `c` opens that ID's associated currency instead; a **Currency defined** badge makes the link visible.
- `/<name-or-id>@@`: open a currency viewer on the current chain; cached definitions and states open offline. Definition properties use the ID-style header, option bits appear as configuration badges, and basket/state/activity sections have distinct headings. Marketplace currency links open the same viewer. In the viewer, `g` seeks by block height, Unix seconds or plain text date/time (UTC; date-only means midnight). Numeric inference matches chart seeking; `height:`, `#` and `unix:` remove ambiguity. Future states are rejected. Time lookups show nearest-block coverage and retain the actual state with source/block context. Then `a` checks imports and exports at that state height, `w` loads the preceding 1,000-block window, and `[`/`]` queries adjacent windows up to the selected state's anchor. Queried windows persist with source/block context and reopen as retained observations. `F8` on an activity import/export inspects its source export's input range; select an input and press `Enter` to open F8 at that exact source output. Opening the import transaction and pressing F8 also inspects empty batches and verified explicit launch allocations. `r` refreshes current data, `Tab` shows JSON with observation provenance, and `Enter` follows selected currency, transaction, or block links. For an older DeFi example on VRSC, open `Bridge.vETH@@` and request height `3962166`; recent heights may have no DeFi activity.
- `a` / `Shift-A` in Marketplace: select the next / previous association, wrapping at either end; preserve that association across offers and breadcrumb navigation
- `s` / `Shift-S` in Marketplace: cycle sort fields forward / backward (expiry, name, ask amount, listing age); `r` reverses ascending/descending order. Changing sorting returns to the top.
- Backtick: toggle the local-daemon peer browser without changing blockchain history
- `w` in the peer browser: toggle the world map
- `a` in Peers adds a node to the local daemon's node list. Press `c` and confirm to clear its ban list. Pressing `p` queues a ping to all connected peers. The list shows connected peers and banned entries, and the traffic tile is a one-time network totals snapshot.
- In a peer's Overview or JSON view, press `x` to disconnect or `b` to ban the peer’s IP. The confirmation identifies the target. Peer Overview groups connection, traffic and latency, and chain progress fields. The Peer log panel reads the local `debug.log` beside the daemon configuration file and shows recent entries containing the peer’s IP or address.
- `F1` or `?`: open or close the keyboard reference
- `t` in Help: switch UTC / local system time for this session
- `b`: open the containing block from a transaction
- `Tab` / `Shift-Tab`: toggle Overview/JSON modes where both are available, including peer details
- `Space`: collapse or expand the selected JSON node
- `j/k` while a long scalar is expanded: scroll the single wrapped value
- `s`: save pretty JSON from the JSON view
- `/`: focus the object search bar; `Enter` resolves and opens, `Esc` cancels
- `/2026-11-07`, `/time:Nov 7 2026 2:30 pm UTC`, or `/1700000000`: find a block by recorded header time. Use `unix:1700000000` to force a timestamp. `Esc` cancels a pending lookup.
- `Esc`: close/cancel the frontmost interface or input, then traverse back. At a navigation root, return to Dashboard; at Dashboard, stay open. Esc never exits the application.
- `Backspace`: go back
- `q`: quit the application outside an active input field or API permission prompt. In text fields it remains a character; numeric fields ignore it. Press Esc to leave the field, then q to quit.

## Date/time navigation

Use `time:` (aliases `date:` and `unix:`) in search or the startup object:
`--open 'time:November 7 2026'`. Search treats a bare integer through the current
tip as a block height. A larger integer at or after the chain's genesis time is
interpreted as Unix seconds. Numbers between the tip height and genesis time
estimate future block heights. Unix timestamps are whole seconds. ISO dates,
year-first numeric dates, named-month dates, 24-hour and AM/PM times, RFC3339
offsets and RFC2822 dates are supported. `today`, `yesterday`, `tomorrow`, `now`
and numeric durations such as `2 hours ago` are also accepted. Relative input is
frozen when submitted. Date-only input, including `2 days ago`, means 00:00 UTC;
timezone-free clock times also use UTC, independently of the display setting.
Ambiguous numeric dates such as `11/07/2026` and timezone abbreviations such as
PST are rejected; use a year-first date and an explicit numeric offset instead.
Millisecond Unix timestamps are rejected rather than silently misinterpreted.

The lookup brackets by header time and checks a contiguous neighborhood of up
to 129 blocks, including backward timestamp steps. It also considers its binary
probes. A tie prefers the earlier timestamp, then lower height among checked
blocks. It reports the interpreted UTC target, signed time difference, checked
window and whether the timestamp matched exactly. Because header timestamps
are not sorted, a non-exact result is the nearest **checked** block, not proof
of the globally nearest timestamp across unvisited history. Past dates outside
this source's observed history select an observed block. Future dates and
heights open a separate estimate, never an invented observed block.

The reusable [block_time module](../src/block_time.rs) separates parsing, the
`TimeSource` search seam, and the asynchronous `find` RPC/cache adapter. Results
carry height/hash/time, source, observed tip, coverage and exactness for later
chart seeks and currency-state requests. RPC work stays serial, uses
hash-qualified cached headers, pins the source, rechecks active membership and
cancels on Esc, replacement search, root navigation or shutdown. No timestamp
index or disabled API access is requested. The same lookup supports chart seeking and historical currency-state requests.

## Future block estimates and height dates

Press F10, or search `estimate:5000000`, `estimate:in 2 hours`,
`estimate:in 2 years`, `estimate:2028-11-08`, or
`estimate:unix:1857254400`. A bare numeric estimate target is a block height;
use `unix:` or `time:` inside `estimate:` to identify Unix seconds. Ordinary searches
for future heights and times also open the estimator. Startup supports
the same syntax, for example `--open 'estimate:in 2 hours'`.

The reusable [block_forecast module](../src/block_forecast.rs) calculates one rate
from two canonical headers and uses it in both directions. On VRSC mainnet the
current calibration starts at height 2,549,920, after PBaaS activation's guard,
and ends at the verified tip. Older eligible VRSC periods have separate anchors;
staking attack/repair and transition intervals cannot be crossed. Other chains
start after their first 500 blocks and require at least 1,000 subsequent
calibration intervals. A two-endpoint average does not detect unknown historical
interruptions or guarantee a future schedule. Calibration and observation metadata
are shown with each result. The source label shows the daemon host and port,
or the API provider actually used. Catching-up, stale, inconsistent or invalid sources
cannot supply a new estimate.

Shift-D resolves the displayed values in data views, including JSON, while retaining
the original values for yanking and export. A block height uses its accompanying
header timestamp immediately. Confirmations use recorded inclusion evidence, never
a cached count subtracted from today's tip. Monetary values and identity user content
are not interpreted as dates. The status bar, breadcrumbs, block navigation,
dashboard block numbers and chart axes remain numeric.

Missing historical dates load asynchronously in small batches through already-enabled
sources. Historical dates are never extrapolated. An unavailable date retains its height with a
short availability label. Cross-chain heights use that chain's configured
source; changing a source or branch discards stale date results. Retained fork
rows use their own cached block timestamps. This does not request an index or
prompt to enable an API. The setting lasts for the session; Shift-D again restores
numeric JSON previews.

## Terminal and tmux titles

Carbide sends **Verus Carbide** as a title hint when its terminal UI starts or
resumes. On Unix, terminals with an xterm-style title stack can restore the
previous title when Carbide exits or suspends. In a Unix shell with job
control, Ctrl-Z suspends Carbide through the terminal driver, including
while its UI is busy. Use `fg` to resume.

In tmux the hint sets the pane title when `allow-set-title` is enabled.
Use `#{pane_title}` or `#T` in a tmux format to display it. To use the
pane title for the outer terminal window, add these lines to your tmux config.

```tmux
set -g set-titles on
set -g set-titles-string '#T'
```

The tmux session name is set separately. For example, `tmux new-session -s carbide vcarbide`
launches Carbide in a session named `carbide`.

## Clipboard testing and tmux

In JSON, `y` copies the raw selected value (strings without quotes; objects and
arrays as compact JSON). Inside tmux, auto mode uses `tmux load-buffer -w` with
an application-named buffer and clients displaying the application's pane.
This works with `set-clipboard external` and does not require passthrough or
changing tmux settings. Elsewhere it sends OSC 52 directly. Clipboard text goes
through stdin or base64, never a shell command or temporary file. The existing
1 MB limit remains; tmux work runs outside the UI with command timeouts.

Press `?`, then `y`, to send the phrase `Verus Explorer clipboard test` and paste
it into a local editor. For transport diagnostics without a daemon/config/cache,
run `vcarbide --clipboard-test`. This prints the detected mode,
tmux clipboard policy/client capabilities, and sends the same known phrase.
Success notices say what was sent; they cannot confirm desktop receipt. The
explorer never reads the clipboard. Nothing copies automatically at startup.

`VERUS_EXPLORER_CLIPBOARD=auto|osc52|tmux|off` selects the transport. Auto is the
default. A tmux policy of `off` or missing terminal clipboard capability produces
an actionable error rather than an unverified success message. Nested tmux
layers still need compatible forwarding at every layer.

For Alacritty → SSH → tmux, `external` is sufficient with the new tmux transport.
The outside terminal must advertise clipboard capability, and Alacritty on the
local machine must permit writes (`[terminal] osc52 = "OnlyCopy"`). See the
[tmux clipboard guide](https://github.com/tmux/tmux/wiki/Clipboard) and
[Alacritty terminal settings](https://alacritty.org/config-alacritty.html#terminal).

## Charts

F9 opens a one-day chart window for the displayed system and preserves Back
navigation. Use `-`/`+` (or `=`) to select 2 hours, 6 hours, 1 day or 7 days.
Ranges use observed header timestamps rather than an assumed block interval.
PageUp/`[` and PageDown/`]` page through older/newer non-overlapping block
ranges. Endpoint heights and dates/times, actual elapsed coverage, intermediate
height labels, graduated vertical axes and subdued horizontal grid lines provide
context. `j`/Down moves to older samples; `k`/Up moves to newer samples.
The active metric has a highlighted heading above the chart controls.
Mining and staking charts show blue block dots and a green MA100 line. Each
average uses the matching production type within the trailing 100 chain heights:
mining excludes staked blocks and staking excludes mined blocks. The selected
point shows the contributing sample count. A full, contiguous 100-height window
is required; missing applicable values leave a gap rather than inventing zeros.

`4 PoW Ratio` uses header timestamps on the horizontal axis and two block levels:
mined at one-third height, staked at two-thirds height. Each known block contributes a blue dot.
Green and orange lines show the staked share of trailing 100- and 1,000-block
windows, with 50/50 fixed at the vertical center. Selected-block details show
mined/staked counts in both windows, including the selected block. Partial or
unknown history is labeled and does not produce a complete-window trend. Lines
break at gaps and non-increasing timestamps. Endpoint dates use the configured
UTC/local display, as do the intermediate time labels.

`5 Fee pool` plots coinbase balance checkpoints with time horizontally and whole
currency units vertically. Native currency is blue; PBaaS charts also show the
network's Verus currency (VRSC on mainnet, VRSCTEST on testnet) in green. The
axis shows currency amounts. `v` cycles both/native/reserve, with an independent
vertical range for the selected series. `z` cycles linear from zero, linear fit
to the visible range, and a clearly labeled log-like scale. The log-like axis
uses ordinary currency labels at powers of ten, keeps zero defined, and becomes
linear below 0.0001 currency units. Fit mode helps reveal small fluctuations;
log-like mode retains large launch balances alongside much smaller later values.

The default loads at most 100 sampled checkpoints; `t` switches to detail mode
(up to 2,000). Windows with at most 2,000 blocks then include every block, so
small or short-lived peaks are retained. Larger windows remain sampled and can
miss intervening peaks. Shorten or page the window for closer inspection.
The chart loads the endpoints first, then fills in progressively finer passes
across the window in random order within each pass. The footer reports
available/requested checkpoints and the active controls. Lines
connect observations and do not assert every intervening balance was measured.
Missing/pre-activation checkpoints remain unknown and break the line; valid
checkpoint maps may omit a currency to represent zero.

Selected-checkpoint details show exact amounts for both currencies. Below them,
**Current fee pool** shows the balance at a separately fetched latest observed
block, with its height and time. Paging history does not turn its historical
endpoint into the current balance; F5 refreshes the observation. Fee-pool charts
reuse hash-qualified cached blocks/transactions and normal chart seeking,
paging, source checks and cancellation. Seeking an unsampled block includes
that exact checkpoint in the bounded sample set. This is a balance chart;
separate inflow attribution is outside its scope.

Block intervals are signed timestamp differences between linked parent/child
headers. The interval plot starts at zero; genuinely negative differences remain
available in selected-block details and are counted below the plot, rather than
being drawn as fabricated zero values. Mining difficulty
uses the daemon value at mined blocks. Staking difficulty is derived at observed
Verus PoS blocks as the fixed initial LWMA `posLimit` divided by the block's
expanded target. The reference is
`000000000f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f`.
This is a stable normalization from Verus source, not the changing permitted
limit, the first visible sample, a staking-supply estimate or a next-target
prediction. The compact target is extracted from the nonce and checked against
`postarget` when available. The ordinary RPC difficulty field is not used for PoS.

Charts reuse hash-qualified cached headers, then fetch missing headers one at a
time. Only the fee-pool metric additionally loads sampled coinbase transactions. Up to 99 preceding headers supply the first
visible moving average and parent/child interval, in either paging direction.
Selecting PoW Ratio extends this to up to 999 preceding headers for its longer
trend; the other metrics retain their smaller context requirement.
Loading is progressive and bounded to 20,000 headers per request. A reached
limit is shown as partial coverage; choose a shorter span or page onward.
Genesis and the source tip can also shorten a range; actual endpoint times
remain visible. Paging waits for the current request to finish; changing the
span cancels and replaces it.
History loads only on entry, adjacent-window selection or explicit refresh, and
cancels when leaving the view. Local sources are preferred; disabled APIs never
prompt. Cached windows are labeled retained observations until refreshed; source
changes, missing/invalid data and orphan anchors remain visible. Missing metrics
are not zero. F5 checks current anchor membership without changing the chosen
historical block. Enter opens the selected exact block; Back restores the chart.

## Chain connections

The invocation chain defaults to VRSC. Dashboard, Mempool, Peers and `/` search use the current chain. Following a foreign transaction leaves that context unchanged; use Sources to switch it explicitly.

Known daemon configurations are read from standard Verus/PBaaS directories, or from the supplied `-conf`/`-datadir` locations. Discovery does not probe ports or read wallet/blockchain files. Discovered connections start disabled, except the invocation daemon and persistently enabled connections. Enabled daemons serve explicit requests and visible refreshes; monitored daemons also update their tip and mempool membership in the background. APIs never receive automatic monitoring calls.

### Local ZMQ live updates

In **F2 → Sources**, select a local daemon and press **z**. The setup window shows its config path and the exact lines to add. Click anywhere inside the **Copy** box or press **c** to copy the whole snippet. Paste it into that file, restart the daemon, and press **r** to recheck from Carbide. The daemon must be built with ZMQ. Carbide loads the optional `libzmq` 4.x runtime on Unix. On Debian, install `libzmq5` if it is missing.

The selected daemon’s **ZMQ** line in Sources shows **Connected · waiting for events** after the subscription connects. When a valid notification arrives, it changes to **Receiving**, with the age of the last notification and the number of topics heard from. Leave Sources open through the next block or naturally occurring transaction. Press **z** for each topic’s status, since topics can share a socket while receiving different events.

The title bar shows ZMQ beside RPC and API. `zmq on` means connected and `zmq live` means Carbide has received a notification.

Give each daemon a different loopback TCP port. Both recommended raw publishers on the same daemon can share one port. Carbide suggests a port that was free when checked, and **p** checks a port you enter. Existing per-topic endpoints are preserved. A complete example for one daemon is:

```ini
zmqpubrawblock=tcp://127.0.0.1:28340
zmqpubrawtx=tcp://127.0.0.1:28340
```

Hash publishers are also supported as lighter alternatives. They trigger refreshes, while raw publishers also supply binary data for local decoding. If configured, `zmqpubcheckedblock` supplies candidate block bodies that are eligible for reuse after the daemon confirms them. The dashboard's transaction pane follows confirmed transactions from all loaded block rows, and both panes update together.

The dashboard shows **Listening on ZMQ for blocks** when connected to a block feed. Block notifications wake the dashboard immediately, and periodic RPC checks reconcile the chain. Without a connected block feed, the dashboard shows **RPC polling**. A connected transaction feed replaces mempool polling with RPC checks on entry, transaction notifications, new blocks, reorgs, gaps, and reconnects. Without a connected transaction feed, the mempool returns to RPC polling. Mempool membership, fees, and entry metadata come from the local daemon. Raw feeds can eliminate calls to fetch transaction and block bodies. Detailed Verus script annotations and missing prevouts may still require hydration. Opting into ZMQ does not enable an API.


Connection state lives in the platform configuration directory (`~/.config/verus-explorer-tui/config.toml` on Linux). Changes in Sources apply to the session; press `s` there to persist them. Example:

```toml
[[connections]]
network = "mainnet"
chain = "VRSC"
config_file = "/home/yourname/.komodo/VRSC/VRSC.conf"
state = "monitored"

[[apis]]
network = "mainnet"
chain = "VRSC"
endpoint = "https://your-chain-specific-endpoint.example/rpc"
enabled = false
```

Daemon credentials remain in daemon configuration files. API endpoints must not embed credentials. Each endpoint requires explicit permission through configuration, its Sources control, or the request modal. The modal explains the lookup failure. Press `r` to retry, select an applicable endpoint from Sources (F2) and press Enter to enable it for the session and retry, or press Esc to cancel. Authentication support and an in-app endpoint editor are deferred.

The API permission modal displays the local daemon's error. A lookup returning
“not found” or requiring a missing daemon index can trigger fallback even when
the daemon connection works.

Requests try the selected local daemon first. Automatic enrichment (timestamps,
related transactions, names, balances/activity indexes, offers and output status)
can fall back to already-enabled APIs, but never opens a permission dialog.
Unavailable enrichment leaves the main object usable. Explicit primary object
requests can ask permission when the local source cannot answer. Enable an API
in Sources (`F2`) to allow automatic fallback to it.

Mainnet and testnet use separate `mainnet.sqlite3` and `testnet.sqlite3` files in the platform cache directory. Clearing one preserves the other, configuration, and the running in-memory view. This development schema does not migrate the old per-chain databases.

Schema 6 separates immutable objects from source observations, qualifies currency
state by observing block, retains persistent reverse relationships and bounded
currency activity pages, and bounds mutable snapshots. Use `--reset-cache` to create a fresh cache after a schema
change requiring replacement; schemas 3, 4 and 5 upgrade without clearing data. Local-daemon
reconciliation retains orphaned branches, derives confirmations from committed
membership and invalidates mutable views after reorgs. In a block view, `p`/`n`
follow known branch links and `a` opens the active block at that height.
See [STORAGE.md](../STORAGE.md) for ownership, reconciliation, retention, and reset behavior.

## Witness investigation

F4 follows evidence input/output references, prior finalizations, and matching
finalizations in known spending transactions. It groups decoded signatures by
identity, signing height, confirming/rejecting statement, signing system and
signed target. Each group retains its distinct key signatures and source
outpoints. These groups are not a quorum count.

Signer names are resolved on the signing system. Shift-I shows raw i-addresses,
and opening a signer shows their current identity. Tab shows the decoded report
as JSON with the investigation root, signing heights, signatures, targets,
evidence sources, and reported gaps. Use the JSON view's standard copy and save controls.

Multipart evidence is assembled in output or evidence-input order and passed to
the daemon's `decodescript` operation for decoding. No transaction is submitted.
Incomplete fragments, unavailable history and traversal limits are reported.
Limits are 64 transactions, 256 visited outputs, 32 levels, 128 lookup/decoding operations
and 2 MiB per multipart set. The initial walk can reuse cached responses;
`r` explicitly refreshes reachable transaction observations. API requests follow
the existing consent rules. Removing Witnesses from the navigation stack cancels its task.

Finalization fields and transaction confirmations retain source attribution.
The investigation covers reachable evidence; it does not discover every witness
broadcast or verify signatures, proofs, quorum membership or consensus.

## Protocol outputs and raw data

An export finalization output shows its reported status and links to the
referenced export output on the same chain. Historical Komodo-style VRSC
notarizations show the committed block, destination transaction ID and
Merkle-of-Merkle-roots fields. Open the committed block by hash from the output.
Both output types also appear in the containing transaction and block summaries.

Select **View OP_RETURN bytes** on an output with unrecognized data and press
`Enter`. The byte viewer shows hexadecimal and printable ASCII side by side,
with one cursor highlighting both representations of a byte. Nonprintable
bytes appear as dots in the ASCII column. Move with the arrows or `h/j/k/l`,
page with PageUp/PageDown, jump to either end with Home/End, and return with
`Esc`. Multiple pushes appear as concatenated payload bytes. If the pushes
cannot be decoded, the viewer shows the script bytes after `OP_RETURN`.

## Notarization browser

Currency Overview shows the daemon's current last-confirmed notarization, its
transaction link and local inclusion height and time, plus the number of
distinct unconfirmed notarizations in the returned view. Press `n` to browse
the rows. `Tab` switches between the two perspectives for a known PBaaS pair.
For example, the VRSC view asks the VRSC daemon about vDEX, and the vDEX view
asks the vDEX daemon about VRSC.

Each row shows the committed heights by system. Foreign proof-root checks use
the exact block hash on an enabled source for the owning chain. The browser
distinguishes active blocks, known off-chain blocks, and explicit misses. It
also explains whether a check was blocked by source configuration, a failed
lookup, or a missing hash. An off-chain block or an explicit miss can indicate
a fork. The browser shows when the checked source is behind the committed height.
`Enter` opens the notarization transaction, `[` and `]` select a foreign proof
root, `b` opens the resolved block, `r` refreshes the current perspective and
`Esc` returns to the currency.

## Reorg investigation

Press F7 on a transaction to browse its known block inclusions, or on a block to
choose among known child branches. Next-block navigation (`n`/`]`) also opens the
picker when multiple child hashes are known in the current workspace. F7 reads
persistent observations, including evidence retained across restarts; it does not
scan the chain or contact sources to discover additional branches.

Each row identifies a block hash, height, source, cached-body availability and
last-observed membership. Active/orphaned labels describe stored source reports;
missing membership is explicitly unverified. Different sources remain separate,
and an empty list does not prove absence. Mempool observations without a block
hash are excluded from inclusion history.

Use `j/k`, arrows, Home/End or PageUp/PageDown to select. `[`/`]` moves between
pages of 100 observations; `r` reloads the cache. A committed reconciliation
revision reloads an open panel automatically, preserving the selected hash/source
when it remains on the page. Enter/Right/`l` opens the exact block hash, preserving
Overview/JSON mode and adding Back history. The current monitoring chain remains
unchanged. Esc/F7/Left/`h`/Backspace closes the panel without navigating.

Cached blocks remain usable with disabled sources. Opening an uncached block uses
the ordinary configured loader and API consent rules; a disabled daemon can be
enabled through F2. The source used for a cached body may differ from the selected
membership observation and is reported when opened. This feature reports known
evidence, without verifying consensus or predicting which branch will win.

For hands-on evaluation without a real reorg, run `python3 tests/reorg_demo.py`
after `cargo build`, then follow its second-terminal command.

## Bookmarks

Press Shift-B from a block, transaction, address, identity, currency, or notarization
browser to open a prefilled bookmark dialog. F6 opens the bookmark list for the
running network, where `n` lets you enter a system ID and target manually. Use Tab/Shift-Tab
to select fields, Enter to save, and Esc to cancel.
Descriptions are editable with `e`; `x` asks before removing the selected bookmark.
Use j/k, Home/End or PageUp/PageDown to select entries and `r` to reload the file
and cached observations. Esc/F6 closes the list without changing your location.

A notarization bookmark opens the live browser for the saved currency and
observing chain. It saves the perspective selected with Tab. To add one manually,
use the observing chain's system ID and a target such as `notarizations:vETH`.

Opening a bookmark preserves the current monitoring chain and adds the object to
navigation history, so Back returns to the previous location. Cached observations
can be opened while their source is disabled or no longer configured. Uncached
entries remain visible and use the ordinary daemon/API permission flow when opened.
The list displays tags derived from cached objects and their observation source;
cached data does not establish current chain membership or freshness.

Bookmarks live in the platform user-data directory, separate from configuration
and cache. On Linux the default is:

```text
~/.local/share/verus-explorer-tui/bookmarks-mainnet.jsonl
~/.local/share/verus-explorer-tui/bookmarks-testnet.jsonl
```

`XDG_DATA_HOME` overrides that base directory. Each JSON line contains `version`
(currently 1), `system_id`, `kind` (`auto`, `block`, `transaction`, `address`,
`identity`, `currency`, or `notarizations`), `target`, `description`, and `created_at` (Unix seconds). Blocks added
from resolved views use their hash; identity bookmarks reopen the identity's
current observed version. Currency bookmarks store a bare name or currency ID.
Existing bookmarks ending in `@@` are read as currency bookmarks. Tags are derived, not stored in the bookmark file.
Invalid lines are reported and preserved when valid entries are edited. External
file changes require a reload before saving. Cache clearing leaves bookmarks intact.

Optional startup population is disabled by default. Add this to the application's
`config.toml` to enable it:

```toml
[bookmarks]
fetch_missing_on_startup = true
startup_limit = 20
```

Population starts after the first terminal draw, skips cached entries, and uses
only already-enabled sources without permission prompts or source activation.
It attempts at most `startup_limit` missing bookmarks (capped at 100), sequentially,
with a 70-second timeout per bookmark. Each lookup requests only the bookmarked
object; ambiguous hashes may try a transaction and then a block. Normal transport
handshakes/fallbacks still apply. Failures remain visible in the list.

For the isolated terminal integration check, run `cargo build`, then
`python3 tests/bookmarks_terminal.py`. It uses temporary directories and fake local
daemons, and requires PTY and loopback-socket access.

Captured DeFi burn/refund regressions are cataloged in
[tests/fixtures/defi-refunds](../tests/fixtures/defi-refunds/README.md).
`cargo test defi::fixtures` checks exact refund receipts, prefix tampering,
financial summaries and the DAI burn's reserve-state evidence;
`python3 tests/defi_fixtures_terminal.py` exercises all ten captured repayments,
an altered-prefix rejection and the imported Burn tag through isolated daemons.
Supplemental burn instruction tags do not assert an independently correlated
Ethereum receipt or a replayed customer payment.

### Import submission rewards in F8

For a supported settled PBaaS import, F8 links the verified customer receipt
and the payouts to the import proposer, configured notary, source block
producer, and primary exporter. Open F8 from the main import or follow the
original reserve transfer. The trail also links the producer coinbase and the
original chain definition used to select the notary. The converted fee and
its unattributed remainder are shown separately from verified payments.

For cross-system imports, coverage applies to the single-transfer,
four-payment native layout. Both chains need enabled sources. For
same-system fractional launches and settled reserve-to-reserve batches,
F8 verifies the native exporter payments using the execution prices
and liquidity retention for that phase.

When transfers come from several heights, the producer lookup uses the
entropy at the export's source-end height to select from the distinct source
heights. A proof-of-stake entropy lookup requires the raw PoS hash from
the header. When the recipient scripts and amounts verify, F8 can show the
exporter payments while leaving the producer role unattributed. Direct
reserve-denominated service payouts remain outside this coverage.

### Successful launch funding in F8

Open F8 on a preconversion contribution or the main import that issued its
receipt. For supported same-system fractional launches, the trail validates the
ordered contributions and exact receipt scripts and amounts. It links the
original definition, launch-clear import, and launch-complete import. Starting
from the receipt import shows every verified contribution and receipt.
The initial window can move closer to the scheduled launch for a contribution
exported well before launch. The verified receipts exclude service payments and
reserve-maintenance outputs.

### Historical marketplace trails

Open a transaction and use its input and output links to follow the marketplace
funding output, signed offer publication, and funding spender. Historical links
use transaction data from the configured source. They also work for spent and
expired offers, since they do not require an entry in `getoffers`.

The trace reads full offer templates from the last OP_RETURN of a separate
publication or a transaction that embeds the proof. A **Marketplace Offer
Taken** result binds the signed funding input and the exact seller consideration
to the observed transaction, then identifies the taker delivery output. A
supported standalone close matches the original assets, net of the wallet close
fee. The flow identifies any transparent or shielded refund without exposing
private recipients. Custom and batched spends show verified identity and
currency returns when the outputs reconcile. Shared funding stays grouped
when identical assets came from several inputs. A spend with insufficient
evidence remains unverified.

Select funding output 0, or the input that spends it, and press `e` to look
for an embedded publication. Each pass checks up to eight confirmed
blocks on the funding source. Repeat `e` to resume at the next block. The
search requires a source that can return decoded block transactions.

DeFi accounting keeps instruction principal, transfer custody and transaction fees distinct. Transfer custody includes explicit and valid gateway fee budgets and excludes mint principal. Protocol transactions show a public native delta and an unavailable consensus fee when descriptor accounting cannot be reconstructed. Missing required shielded public balances remain unknown; individual private values remain hidden.

## Badge capitalization

Use Title Case for classification badges and sentence case for status and descriptive badges. Preserve protocol spellings such as PBaaS and StakeGuard, acronyms such as ID and NFT, and the case of currency and identity names. Use typed identifiers when filtering or matching classifications.
