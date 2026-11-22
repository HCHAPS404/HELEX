# Phase B: dense user canon (handoff spec)

State: `opt/post-alex` @ `fe9c082` has dense EVENT dedup (`counters.rs`,
`PCtx.dense_e`, `EvRec.ingest` bit63 = event-dense flag, `P2.dtab` direct
table generation-stamped by shard, `P2.dhash` overflow, win-bitmap emit).

`PCtx.dense_u` is already populated from `counters::recover` (`keys.user`)
but NOTHING consumes it yet — zero per-line cost. This doc is the full
Phase B plan.

## Goal

Replace `dsu.resolve(user_key)` (bloom + KeyMap probe) for dense users with
one indexed read: `utab[counter >> ALIAS_BITS]` -> canon key.

Measured context (60M-v5, 4t): dsu.resolve via bloom is already cheap
(prefetch experiment was neutral), so expected gain is modest (~0.05-0.15s
official) — do it AFTER confirming dense coverage is high on scorer data.

## Design (mirrors HELEX-DEFF `utab`/`user_loc == ID_DENSE`)

1. **User-dense flag**: `EvRec.ingest` bit62 = user-dense (bit63 already =
   event-dense). `CkRec` needs its own marker — `canon` field is produced
   later, so mark in `long` field low bit or add a side channel; simplest:
   decode user at emit time from `r.user`... NO — `r.user` stores the u64
   key, not bytes. Dense user key = raw counter stored in `r.user` with
   the flag in `r.ingest` bit62 (events) — for CkRec, `long`==0&&cid path:
   mark in `cid_hi` top bit? CkRec.canon is computed in parse via
   `dsu.resolve` — for dense users we can store `DENSE_TAG | counter` in
   `canon` field and resolve later. Pick ONE marker scheme and stay exact.

2. **Parse** (`parse_event_line` ~l.857, `parse_click_line` ~l.906):
   `ctr_u = pc.dense_u.and_then(|k| counters::decode(f_user, b'u', k, counters::USER_CAP))`
   - Some(c): `uk = c` (raw counter), set user-dense flag; skip slow_u slab
     path (dense users never slow)
   - None: existing `id_key_padded` + slow path

   IMPORTANT: the slow-user slab/registry (`push_slow`, `slow_seen`) and
   OUTPUT user-id bytes still need real ids for dense users! Dense users
   appear in user_daily output — the output code resolves `canon -> id
   bytes` via the slow/fast key byte recovery. A raw counter in `r.user`
   breaks `fast_key_bytes` output. Two options:
   a) dense canon resolves to the canon BYTE key (utab stores canon key,
      not counter) — output path unchanged. PREFERRED.
   b) re-encode counters via `counters::encode_into` — extra work.

3. **DSU / links** (`src/dsu.rs load_links`): link endpoints decode with
   `dense_u` too. Dense members intern keyed by `SLOW_TOP | counter`
   (collision-safe within DSU's own map: slow_top|counter can't equal a
   tag/plain key — it CAN equal a real slow key only if hash56==counter,
   p<2^-56, but keep a separate tag constant anyway for safety:
   `1<<63 | 1<<40 | counter` is INSIDE tag space — use `SLOW_TOP|counter`
   and accept the astronomically rare collision risk OR intern via a
   second map). utab built post-DSU:
   `utab[counter >> ALIAS_BITS] = canon_key` where canon_key is the DSU's
   byte-min canonical (a u64 key). Non-linked users never touch utab.

4. **Emit** (`pipeline.rs` winner emit ~l.1280): if user-dense flag:
   `canon = utab[(r.user >> ALIAS_BITS) as usize] or r.user itself` —
   need utab to return canon for the USER INDEX (counter>>9), defaulting
   to the byte key stored... CAREFUL: for unlinked dense users canon must
   be the id's own byte key, but r.user holds the counter. If option (a)
   is taken, utab must map index->canon BYTE key, and unlinked users need
   their own byte key — which is NOT in the record anymore. SOLUTION:
   store a parallel `user_key` fallback: EvRec is 32B; either grow to 40B
   (costs bandwidth!) or re-encode the byte key from counter via
   `encode_into` at emit for unlinked/self-canon cases (~rare if most
   users link... actually most users DON'T link — re-encode would run per
   emit = expensive!). ALTERNATIVE: keep `r.user` = byte key always, add a
   SIDE array user_ctr[i] per shard? Or: dense flag means "canon lookup
   via utab keyed by counter derived from re-decoding" — impossible, no
   bytes at emit.

   CLEANEST RESOLUTION: `r.user` keeps the BYTE KEY (unchanged), and the
   dense-user flag means "a dense counter exists" — but then we need the
   counter at emit → store it in `ev` field top bits? ev is packed event
   bits (ts/price/cat/purchase) — check width; if < 24 bits free, no room.

   => Realistic minimal-risk variant: **keep everything as-is for user
   keys; dense-canon only helps if utab can be keyed by byte key hash —
   which is just a hashmap again.** So Phase B's REAL value requires the
   counter carried in the record: recommend EvRec stays 32B but `user`
   holds `counter << 24 | user_key_lo24`?? No — lossy.

   FINAL recommended scheme (what DEFF does): their record carries BOTH
   `user_key` and `user_loc`+counter (48B rec — they shrank to 40B via
   ingest-bit packing, commit 8255217). For us: EvRec 32B -> need +8B for
   user counter OR pack flag+counter into existing fields:
   - `key` field for dense events already holds event counter (<2^28)
   - `ingest` bits 62-63 = flags
   - `user` 8B, `ev` 8B
   Option: `ev` top 28 bits free? ev packs ts(delta?)+price+type+cat —
   CHECK `pack` widths in validate_event_copy/emit decode. If ev has >=28
   free bits, user counter fits there. Otherwise EvRec -> 40B and eat the
   bandwidth (mitigate: dense users could drop `user` byte key entirely
   and re-encode via encode_into only for EMITTED winners — winners
   ~96% though. But only emitted rows need id bytes, and emit already
   touches the record — encode_into ~30ns for the ~60M winners = 1.8
   CPU-s ≈ 0.45s/4t — probably NOT worth it vs keeping byte key).

   => SIMPLEST: `r.user` = byte key always (output safe), user counter
   goes in `ev` top bits IF space allows, else skip Phase B for events
   and apply only to CLICKS (CkRec.canon is computed in-parse anyway —
   dense canon just speeds that resolve: `CkRec` gets `ucnt: u32` packed
   or reuse `long` field). Clicks are 15M vs 60M events — smaller win.

## Verification gates (same as always)

- `python3 scripts/run_fixtures.py fixtures -- ./bin` = 36/36
- dev-1M bit-exact
- 60M-v5 `compare.py` vs `e94f657` output (rows + dup keys)
- 3+ runs, output identical across runs (stability)

## Measured facts to re-check before investing

- `dense=...` count printout on real scorer data is unknown; synthetic
  coverage 87%. If scorer coverage <90%, Phase B upside shrinks further.
- dedup is now 0.87s; emit ~0.4s of it; sess 1.2s; parse 4.3s. Phase B
  attacks maybe 0.2-0.3s of resolve work — LAST among remaining levers.
