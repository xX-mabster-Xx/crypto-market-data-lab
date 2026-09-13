# Техническое задание для следующего агента

## CPMM post-trade DEX↔perp vertical slice

Версия: 1.0, 12 сентября 2026

Репозиторий: корень проекта (путь намеренно не фиксируется в документации).

## 0. Цель задачи

Реализовать **один полностью воспроизводимый read-only сценарий** для Raydium CPMM:

```text
immutable worker snapshot
  -> typed local AMM path request
  -> exact-in buy stable -> base
  -> exact-in sell base -> stable на post-state того же pool
  -> проверка фактического raw base quantity
  -> perp hedge только на точно совпадающее количество
  -> research candidate с evidence/hash
  -> offline replay без сети
```

Это должен быть первый принятый vertical slice M2/M4. Он не должен отправлять транзакции, открывать кошельки, размещать ордера, переводить средства или менять существующий live-поток, если feature flag выключен.

Задача **не включает** CLMM, Meteora DLMM, AMM v4, общий поиск маршрутов, portfolio reservation, borrow, maker execution или запуск live-сканера на сутки.

## 1. Обязательное исходное чтение

Перед изменениями агент обязан прочитать полностью:

1. `TECH_SPEC_STRATEGY_ENGINE.md`, разделы 4, 6–9, 11–12, 14–19.
2. `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, особенно контракты snapshot/path/evidence и разделы acceptance.
3. `PROJECT_READINESS_AUDIT_2026-09-12.md`, пункты F01–F10.
4. `src/market_data_lab/amm_simulation/contracts.py`.
5. `src/market_data_lab/amm_simulation/engine.py`.
6. `src/market_data_lab/amm_simulation/backend.py`.
7. `src/market_data_lab/amm_simulation/replay.py` и `codec.py`.
8. `src/market_data_lab/quote_broker.py` полностью, включая оба текущих определения `simulate_local_path`.
9. `src/market_data_lab/unified_market_data.py`, функции `build_solana_market_sources` и `build_unified_market_data_scanner`.
10. `src/market_data_lab/unified_perp_analyzer.py`, конструктор, `_sequential_result_for`, обработку DEX/perp и `_evaluate_dex_perp_sequential`.
11. `src/market_data_lab/solana_quote_worker.py` и конфигурацию `src/market_data_lab/solana_realtime_scanner.py`.
12. `workers/solana-quote-worker/src/protocol.ts`, `worker.ts`, `simulation/path.ts`, `simulation/snapshots.ts`, `raydiumStandard.ts`.
13. Тесты `tests/test_amm_simulation.py`, `tests/test_amm_raydium_cpmm.py`, `tests/test_amm_simulation_backend.py`, `tests/test_amm_simulation_worker.py`, `tests/test_quote_broker.py`, а также `workers/solana-quote-worker/test/path.test.ts`.

Не принимать README за доказательство: текущий `README_AMM_SIMULATION.md` утверждает готовность, которая фактически не подтверждена.

## 2. Текущее состояние и известные блокеры

### 2.1. Broker API затенён

В `src/market_data_lab/quote_broker.py` сейчас есть:

- typed AMM-метод около строки 809:
  `simulate_local_path(request: AmmPathRequest, *, deadline_monotonic_ns: int) -> AmmPathResult`;
- второй метод около строки 943:
  `simulate_local_path(request: QuoteRequest) -> QuoteResult`.

В Python последний метод заменяет первый. Runtime-сигнатура сейчас фактически:

```text
(self, request: 'QuoteRequest') -> 'QuoteResult'
```

### 2.2. Composition root не подключает AMM

В `src/market_data_lab/unified_market_data.py`:

```python
spot_sources, _local_quote_source = build_solana_market_sources(config)
```

Локальный source отбрасывается. `UnifiedPerpAnalyzer` создаётся без `sequential_amm_simulator`, а `QuoteBroker` создаётся с `backends={}`.

Одного изменения конфигурационного флага недостаточно.

### 2.3. Worker не передаёт initial balances

`workers/solana-quote-worker/src/worker.ts` вызывает `simulatePathLegs(bundle, legs)` без третьего аргумента. В `simulation/path.ts` пустой баланс приводит к:

```text
status=insufficient_balance
complete=false
```

В `protocol.ts` тип `SimulatePathRequestMessage` пока не содержит `initial_balances`.

### 2.4. Snapshot TTL не проверяется

`PathSimulator` проверяет deadline запроса, но не `snapshot.state_valid_until_monotonic_ns`. Просроченный snapshot может вернуть `complete=True`. Для live simulation это запрещено; offline replay должен иметь явный режим, в котором TTL может быть зафиксирован историческим evidence.

### 2.5. Текущий worker capture фактически CPMM-only

`processSnapshotRequest` в `worker.ts` использует `raydiumStandardEngine.cpmmSimulationState`. Для этой задачи это допустимо и даже желательно. Нельзя одновременно заявлять готовность остальных протоколов.

## 3. Границы изменений

### Разрешено менять

- `src/market_data_lab/quote_broker.py`.
- `src/market_data_lab/amm_simulation/contracts.py`, `engine.py`, `backend.py`, `replay.py`, `codec.py` — только для нужных контрактов, TTL, evidence и безопасной интеграции.
- `src/market_data_lab/unified_market_data.py`.
- `src/market_data_lab/unified_perp_analyzer.py` — только строгая binding-проверка результата AMM и output schema.
- `src/market_data_lab/solana_quote_worker.py` — только bridge, необходимый для snapshot/simulation/evidence.
- `src/market_data_lab/solana_realtime_scanner.py` — только уже предусмотренный feature flag/конфигурационный wiring.
- `workers/solana-quote-worker/src/protocol.ts`, `worker.ts`, `simulation/path.ts`, `simulation/snapshots.ts` — только CPMM protocol path и request lifecycle.
- Новые тесты в `tests/` и `workers/solana-quote-worker/test/`.
- При необходимости отдельная документация `README_CPMM_VERTICAL_SLICE.md`.

### Запрещено в этой задаче

- Реализовывать или улучшать CLMM/DLMM/AMM v4.
- Добавлять новые RPC/provider endpoints или обходить существующие quota limits.
- Менять default: `[amm_simulation] enabled` остаётся `false`.
- Подключать новый backend в обычном scanner при выключенном feature flag.
- Масштабировать DEX quote линейно под perp lot size.
- Превращать research result в `execution_ready=true`.
- Использовать account balances, private keys, wallets, signing, transactions или orders.
- Запускать длительный live scanner без отдельной просьбы пользователя.
- Удалять старые paired exact-quote analysis kinds.
- Делать destructive git/file операции.

## 4. Целевые контракты

### 4.1. Domain AMM request/result

Использовать существующие immutable dataclasses из `amm_simulation/contracts.py`:

- `AmmSnapshot`;
- `AmmPathRequest`;
- `AmmPathResult`;
- `SwapLeg`;
- `SequentialUnwindResult`.

Не создавать вторую параллельную модель тех же объектов.

Обязательные свойства request:

- `schema_version`;
- уникальный `request_id`;
- `reason="shadow_sequential_unwind"` или эквивалентная существующая семантика;
- `snapshot` immutable;
- ровно две ноги для vertical slice;
- `initial_balances` содержит stable raw amount;
- `scenario_kind="frozen_market"`;
- `required_consistency="validated_multi_account_snapshot"`;
- положительный deadline или явное отсутствие deadline только в offline replay;
- `execution_policy="require_complete"`.

### 4.2. Публичные методы Broker

Устранить затенение. Рекомендуемый контракт:

```python
async def simulate_amm_path(
    self,
    request: AmmPathRequest,
    *,
    deadline_monotonic_ns: int | None = None,
) -> AmmPathResult:
    ...

async def estimate_local_path(self, request: QuoteRequest) -> QuoteResult:
    ...

async def simulate_local_path(self, request: QuoteRequest) -> QuoteResult:
    ...
```

`simulate_local_path(QuoteRequest)` — существующий legacy/local quote API; его нельзя ломать, потому что его используют текущие тесты `tests/test_quote_broker.py`.

`simulate_amm_path(AmmPathRequest)` — новый typed API. Если агент выберет другое имя, он обязан:

1. удалить ambiguity;
2. обновить все callers;
3. добавить runtime `inspect.signature` test;
4. объяснить выбор в changelog/spec update.

Typed AMM path не должен обращаться к remote provider и не должен расходовать remote quote budget.

### 4.3. TTL и clocks

Для live execution:

- если `state_valid_until_monotonic_ns` истёк до начала path, вернуть `status="state_unavailable"` или существующий эквивалент, `complete=False`;
- если срок истёк между ногами, вернуть typed failure и не выдавать полный PnL;
- не заменять фактический snapshot receipt временем analyzer evaluation;
- использовать monotonic clock для TTL/deadline;
- realtime clock использовать только для диагностической отметки возраста;
- source epoch, worker generation и boot ID должны присутствовать в evidence.

Для offline replay добавить явный параметр/режим `replay=True` либо эквивалентную существующую семантику. Нельзя молча игнорировать TTL в обычном live path.

### 4.4. CPMM result binding

После первой ноги локальный результат должен содержать фактический `actual_net_output_raw` и asset ID. Analyzer обязан проверить:

```text
result.base_asset_id == perp canonical base asset
result.buy_output_raw == quantity raw after exact conversion
result.pool_id == configured allowlisted pool
result.snapshot source_epoch/generation are accepted
```

Если raw quantity отличается от executable perp quantity — вернуть отказ и счётчик, не масштабировать цену или PnL.

## 5. Worker protocol changes

### 5.1. Request schema

В `workers/solana-quote-worker/src/protocol.ts` расширить `SimulatePathRequestMessage`:

```ts
interface InitialBalance {
  readonly asset_id: string;
  readonly amount_raw: string;
}

interface SimulatePathRequestMessage {
  type: "simulate_path_request";
  request_id: string;
  snapshot_token: string;
  legs: readonly SimulatePathLeg[];
  initial_balances: readonly InitialBalance[];
  deadline_monotonic_ns?: string;
}
```

Если текущая protocol convention использует number, сохранить единый existing convention; для raw amounts всегда использовать canonical decimal strings. Не смешивать `number` и `bigint` без явного преобразования.

Parser обязан:

- требовать непустой `initial_balances` для exact-in path;
- запрещать duplicate asset IDs;
- проверять canonical non-negative raw integer;
- проверять deadline;
- не принимать неизвестные дополнительные semantic fields молча, если это нарушает текущий protocol policy.

### 5.2. Worker handler

В `worker.ts`:

- передавать parsed balances в `simulatePathLegs(bundle, legs, initialBalances)`;
- при неизвестном/истёкшем snapshot token возвращать `state_unavailable`;
- при deadline до старта возвращать `deadline_exceeded`;
- результат после deadline не считать complete;
- сохранить observed snapshot immutable.

`cancel_simulation` в этой задаче можно оставить cooperative, но результат отменённого request не должен быть опубликован как complete. Добавить test late-result suppression.

### 5.3. Snapshot token

Для CPMM bundle token должен быть связан как минимум с:

- `request_id`;
- worker generation;
- boot ID;
- context slot;
- pool set;
- snapshot creation time.

Добавить bounded registry:

- максимальное число snapshot bundles;
- TTL eviction;
- удаление по generation/boot restart;
- счётчики `snapshot_registry_hits`, `misses`, `evictions`, `bytes`.

Точная policy может быть небольшой для первого vertical slice, например 32 bundles и configurable TTL, но она должна быть явной и протестированной. Не оставлять бесконечный `Map`.

## 6. Composition root integration

### 6.1. Feature flag

В `build_unified_market_data_scanner`:

1. Получить `_local_quote_source` как именованный объект.
2. Если `config.amm_simulation.enabled is False`:
   - не создавать local simulator;
   - не регистрировать local backend;
   - поведение текущего scanner и существующие analysis kinds не менять.
3. Если flag включён:
   - разрешить только `raydium_cpmm` в этом vertical slice;
   - проверить allowlisted pool и canonical asset pair;
   - создать WorkerBackend/bridge;
   - инициализировать LazySnapshotSequentialSimulator или эквивалентный lifecycle-aware объект;
   - передать simulator в `UnifiedPerpAnalyzer(sequential_amm_simulator=...)`;
   - зарегистрировать backend только в dedicated AMM API, не в legacy remote quote namespace.

### 6.2. Initialization lifecycle

Нельзя синхронно блокировать построение scanner на бесконечном RPC capture. Нужен один из допустимых вариантов:

- явный async initialization до начала event loop;
- lifecycle hook `initialize()` с bounded timeout;
- deferred initialization с состоянием `initializing` и явным отказом до готовности.

В каждом случае должны быть видны:

- `initialized`;
- `snapshot_id`;
- `pool_id`;
- `worker_generation`;
- `error`;
- `wallet_or_private_key_used=false`;
- `transactions_submitted=false`.

Если snapshot не capture-ится, анализатор должен пропускать sequential model с понятной причиной, а не использовать старую remote quote как post-state.

## 7. Analyzer integration

Изменять только DEX/perp sequential path в `src/market_data_lab/unified_perp_analyzer.py`.

Обязательное поведение:

1. Старые `dex_perp_entry_hedge` и `dex_perp_paired_exact_quote_*` остаются как есть.
2. Sequential analysis имеет отдельный `analysis_kind="dex_perp_sequential_flat_model"`.
3. `candidate_eligible=false` и `execution_ready=false` для первой поставки.
4. `dex_post_trade_pool_state_simulated=true` только если simulation result complete и binding checks пройдены.
5. `dex_sequential_snapshot_id`, `snapshot_hash`, `evidence_hash`, `worker_generation`, `pool_id`, leg IDs и raw quantities обязательны в result.
6. Perp PnL считается для фактически симулированного base quantity, а не для старой quote quantity.
7. Нельзя использовать DEX output другого pool/asset/epoch.
8. При mismatch записывать отдельный bounded counter и не создавать positive candidate.
9. При недостатке свежести/ликвидности/баланса выдавать отказ без PnL.

## 8. Evidence и replay

Использовать существующие `build_evidence_bundle`, `save_evidence_bundle`, `load_evidence_bundle`, `replay_evidence_bundle`.

Evidence должен включать:

- canonical request;
- canonical snapshot;
- canonical result;
- protocol/model version;
- worker boot ID и generation;
- source epoch;
- context slot/dependency vector;
- initial balances;
- path legs;
- deadline/receipt metadata;
- evidence hash.

Replay обязан:

- работать без RPC/network;
- воспроизвести status, completion, leg outputs, final balances и post-state hashes;
- упасть при изменении request/snapshot/result;
- явно помечать evidence как historical replay, чтобы TTL не маскировался под live freshness.

Worker export для этой задачи должен возвращать не только snapshot, но и достаточно данных для формирования полного evidence bundle. Если worker не может вернуть result/evidence напрямую, Python bridge обязан собрать полный bundle после успешной simulation и сохранить его bounded способом.

## 9. Файлы и рекомендуемые места тестов

### Python tests

Добавить отдельный файл:

`tests/test_cpmm_vertical_slice.py`

Обязательные тесты:

1. `test_broker_typed_amm_api_is_not_shadowed` — inspect signature и вызов typed request.
2. `test_legacy_quote_local_api_remains_compatible` — текущий `QuoteRequest` API.
3. `test_expired_snapshot_is_rejected_in_live_mode`.
4. `test_offline_replay_can_replay_expired_historical_snapshot_explicitly`.
5. `test_worker_backend_passes_initial_balances`.
6. `test_cpmm_two_leg_path_uses_post_state` — 100→90→99 для synthetic baseline и отдельный CPMM fixture.
7. `test_second_leg_sees_first_leg_state`.
8. `test_same_pool_asset_and_quantity_binding`.
9. `test_raw_quantity_mismatch_does_not_create_perp_model`.
10. `test_snapshot_epoch_or_generation_mismatch_is_rejected`.
11. `test_evidence_roundtrip_replays_without_network`.
12. `test_evidence_tamper_is_rejected`.
13. `test_feature_flag_off_keeps_current_composition`.
14. `test_feature_flag_on_composition_injects_simulator` через fake worker, без RPC.
15. `test_worker_failure_is_visible_and_does_not_fallback_to_remote_quote`.
16. `test_local_amm_path_does_not_consume_remote_budget`.

Не ослаблять существующие assertions и не удалять старые tests.

### TypeScript tests

Добавить или расширить:

`workers/solana-quote-worker/test/path.test.ts`

Обязательные cases:

1. initial balance required and sufficient balance completes;
2. missing balance returns `insufficient_balance`;
3. expired snapshot token returns `state_unavailable`;
4. expired deadline returns `deadline_exceeded`;
5. canceled request cannot emit complete result;
6. snapshot registry evicts by cap/TTL;
7. CPMM repeated pool visit consumes virtual post-state;
8. worker snapshot bundle is immutable;
9. canonical raw amounts remain strings across JSON boundary;
10. evidence/result contains generation, boot, epoch and snapshot ID.

### Composition test

Добавить Python integration test с fake source/worker, не требующий RPC:

`tests/test_unified_amm_composition.py`

Он должен построить `build_unified_market_data_scanner` для:

- flag off;
- flag on with one allowlisted CPMM pool;
- capture failure;
- successful capture + one synthetic matching perp event.

Проверить, что при flag off не появляется sequential model и не меняются старые counters.

## 10. Acceptance criteria

Задача считается выполненной только если одновременно выполнены все пункты:

### API

- В классе `QuoteBroker` нет двух методов с одним именем и разными контрактами.
- Typed AMM API имеет runtime test.
- Legacy quote tests проходят без изменений семантики.

### Correctness

- CPMM exact-in buy/sell использует virtual post-state.
- Initial balances явно передаются и проверяются.
- Нет linear scaling exact quote.
- Asset IDs, decimals, pool ID, source epoch и worker generation строго связаны.
- Просроченный live snapshot не создаёт complete/PnL result.
- `execution_ready` всегда false.
- `candidate_eligible` для первой поставки false.

### Integration

- Flag off: старый scanner path работает как раньше.
- Flag on: fake composition root реально создаёт simulator и выдаёт отдельный sequential research row.
- Capture failure не приводит к fallback на независимую remote quote как post-state.
- Remote provider budget не уменьшается от local simulation.

### Evidence

- Успешный результат можно сохранить и replay без сети.
- Tampered snapshot/request/result отвергается.
- Evidence содержит immutable snapshot ID/hash, request, result, generation/epoch/boot и raw quantities.

### Boundedness

- Snapshot registry bounded по item count и TTL.
- In-flight/cancel/deadline counters bounded и видимы.
- Нет бесконечного роста Map/journal в добавленном коде.

### Tests

В корне репозитория:

```bash
.venv/bin/python -m unittest discover -s tests
```

Должны пройти все существующие и новые Python-тесты.

В worker:

```bash
npm run check
npm test
```

Должны пройти typecheck и все существующие/новые TS-тесты.

Дополнительно обязательно:

```bash
.venv/bin/python -m py_compile scanner.py src/market_data_lab/*.py
```

Если добавлены модули в подпакет, pycompile должен покрыть и их.

## 11. Ручная проверка перед handoff

Агент обязан выполнить read-only проверки:

1. `inspect.signature(QuoteBroker.simulate_amm_path)` и legacy метода.
2. Synthetic 100→90→99 с post-state reserves.
3. Expired snapshot live rejection.
4. Worker JSON-lines request с initial balances.
5. Feature flag off/on composition test.
6. Evidence save/load/replay.
7. Проверка отсутствия wallet/order/transaction capabilities в изменённых файлах.
8. Проверка, что config default остаётся disabled.

Запуск полноценного непрерывного scanner для handoff не нужен и не должен выполняться агентом без отдельной команды пользователя.

## 12. Формат отчёта агента

В конце агент должен вернуть:

1. Список изменённых файлов.
2. Краткое описание API и lifecycle.
3. Какие тесты добавлены и команды запуска.
4. Точные результаты Python/TS test suites.
5. Evidence/replay smoke result.
6. Что осталось ограниченным.
7. Любой failing test с полным именем и причиной.
8. Подтверждение:

```text
no wallet used
no private key used
no transaction submitted
no order submitted
remote quote budget unchanged by local simulation
execution_ready remains false
```

## 13. Definition of Done

Definition of Done — это не «добавились классы» и не «тесты на positive output проходят».

Работа завершена только когда один CPMM request проходит все этапы:

```text
validated snapshot
→ explicit initial balance
→ exact two-leg post-state path
→ strict asset/quantity binding
→ separate non-executable research result
→ immutable evidence
→ offline deterministic replay
```

После этого можно планировать следующую задачу: Orca Whirlpool vertical slice. До этого не расширять CLMM/DLMM и не объявлять AMM production-ready.
