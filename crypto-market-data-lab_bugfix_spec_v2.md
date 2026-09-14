# Техническое задание v2: полное исправление runtime-багов, boundedness и Node/Solana performance в `crypto-market-data-lab`

**Репозиторий:** `https://github.com/xX-mabster-Xx/crypto-market-data-lab`  
**Целевая ветка:** актуальный `main` на момент начала работ  
**Тип работ:** correctness bugfix / reliability hardening / memory & CPU stabilization / regression testing  
**Статус предыдущего ТЗ:** считать **не выполненным** до доказательства обратного тестами и acceptance criteria из этого файла.  
**Этот документ полностью заменяет предыдущую версию ТЗ.**

---

## 1. Цель

Нужно довести live scanner и Solana Node worker до состояния, в котором длительный 24/7 запуск не приводит к постепенному росту памяти/CPU, старые состояния не переживают reconnect/epoch boundary, event delivery имеет формально понятную bounded semantics, а конфигурация и runtime wiring соответствуют фактической реализации.

Текущий код содержит две группы проблем:

1. **Ранее найденные correctness/lifecycle ошибки**, которые по актуальному `main` всё ещё нельзя считать исправленными: dynamic-notional cardinality, fake coalescing bus, epoch leakage, скрытые CEX reconnect, backoff, health accounting, QuoteBroker failure accounting, pacing, retention, wall-clock durations, config validation, Decimal canonicalization, Protocol indentation, Node dependency contract и пустой inflight test.
2. **Новые ошибки и performance-регрессии**, обнаруженные при повторном аудите: отсутствие stdout backpressure в Node worker, потенциально бесконечные Promise/RPC очереди, избыточная публикация Meteora/Orca dependency updates, неверная slot provenance, несовместимость `CexTopOfBookEvent` с `SolanaRouteEvaluator`, несогласованное wiring evaluator в `scanner.py`, bursty global refresh и отсутствие достаточной worker observability.

Работа считается законченной только после выполнения **всех** требований этого документа и прохождения unit/integration/stress/soak тестов.

---

# 2. Главные runtime-инварианты после исправления

После мержа должны быть истинны следующие утверждения.

1. **Bounded state cardinality.** Изменение цены/точного notional не создаёт бесконечное число state keys, analyzer cache entries или secondary index entries.
2. **Bounded queues everywhere.** Ни Python event plane, ни Node stdout path, ни Node RPC scheduler, ни per-pool update processing не имеют неограниченной очереди/Promise chain.
3. **Latest-state semantics.** Высокочастотные заменяемые state updates допускается coalesce'ить по стабильному ключу, но нельзя случайно вытеснить единственное обновление другого ключа.
4. **Strict epoch isolation.** После смены `source_epoch` ни store, ни analyzer cache, ни full-depth resolver не возвращают старое состояние как валидное.
5. **Reconnect provenance.** Любой reconnect, который способен нарушить непрерывность market state, либо повышает epoch, либо явно пересобирает состояние с новым provenance boundary.
6. **Monotonic durations.** TTL, persistence, retry windows и idle time измеряются только monotonic clock. Wall clock остаётся для persisted/user-facing timestamps.
7. **Accepted-state health.** Health различает transport-received и state-accepted updates; rejected/out-of-order пакет не может сделать источник здоровым.
8. **Per-request pacing.** Rate limit применяется к фактическим HTTP/RPC starts, а не к внешнему логическому `quote_round()`.
9. **Backpressure-aware Node protocol.** `process.stdout.write() == false` не игнорируется. High-frequency state messages coalesce'ятся; control/results не теряются.
10. **Bounded per-pool processing.** Один hot Solana account не может построить бесконечную Promise chain из устаревших обновлений.
11. **Correct slot provenance.** Slot core pool account не подменяется slot'ом tick/bin dependency. Компонентные slots доступны отдельно.
12. **Correct scanner wiring.** Если manifest/config утверждает, что local route evaluator enabled и routes активны, evaluator действительно подключён; иначе startup должен fail-fast либо feature должен быть явно disabled.
13. **Stable memory.** В soak test Node RSS/heap после warm-up не демонстрируют монотонный неограниченный рост при фиксированном universe.
14. **Stable CPU.** При одинаковом входном потоке worker не создаёт постоянный churn из JSON/full-state emissions, который можно заменить coalescing/debounce без потери нужной semantics.
15. **Observable boundedness.** Размеры очередей, coalescing/drop counters, worker memory и refresh activity можно увидеть в status/metrics.

---

# 3. Приоритеты и порядок блокировки live-run

## P0 — исправить до следующего длительного live-run

- `BUG-001` dynamic-notional → unbounded state/analyzer keys.
- `BUG-002` FIFO-drop event bus вместо keyed coalescing.
- `BUG-003` analyzer caches переживают `source_epoch`.
- `BUG-004` скрытые CEX reconnect нарушают epoch semantics.
- `BUG-005` retention/capacity не гарантируют bounded memory.
- `BUG-016` Node stdout path игнорирует backpressure.
- `BUG-017` Raydium CLMM `updateChain` может расти как бесконечная Promise chain.
- `BUG-018` RPC pacer/scheduler имеет потенциально неограниченный backlog.
- `BUG-019` Meteora/Orca dependency updates создают лишний full-state/JSON churn.
- `BUG-021` `CexTopOfBookEvent` сломал real integration `SolanaRouteEvaluator`.
- `BUG-022` `scanner.py` заявляет local evaluator/routes, но composition root не гарантирует фактическое подключение.

## P1 — выполнить в той же серии

- `BUG-006` reconnect backoff не reset после стабильной работы.
- `BUG-007` health обновляется до state acceptance.
- `BUG-008` QuoteBroker key mismatch обходит failure accounting.
- `BUG-009` pacing находится не на уровне реального request.
- `BUG-010` lifecycle durations используют wall clock.
- `BUG-020` Meteora/Orca/Raydium slot provenance смешивает core/dependency slots.
- `BUG-023` глобальный 15-секундный refresh создаёт ненужные bursts и повторный emit unchanged state.
- `BUG-024` недостаточная Node worker observability для memory/backlog диагностики.

## P2 — correctness / reproducibility / type safety

- `BUG-011` numeric config принимает `NaN`/`inf` и некорректные ints.
- `BUG-012` несовместимая Decimal canonicalization.
- `BUG-013` `DexQuoteProvider.config()` объявлен с неправильным indentation.
- `BUG-014` Node engine/dependency manifest не согласованы и используются `latest`.
- `BUG-015` remote inflight-dedup test фактически пустой.

---

# 4. Файлы/узлы, которые обязательно проверить и изменить

Минимальный набор, но не ограничение:

### Python

- `scanner.py`
- `src/market_data_lab/realtime_scanner.py`
- `src/market_data_lab/versioned_market_state.py`
- `src/market_data_lab/polling_quote_sources.py`
- `src/market_data_lab/unified_market_data.py`
- `src/market_data_lab/unified_cycle_analyzer.py`
- `src/market_data_lab/unified_perp_analyzer.py`
- `src/market_data_lab/cex_book_streams.py`
- `src/market_data_lab/solana_realtime_scanner.py`
- `src/market_data_lab/solana_route_evaluator.py`
- `src/market_data_lab/quote_broker.py`
- `src/market_data_lab/dex_quotes.py`
- тесты в `tests/`

### Node/Solana worker

- `workers/solana-quote-worker/src/protocol.ts`
- `workers/solana-quote-worker/src/worker.ts`
- `workers/solana-quote-worker/src/rpcPacer.ts`
- `workers/solana-quote-worker/src/raydiumClmm.ts`
- `workers/solana-quote-worker/src/raydiumStandard.ts` / аналогичные engine files, если используют общий emitter/pacer
- `workers/solana-quote-worker/src/meteoraDlmm.ts`
- `workers/solana-quote-worker/src/orcaWhirlpool.ts`
- `workers/solana-quote-worker/package.json`
- `workers/solana-quote-worker/package-lock.json`
- Node tests / test harness / scripts.

Перед правкой агент обязан сверить точные имена символов по текущему `main`. Номера строк в этом ТЗ не являются контрактом.

---

# 4. Общая стратегия реализации

Исправления нельзя делать как независимые локальные заплатки. Нужно ввести три сквозных понятия и затем использовать их последовательно:

### 4.1. Stable state identity

Identity живого состояния должна отвечать на вопрос **«какой логический слот состояния это обновление заменяет?»**, а не **«какое exact числовое значение было запрошено в этот момент?»**.

Пример:

- неправильно: `provider:0.001524213567:buy_base`;
- правильно для dynamic reference quote: `provider:triangle-reference-usdt-100:buy_base`.

Точное `requested_notional_quote` остаётся внутри payload и может меняться каждый round, но identity key остаётся стабильным.

### 4.2. Explicit source epoch boundary

Epoch transition — отдельное control-plane событие/вызов. Analyzer обязан знать текущую epoch каждого источника и purge/ignore старые данные.

### 4.3. Keyed latest-state delivery

Event bus должен coalesce несколько pending updates **одного и того же state key**, но не выбрасывать произвольное событие другого state key только потому, что hot source заполнил очередь.

---

# 5. BUG-001 — устранить unbounded cardinality dynamic quote keys

## 5.1. Проблема

Dynamic triangle quote amount пересчитывается из текущего CEX ask. Поэтому exact requested amount меняется при движении цены.

Сейчас exact notional участвует в identity event/cache, из-за чего долгий процесс постепенно получает новые ключи:

```text
provider:0.001524213567:buy_base
provider:0.001524190238:buy_base
provider:0.001524291772:buy_base
...
```

Это раздувает одновременно:

- `RollingStateStore._latest`;
- `RollingStateStore._history`;
- `VersionedMarketState._records`;
- `VersionedMarketState._keys_by_source`;
- `UnifiedCycleAnalyzer._triangle_quotes`;
- provider-to-key indexes analyzer'а;
- аналогичные DEX quote caches/indexes в `UnifiedPerpAnalyzer`.

Дополнительно CPU cost может расти, если каждый новый CEX book update перебирает все исторически накопленные keys.

## 5.2. Требуемое решение

### 5.2.1. Ввести понятие `quote_slot_id`

Расширить `QuoteRoundInput` стабильным идентификатором логического quote slot:

```python
@dataclass(frozen=True, slots=True)
class QuoteRoundInput:
    amount: Decimal
    reference_notional_usdt: Decimal | None = None
    slot_id: str | None = None
```

Допустимо другое имя (`quote_slot_id` предпочтительнее), но смысл должен быть именно таким.

Требования к `slot_id`:

- не зависит от текущей рыночной цены;
- детерминирован для одной конфигурации scanner;
- различает реально разные одновременно отслеживаемые notionals;
- не содержит бесконтрольно меняющееся decimal значение;
- валидируется как непустая строка разумной длины.

### 5.2.2. Правила формирования slot

Для статического exact notional:

```text
notional:<canonical-decimal>
```

Например:

```text
notional:100
notional:1000
```

Для динамического triangle reference-size:

```text
triangle-reference-usdt:<canonical reference notional>
```

Например при reference size 100 USDT:

```text
triangle-reference-usdt:100
```

Сам рассчитанный base amount (`100 / ask`) остаётся в `amount` и payload, но **не входит в identity**.

### 5.2.3. Расширить `ExactInputQuote`

Добавить стабильный `quote_slot_id` к внутренней модели quote.

В persisted/diagnostic output сохранить оба значения:

- `quote_slot_id` — identity;
- `requested_notional_quote` — реально запрошенный размер данного quote.

Не заменять actual amount slot'ом: они решают разные задачи.

### 5.2.4. Изменить MarketEvent key

В `PollingDexQuoteSource._publish_record()` вместо exact notional использовать stable slot:

```text
<source>:<quote_slot_id>:<direction>
```

`instrument_or_pool_id`/state key должны следовать той же стабильной семантике.

### 5.2.5. Изменить analyzer caches

`UnifiedCycleAnalyzer`:

```text
(provider, quote_slot_id, direction)
```

вместо:

```text
(provider, exact_notional, direction)
```

`UnifiedPerpAnalyzer` — аналогично.

Новое обновление одного slot должно **заменять** предыдущее, а не добавлять новый key.

### 5.2.6. Малформированные provider records

Если provider вернул record, который нельзя сопоставить ни одному `QuoteRoundInput`, запрещено генерировать новый уникальный state key из произвольных полей ответа.

Нужно:

- классифицировать запись как malformed/unmapped provider response;
- увеличить диагностический счётчик;
- при необходимости публиковать один стабильный diagnostic key на source/error class;
- не помещать такую запись в strategy quote cache.

## 5.3. Acceptance tests

Обязательные тесты:

1. Сгенерировать не менее 10 000 (желательно 100 000 в stress test) dynamic quote updates одного slot с разными `amount`.
2. После обработки:
   - количество live quote slots на provider/direction остаётся `1`;
   - размер analyzer quote cache не растёт пропорционально числу updates;
   - state store не содержит 10 000 keys;
   - последний quote содержит последний фактический `requested_notional_quote`.
3. Два статических notionals `100` и `1000` должны оставаться двумя разными slots.
4. Dynamic `triangle-reference-usdt:100` и static `notional:100` должны быть разными slots.
5. Buy и sell directions не должны перезаписывать друг друга.

---

# 6. BUG-012 — единая canonicalization Decimal

Этот пункт реализуется вместе с `BUG-001`.

## 6.1. Проблема

В одном месте mapping key строится через `format(decimal, "f")`, а provider record может использовать representation с удалёнными trailing zeros. Например:

```text
Decimal("1.00") -> "1.00"
provider -> "1"
```

Lookup не совпадает.

## 6.2. Требуемое решение

Создать одну публичную внутреннюю helper-функцию, например:

```python
canonical_decimal_text(value: Decimal) -> str
```

Рекомендуемый файл:

```text
src/market_data_lab/numeric_text.py
```

Либо существующий общий numeric utility module, если он уже есть.

Требования:

- fixed-point representation, без scientific notation;
- удалить trailing zeros только из fractional part;
- удалить завершающую decimal point;
- `-0`, `-0.0` -> `0`;
- finite values only;
- одинаковая функция должна использоваться producer и consumer mapping;
- не применять `float()` для Decimal identity.

Примеры:

```text
Decimal("1.00")      -> "1"
Decimal("0.5000")    -> "0.5"
Decimal("100")       -> "100"
Decimal("1E-8")      -> "0.00000001"
Decimal("-0.000")    -> "0"
```

## 6.3. Tests

Unit tests на все примеры выше и на non-finite Decimal.

---

# 7. BUG-002 — заменить FIFO-drop bus на настоящий keyed coalescing bus

## 7.1. Проблема

Текущий `CoalescingEventBus` при заполнении очереди удаляет самое старое произвольное событие. Это не coalescing по state key.

Из-за этого hot CEX book может вытеснить единственный свежий DEX/perp update. State store при этом уже содержит DEX update, но analyzer работает по delivered events и собственным caches, поэтому может продолжить использовать старые данные до следующего quote.

## 7.2. Требуемая семантика

Для каждого `MarketEvent.key` в pending queue одновременно должен существовать максимум один pending update.

Если приходит новый event с key, который уже pending:

- старый pending payload заменяется новым;
- queue position может оставаться прежней;
- увеличивается `coalesced_updates`;
- distinct key не теряется.

Если приходит новый distinct key и достигнут capacity:

- **не удалять случайный другой key**;
- применить bounded backpressure: publisher ждёт освобождения capacity;
- stop/shutdown должен корректно разбудить ожидающих publishers.

Capacity должен измерять количество **distinct pending keys**, а не количество raw updates.

## 7.3. Рекомендуемая реализация

Не требуется буквально копировать псевдокод, но реализация должна иметь аналогичные свойства:

```python
pending_latest: dict[str, MarketEvent]
pending_order: deque[str]
condition: asyncio.Condition
```

Алгоритм `publish(event)`:

1. Сначала попытаться принять event в `RollingStateStore`.
2. Если state layer отверг event — вернуть `accepted=False`, очередь не менять.
3. Под `Condition`:
   - если key уже в `pending_latest`, заменить payload и increment `coalesced_updates`;
   - иначе ждать, пока `len(pending_latest) < capacity`;
   - затем добавить key в `pending_order` и payload в `pending_latest`.
4. Разбудить consumer.

Алгоритм `next_event()`:

1. ждать непустой `pending_order`;
2. взять oldest key;
3. взять latest payload для key;
4. удалить key из pending structures;
5. notify publishers, ожидающих capacity;
6. вернуть event.

Важно не создавать deadlock: ожидание `Condition.wait()` обязано отпускать lock.

## 7.4. Метрики

Добавить/сохранить минимум:

- `accepted_events`;
- `rejected_state_events`;
- `coalesced_updates`;
- `pending_keys`;
- `queue_capacity`;
- `queue_high_watermark`;
- `publisher_backpressure_waits`;
- опционально total backpressure wait time.

Старое поле `dropped_events`:

- либо сохранить для совместимости и после исправления держать `0`;
- либо удалить только с явным schema version bump.

Не переименовывать status fields молча.

## 7.5. Исправить текст status policy

Удалить/заменить формулировку вроде `live bus receives every update`, если raw updates могут coalesce.

Корректная формулировка по смыслу:

> Accepted state updates use keyed latest-state coalescing: superseded pending updates of the same state key are replaced; distinct-key saturation applies bounded backpressure rather than arbitrary dropping.

## 7.6. Tests

### Test A — hot key coalescing

- capacity 4;
- быстро отправить 1000 updates key `A`;
- consumer медленный;
- убедиться, что queue cardinality не растёт;
- доставленный последний `A` соответствует update #1000;
- `coalesced_updates > 0`.

### Test B — unique key не вытесняется

- заполнить очередь distinct keys;
- publisher следующего distinct key должен ждать, а не удалять старый;
- после consume одного key publisher продолжает;
- все distinct keys в итоге доставлены.

### Test C — mixed hot/cold

- `A` публикуется тысячами;
- `B` публикуется один раз;
- analyzer/consumer обязан получить `B`;
- hot `A` не имеет права вытеснить `B`.

### Test D — rejected store event

Out-of-order/duplicate rejected state event не должен попасть в bus.

---

# 8. BUG-005 — сделать state store действительно bounded

Stable quote identity устраняет главный источник роста, но generic state layer всё равно должен иметь собственные предохранители.

## 8.1. Проблема

`retention_seconds` очищает history только при следующем `add()` того же key. Если key перестал обновляться, старый key/history может оставаться в памяти неограниченно долго.

`VersionedMarketState.advance_source_epoch()` инвалидирует records, но не обязательно физически удаляет их и membership в `_keys_by_source`.

## 8.2. Требуемое решение

### 8.2.1. Добавить global idle-key retirement

`RollingStateStore` должен уметь периодически удалять keys, которые не обновлялись дольше retention.

Добавить метод по смыслу:

```python
sweep(now_monotonic_ns: int | None = None) -> SweepStats
```

При retirement key удалить его из:

- `_latest`;
- `_history`;
- `VersionedMarketState`;
- всех index structures, связанных с key.

### 8.2.2. Добавить hard maximum number of state keys

Добавить конфиг `max_state_keys`.

Если после TTL sweep количество keys всё ещё превышает limit:

- удалить oldest idle/LRU keys до limit;
- increment отдельный `capacity_evictions` counter;
- удаление должно быть детерминированным.

Рекомендуемый default: `65536`, если нет более обоснованного существующей конфигурацией значения. Если выбран другой default, зафиксировать его в docs/tests.

### 8.2.3. `VersionedMarketState.retire/remove`

Добавить безопасный API физического удаления state key, например:

```python
retire(state_key: str) -> bool
```

Он обязан:

- удалить `_records[state_key]`;
- удалить key из `_keys_by_source[source]`;
- удалить пустой source set при необходимости;
- не оставлять dangling index entries.

Не обращаться к private dicts `VersionedMarketState` из `RollingStateStore` напрямую.

### 8.2.4. Когда запускать sweep

Минимум один из вариантов обязателен:

- scanner вызывает sweep периодически с ограниченной частотой;
- status snapshot вызывает sweep;
- `add()` запускает amortized sweep не чаще заданного интервала.

Не делать O(number_of_all_keys) scan на каждый market tick.

## 8.3. Tests

1. Inactive key физически исчезает после retention + sweep.
2. `_keys_by_source` не содержит retired key.
3. При `max_state_keys=N` store никогда не остаётся с >N live keys после sweep/admission.
4. Source epoch transition не перебирает все исторически retired keys.
5. Active key не удаляется только потому, что соседние keys старые.
6. `history` и `latest` имеют согласованную cardinality semantics.

---

# 9. BUG-003 — строгая инвалидация analyzer caches по `source_epoch`

## 9.1. Проблема

`RollingStateStore` знает о смене source epoch, но `UnifiedCycleAnalyzer` и `UnifiedPerpAnalyzer` имеют собственные caches. Старый quote может остаться там после restart и быть использован на свежем CEX update, если проходит обычную freshness/skew проверку.

Fresh timestamp не является заменой epoch validation.

## 9.2. Требуемая архитектура

Ввести явный control-plane object, например:

```python
@dataclass(frozen=True, slots=True)
class SourceEpochChange:
    source: str
    source_epoch: int
    reason: Literal["initial_start", "restart", "transport_reconnect"]
    realtime_ns: int
    monotonic_ns: int
```

И callback/handler в scanner composition:

```python
async def handle_source_epoch(change: SourceEpochChange) -> None: ...
```

Не имитировать epoch change обычным market data event.

## 9.3. Порядок epoch transition

Перед публикацией **любого** market event новой epoch:

1. scanner увеличивает/назначает epoch;
2. state store инвалидирует старую epoch;
3. вызывается control-plane epoch handler;
4. analyzers очищают старые caches/dependencies;
5. только после этого source может публиковать market events новой epoch.

Это ordering requirement является частью acceptance criteria.

## 9.4. `UnifiedCycleAnalyzer`

Нужно:

- хранить current known epoch для relevant source;
- при epoch advance DEX source удалить все quote cache slots этого source/provider;
- убрать dirty/pending work, которое ссылается на старый quote;
- active candidates, evidence которых зависит от старого source, закрыть с reason типа `source_epoch_advanced` либо немедленно сделать невалидными так, чтобы они не могли продлиться старым evidence;
- при каждом quote use выполнить defense-in-depth проверку `cached_epoch == current_epoch`.

Если source name и provider name — разные namespace, добавить явное отображение вместо string parsing в нескольких местах.

## 9.5. `UnifiedPerpAnalyzer`

Требование строже, потому что analyzer хранит не только DEX quotes, но и spot/perp legs.

Если внутренние leg dataclasses не несут provenance, добавить минимум:

- `source`;
- `source_epoch`.

Изменить update methods так, чтобы они получали `MarketEvent` либо source/epoch metadata вместе с value. Нельзя сначала отбросить envelope metadata, а затем пытаться восстановить provenance из venue name.

При epoch advance:

- удалить DEX quotes старой epoch этого source;
- удалить/инвалидировать spot legs этого source;
- удалить/инвалидировать perp legs этого source;
- пересчитать/закрыть active candidates, зависящие от них;
- удалить pending dirty calculations на устаревшем evidence.

## 9.6. Обработка запоздалых событий

После того как current epoch source стала `N`, событие epoch `<N` должно быть отвергнуто на earliest practical boundary и не попадать в analyzer cache.

Событие epoch `>N` без соответствующего control transition должно считаться invariant violation или инициировать безопасную синхронизацию — выбрать один вариант и покрыть тестом. Предпочтительно invariant violation в development/test, потому что scanner должен гарантировать ordering.

## 9.7. Tests

Критический regression scenario:

```text
DEX quote epoch=1 accepted
source transitions to epoch=2
fresh CEX book arrives before first epoch=2 DEX quote
```

Ожидание:

- zero calculations используют epoch=1 quote;
- active candidate не refresh'ится старым quote;
- старый quote удалён/не выбирается.

Дополнительно:

- delayed epoch=1 event после transition к 2 игнорируется;
- первый epoch=2 quote снова разрешает calculation;
- CEX spot/perp cached legs аналогично инвалидируются.

---

# 10. BUG-004 — убрать скрытые CEX reconnect, нарушающие epoch semantics

## 10.1. Проблема

CEX stream имеет внутренний reconnect loop. В результате transport connection может оборваться и установиться заново внутри того же outer `source.run()`. `RealtimeScanner` не видит reconnect и не меняет `source_epoch`.

Это разрушает смысл epoch как connection/session boundary.

## 10.2. Предпочтительное решение: один владелец reconnect

**Reconnect должен принадлежать `RealtimeScanner` supervisor.**

`_EventedBookStream` и venue-specific/sharded book streams должны представлять одну transport session. При terminal socket/receiver failure ошибка должна выйти наружу до `CexBookStateSource.run()`, а затем до `RealtimeScanner._source_supervisor()`.

Supervisor уже отвечает за:

- backoff;
- новую source epoch;
- state invalidation;
- health/error accounting.

Два независимых reconnect loops использовать не следует.

## 10.3. Что изменить в stream layer

### 10.3.1. Удалить бесконечный reconnect loop из `_EventedBookStream`

Одна lifecycle iteration:

1. connect;
2. subscribe;
3. receive/process;
4. при transport/protocol failure сохранить diagnostic error и завершить task с exception;
5. caller должен иметь способ получить exception.

### 10.3.2. `next_update()` не должен зависнуть после смерти receiver task

Если receiver завершился с exception и новых updates нет, `next_update()` должен пробросить failure/terminal state, а не ждать бесконечно.

### 10.3.3. Очистка transport-local state

При создании новой session/новой source epoch очистить всё reconstruction state, которое нельзя переносить через reconnect:

- bid/ask delta maps;
- sequence/checksum state;
- snapshot-ready flags;
- latest book для старой session;
- любые exchange-specific incremental state.

Нельзя применять delta новой WebSocket session к book старой session.

### 10.3.4. Sharded streams

Если source состоит из нескольких WebSocket shards и любой shard terminally падает:

- весь logical source должен считаться failed;
- outer supervisor запускает новую epoch для всего source;
- все shards пересоздаются в согласованной epoch.

Не оставлять часть shards в старой session, а часть в новой при одном `source_epoch`.

Если команда решит оставить independent shard epochs, это уже более крупная архитектурная смена и требует отдельного formal design; в рамках этого ТЗ предпочтителен whole-source restart.

## 10.4. `CexBookStateSource`

В начале каждого нового outer run:

- `_latest_books.clear()`;
- создать fresh stream/session;
- не публиковать ничего до получения корректного fresh snapshot/state;
- transport exception обязан завершить `run()` exception'ом.

## 10.5. Tests

1. Session 1 публикует book.
2. Simulated socket disconnect.
3. `CexBookStateSource.run()` завершается failure.
4. `RealtimeScanner` создаёт epoch 2 до следующего event.
5. `latest_books` старой session очищен.
6. Analyzer cache старой epoch очищен через `SourceEpochChange`.
7. Delta без fresh snapshot после reconnect не может быть опубликована как valid book.
8. Для sharded source failure одного shard вызывает whole-source epoch transition.

---

# 11. BUG-006 — корректный reset exponential reconnect backoff

## 11.1. Проблема

Retry delay увеличивается после каждого failure, но практически не сбрасывается после длительного успешного run, потому что `source.run()` обычно не возвращается «успешно» до shutdown.

В итоге несвязанные редкие ошибки способны со временем довести delay до maximum.

## 11.2. Требуемое решение

Добавить stable-run reset threshold, например config:

```text
supervisor_retry_reset_after_seconds = 30.0
```

При старте run сохранить `run_started_monotonic`.

При failure вычислить duration через monotonic clock.

Если run длился >= threshold, считать предыдущую failure streak завершённой и применять initial retry delay к текущему failure.

Желаемая семантика:

```text
быстрый failure -> 0.25
быстрый failure -> 0.5
быстрый failure -> 1.0
стабильная работа 60s
failure -> снова 0.25
```

Не сбрасывать streak просто после одного market event: источник, который каждый раз успевает выдать один event и тут же падает, должен продолжать exponential backoff.

## 11.3. Tests

Использовать fake clock/sleep либо инъекцию времени — unit test не должен реально ждать десятки секунд.

Проверить:

- rapid failures дают exponential progression;
- max cap соблюдается;
- stable run сбрасывает к initial;
- shutdown не считается failure.

---

# 12. BUG-007 — health должен обновляться только по принятому состоянию

## 12.1. Проблема

Сейчас source health может `observe(event)` до того, как state store решит принять событие. Rejected duplicate/out-of-order event способен обновить `last_event`, увеличить updates и очистить `last_error`.

## 12.2. Требуемое решение

`bus.publish()` должен возвращать результат admission, минимум:

```python
accepted: bool
```

Лучше небольшой type:

```python
@dataclass(frozen=True, slots=True)
class PublishResult:
    accepted: bool
    coalesced: bool
```

`SourceHealth.observe_accepted(event)` вызывать только если `accepted=True`.

Отдельно можно вести transport counters:

- `received_events`;
- `accepted_events`;
- `rejected_events`.

`last_error` должен очищаться только событием, которое действительно стало current accepted state (или по другой чётко определённой health policy, покрытой тестами).

## 12.3. Tests

- valid event очищает last_error;
- duplicate/out-of-order rejected event не очищает last_error;
- rejected event не обновляет accepted freshness;
- received/rejected counters отражают обе стороны.

---

# 13. BUG-008 — `QuoteBroker` key mismatch должен проходить failure accounting

## 13.1. Проблема

В remote и local execution paths backend result с `result.key != request.key` превращается в failure через ранний `return`, поэтому стандартный `observe_result()`/failure gate/cooldown может не выполниться.

## 13.2. Требуемое решение

Избавиться от special early return.

Схема должна быть одинаковой для remote/local:

```python
result = await backend(request)

if result.key != request.key:
    result = make_provider_error(...)

observe_result(result)
return result
```

Все не-cancelled terminal results должны пройти единый accounting path ровно один раз.

Проверить, что budget release/finally semantics при этом не ломаются.

## 13.3. Tests

- backend возвращает mismatched key;
- caller получает expected `provider_error`;
- failure counter/gate обновляется;
- immediate повторный request ведёт себя в соответствии с уже существующей policy `provider_error` (например cooldown/endpoint gate), а не бесконтрольно снова вызывает backend;
- remote и local path имеют симметричные тесты.

---

# 14. BUG-009 — pacing на каждый фактический outbound request

## 14.1. Проблема

Shared pacer вызывается один раз перед `provider.quote_round()`, но один round может выполнить несколько фактических HTTP/RPC/WS request actions — например buy batch и reverse sell batch.

Следовательно `minimum_request_interval` фактически не гарантируется между network requests.

## 14.2. Требуемая семантика

Pacer должен находиться непосредственно у границы каждого реального outbound request.

Для каждого provider определить метод, который фактически инициирует remote request, и вызвать:

```python
await request_pacer.wait()
```

непосредственно перед ним.

## 14.3. Не допустить double pacing

Providers, у которых уже есть per-request pacer (например соответствующие текущей реализации Raydium/Jupiter), не должны получать второй внешний wait на тот же request.

Для providers, использующих shared quota domain (STON.fi / Omniston / Uniswap и другие актуальные в `main`), передать один и тот же pacer instance непосредственно внутрь provider.

`PollingDexQuoteSource` после этого:

- не должен вызывать `.wait()` один раз на весь round;
- может сохранять ссылку на shared pacer для `defer(cooldown)`/quota coordination;
- желательно переименовать поле из двусмысленного `shared_request_pacer` в `shared_quota_pacer` либо документировать его новую роль.

## 14.4. Tests

С fake clock/pacer:

1. Один round с двумя фактическими requests — starts разделены минимум configured interval.
2. Два sibling sources одного quota domain используют общий pacer и не стартуют одновременно.
3. Cooldown/defer одного source влияет на общий quota domain, если такова текущая intended policy.
4. Provider с уже встроенным pacing не ждёт два интервала на один запрос.

---

# 15. BUG-010 — lifecycle durations только через monotonic clock

## 15.1. Проблема

Candidate idle timeout/minimum persistence/console throttle местами используют `time.time_ns()` и разницу realtime timestamps.

Wall clock может прыгнуть вперёд/назад из-за NTP, ручной коррекции или VM clock changes.

## 15.2. Правило

Использовать:

- `time.time_ns()` / realtime timestamp — для persisted event time, логов, JSON, correlation с exchange timestamps;
- `time.monotonic_ns()` — для elapsed duration, TTL, freshness, timeout, persistence threshold, throttle.

## 15.3. Candidate state

Расширить `_ActiveCandidate` в обоих analyzers минимум полями:

```text
started_realtime_ns
started_monotonic_ns
last_seen_realtime_ns
last_seen_monotonic_ns
```

При observe одной candidate фиксировать согласованную пару clocks одного evaluation point.

### Persistence

```text
observed_monotonic_ns - started_monotonic_ns
```

### Idle close

```text
now_monotonic_ns - last_seen_monotonic_ns
```

### Persisted close/open timestamps

Оставлять realtime.

## 15.4. Console/report throttle

Если используется elapsed interval для throttling — перевести на monotonic.

## 15.5. Tests

Инъецировать/fake clock:

- wall clock прыгает +1 час, monotonic +100ms — candidate не должен внезапно истечь;
- wall clock прыгает назад — timeout всё равно срабатывает по monotonic;
- persisted timestamp остаётся realtime.

---

# 16. BUG-011 — строгая validation config numeric values

## 16.1. `_positive_float`

После parse обязательно:

```python
math.isfinite(parsed)
```

и `parsed > 0`.

Отвергать:

- `NaN`;
- `+inf`;
- `-inf`;
- zero;
- negative.

## 16.2. `_positive_int`

Запрещено молча делать:

```text
1.9 -> 1
```

Требования:

- reject `bool`;
- integer принимается, если >0;
- float допустим только если finite и `is_integer()`;
- string допустим только если представляет точное целое значение по уже принятой project policy;
- non-integral numeric отвергать;
- `NaN`/`inf` отвергать.

Желательно использовать общий strict numeric parser для `_positive_int` и `_non_negative_int`, чтобы правила не расходились.

## 16.3. Tests

Минимум:

```text
nan       -> reject
inf       -> reject
-inf      -> reject
1.9 int   -> reject
True      -> reject
2         -> accept
2.0       -> accept (если сохраняется текущая permissive policy)
"2"       -> accept
0         -> reject для positive
```

---

# 17. BUG-013 — исправить `DexQuoteProvider` Protocol

## 17.1. Проблема

`config()` из-за расположения/отступа находится после `return` внутри другой функции и не является методом `DexQuoteProvider` Protocol.

## 17.2. Требуемое решение

Переместить declaration внутрь Protocol:

```python
class DexQuoteProvider(Protocol):
    name: str

    async def quote_round(...): ...

    def config(self) -> Mapping[str, Any]: ...
```

Если `config()` по архитектуре действительно optional, не притворяться, что он mandatory Protocol method. В таком случае:

- либо оставить base Protocol без `config` и завести отдельный capability Protocol;
- либо стандартизировать все production providers, чтобы каждый реализовывал `config()`.

Предпочтительно второе, если фактический код уже ожидает metadata/config от всех production providers.

## 17.3. Tests/checks

- `python -m compileall`;
- project type checker, если настроен;
- unit test/config enumeration всех production providers.

---

# 18. BUG-014 — привести Node runtime и dependency manifest к воспроизводимому состоянию

## 18.1. Проблема

Worker manifest заявляет Node `>=22`, тогда как зафиксированный `@drift-labs/sdk 2.156.0` требует Node `^24.0.0`.

Кроме того, несколько top-level dependencies указаны как `latest`, хотя `package-lock.json` уже содержит конкретно разрешённый dependency graph. Regeneration lockfile в будущем может неожиданно обновить SDK.

## 18.2. Требуемое решение

### Node

Установить worker engine минимум:

```json
"engines": {
  "node": ">=24 <25"
}
```

Если команда осознанно хочет более широкий range, он обязан удовлетворять всем direct dependencies; Node 24 должен быть CI/reference runtime.

Обновить все места репозитория, где зафиксирована версия Node:

- CI workflow;
- Docker/devcontainer;
- README;
- scripts;
- package metadata.

### Dependency pinning

Для top-level worker dependencies/devDependencies убрать `"latest"`.

**Не выполнять произвольный upgrade.**

Нужно взять версии, уже фактически зафиксированные текущим `package-lock.json`, записать их как exact versions в `package.json`, затем регенерировать/проверить lockfile под Node 24.

Caret ranges, которые намеренно используются (`^...`), тоже рекомендуется заменить exact version для simulation worker, если цель проекта — детерминированный replay. Если оставляется range, это должно быть осознанно документировано.

Особенно важны:

- `@meteora-ag/dlmm`;
- `@raydium-io/raydium-sdk-v2`;
- `@solana/web3.js`;
- `bn.js`;
- dev tooling (`tsx`, `typescript`, `@types/node`, `@types/bn.js`).

`@drift-labs/sdk` уже exact и должен остаться совместимым с выбранным Node runtime.

### Documentation consistency

Если README_AMM_SIMULATION или другая документация заявляет конкретные pinned SDK versions, автоматически/вручную сверить их с `package.json` и lockfile. После исправления три источника не должны противоречить друг другу.

## 18.3. Acceptance

На clean environment Node 24:

```bash
npm ci
npm run check
npm test
```

должны завершаться успешно.

Повторный `npm ci` не должен менять lockfile.

---

# 19. BUG-015 — восстановить реальный test remote inflight deduplication

## 19.1. Проблема

Тест с названием по смыслу `ten_consumers_share_one_inflight_request` создаёт broker/backend, но не запускает 10 consumers и не содержит необходимых assertions.

## 19.2. Требуемый тест

Для remote backend:

1. Backend increment `call_count`.
2. Первый вызов сигнализирует `started` и ждёт `release` Event.
3. Создать 10 concurrent tasks одного exact request key.
4. Дождаться `started`.
5. Убедиться, что backend вызван ровно 1 раз до release.
6. Release backend.
7. `await gather(10 tasks)`.
8. Все результаты successful/equivalent.
9. Только один execution является реальным remote execution; остальные получают shared inflight result в соответствии с текущей `served_from` semantics.
10. Follow-up после completion должен брать cache, если TTL позволяет.

Сделать аналогичные assertions согласованными с уже существующим local inflight test.

---

# 20. Дополнительное требование: cache/index lifecycle в analyzers

Даже после stable slot identity analyzer caches должны иметь явную lifecycle policy.

## 20.1. Нельзя оставлять dangling index entries

Если quote удалён из primary cache, его key должен быть удалён из:

- provider index;
- base index;
- triangle index;
- любых dirty/pending sets.

Создать helper methods вида:

```python
_remove_quote_key(key)
_purge_provider(provider)
_purge_source_epoch(source, old_epoch)
_prune_expired_quotes(now_monotonic_ns)
```

вместо ручного удаления из нескольких dict/set в разных местах.

## 20.2. Expired cache pruning

Даже stable slot должен физически удаляться после разумного internal TTL, если source больше не существует/не обновляется.

Freshness rejection без удаления не должна быть единственной lifecycle policy.

## 20.3. Assertions в tests

После purge primary cache и все secondary indexes имеют одинаковую cardinality; нет key, который существует только в set index.

---


# 21. BUG-016 — Node stdout должен иметь настоящий backpressure и bounded latest-state coalescing

## 21.1. Проблема

Сейчас protocol path концептуально эквивалентен:

```ts
process.stdout.write(`${JSON.stringify(message)}\n`);
```

Return value `Writable.write()` игнорируется. При `false` Node сообщает, что внутренний buffer превысил high-water mark и producer должен остановить запись до события `drain`. Если продолжать сериализовать и писать high-frequency `pool_state`, память может накапливаться до Python consumer и до bounded Python event bus.

Это особенно опасно при:

- Meteora bin-array update storm;
- Orca tick-array update storm;
- periodic refresh всех pools;
- одновременных quote/simulation responses;
- медленном Python reader или временной остановке pipe consumer.

Нельзя считать Python queue достаточной защитой: backlog возникает **раньше**, внутри Node writable/JS heap.

## 21.2. Требуемая архитектура

В `protocol.ts` или отдельном модуле создать единый `ProtocolEmitter`/`WorkerOutputWriter`. Никакие engine modules не должны напрямую вызывать `process.stdout.write`.

Разделить outbound сообщения на два класса.

### A. Lossless/control messages

Примеры:

- worker ready/hello;
- request result (`quote_result`, simulation/snapshot result);
- request-scoped error;
- shutdown acknowledgement;
- критические protocol errors.

Для них нельзя silently drop/coalesce по pool id.

### B. Replaceable state messages

Примеры:

- `pool_state`;
- компактный pool-dirty/state-changed signal;
- периодические status snapshots, если они полностью заменяемы новым status.

Они должны иметь стабильный `coalesceKey`, например:

```text
pool_state:<protocol>:<pool_id>
```

В памяти хранится максимум **одно pending latest message на key**.

## 21.3. Writer state machine

Минимальная семантика:

```text
losslessQueue: bounded deque
statePending: Map<coalesceKey, Message>
blocked: bool
flushing: bool
```

`emitLossless(message)`:

1. Если writer свободен и очереди пусты — попробовать write сразу.
2. Если `write()` вернул `false`, поставить `blocked=true` и дождаться `drain`.
3. Если blocked/flushing — положить в bounded lossless queue.
4. Lossless queue не может расти бесконечно. Установить hard maximum, например configurable `WORKER_MAX_LOSSLESS_OUTPUT_QUEUE` с безопасным default `1024`.
5. Если hard limit превышен, это protocol-fatal condition: записать diagnostic в stderr и завершить worker non-zero, чтобы supervisor перезапустил процесс. **Не продолжать unlimited buffering.**

`emitState(key, message)`:

1. Никогда не создавать более одной pending entry на key.
2. Новое сообщение заменяет старое pending сообщение этого key.
3. Увеличить `state_messages_coalesced_total`, если значение было заменено.
4. JSON stringify выполнять максимально поздно, непосредственно перед write, чтобы не хранить большие serialized strings для уже superseded state.

`flush()`:

- не должен выполняться конкурентно сам с собой;
- приоритет: lossless/control → затем pending state;
- при `write()==false` немедленно прекратить flush до `drain`;
- после `drain` продолжить с оставшимися latest values;
- shutdown должен либо корректно drain'ить lossless очередь с timeout, либо завершаться явной ошибкой, но не висеть бесконечно.

## 21.4. Fairness

Нельзя сделать так, чтобы постоянный поток lossless messages полностью навсегда starve'ил state messages. Допустима схема quota, например после N lossless writes (например 32) разрешать один state write, если writer не blocked. Главное — bounded memory и сохранение control correctness.

## 21.5. Все call sites перевести на единый emitter

Обязательная проверка grep'ом:

```bash
rg 'process\.stdout\.write|console\.log' workers/solana-quote-worker/src
```

В stdout не должно оставаться произвольных логов, которые ломают JSON-line protocol или обходят backpressure-aware writer. Диагностические human-readable logs отправлять в `stderr`.

## 21.6. Метрики

Добавить как минимум:

- `stdout_blocked_total`;
- `stdout_drain_total`;
- `stdout_lossless_queue_size`;
- `stdout_lossless_queue_high_watermark`;
- `stdout_state_pending_keys`;
- `stdout_state_coalesced_total`;
- `stdout_write_failures_total`.

## 21.7. Tests

Создать fake `Writable`, у которого:

1. `write()` начинает возвращать `false` после N writes.
2. `drain` испускается только вручную из test.
3. До `drain` writer не делает новых raw writes.
4. 10 000 `emitState()` одного pool key занимают одну pending state entry.
5. 10 000 updates по 20 pool keys занимают максимум 20 pending entries.
6. Последнее доставленное state для каждого key соответствует последнему input.
7. Lossless request results сохраняют порядок и не теряются.
8. При превышении hard lossless limit worker/emitter уходит в явную fatal path, а не продолжает наращивать память.

## 21.8. Acceptance

- При искусственно медленном Python reader Node RSS не растёт пропорционально числу `pool_state` updates.
- `stdout_state_pending_keys <= number_of_active_pool_keys`.
- Нет direct stdout writes вне writer.

---

# 22. BUG-017 — заменить Raydium CLMM `updateChain` на latest-only bounded mailbox

## 22.1. Проблема

Per-pool update path использует последовательную Promise chain вида:

```ts
pool.updateChain = previous
  .catch(...)
  .then(async () => {
      // decode/update/possibly RPC tick refresh
  });
```

Если обработка одного update медленнее входящего WebSocket потока, каждый следующий update добавляет Promise + closure + captured account/context. Backlog не имеет hard bound и может расти бесконечно.

Для market state это неправильная semantics: если updates 101..150 ещё не обработаны, обычно нет смысла обязательно вычислять все промежуточные состояния; нужен последний валидный update.

## 22.2. Требуемая модель

Для каждого pool хранить:

```ts
processingCoreUpdate: boolean
pendingCoreUpdate: CoreUpdate | null
coreUpdatesCoalescedTotal: number
```

Callback WebSocket:

```text
if !processing:
    pending = update
    start processing loop
else:
    pending = latest update   # replace previous pending
```

Processing loop:

```text
while pending != null:
    current = pending
    pending = null
    process(current)
```

Таким образом на pool одновременно существуют максимум:

- 1 update in processing;
- 1 latest pending update.

## 22.3. Slot rules

Перед тяжёлой обработкой:

- update со slot меньше уже принятого relevant core slot можно discard как stale;
- одинаковый slot должен иметь deterministic duplicate policy;
- pending update с большим slot заменяет меньший;
- update с меньшим slot не должен вытеснять pending update с большим slot.

## 22.4. Error handling

Исключение при обработке current update:

- увеличивает error metric;
- не оставляет `processing=true` навсегда;
- не теряет уже pending latest update;
- после ошибки loop продолжает с latest pending, если engine всё ещё active.

## 22.5. Не удерживать большие устаревшие Buffer objects

В pending mailbox должен оставаться только последний `AccountInfo`/Buffer. Старое pending значение при replacement должно становиться недостижимым для GC.

## 22.6. Tests

Искусственно заблокировать первую обработку Promise'ом и отправить 10 000 core updates.

Проверить:

- реально обработан первый update и затем последний/небольшое bounded число updates;
- pending cardinality никогда не превышает 1;
- итоговый pool state соответствует максимальному slot;
- memory structure не содержит array/list из 10 000 updates;
- exception в первом update не блокирует обработку latest pending.

---

# 23. BUG-018 — заменить unbounded Promise-tail RPC pacer на bounded scheduler

## 23.1. Проблема

Текущий pacer сериализует jobs через растущую Promise chain (`tail = tail.then(...)`). Если producers enqueue быстрее, чем разрешённый request start rate, все jobs остаются в памяти без ограничения.

При `minimumIntervalMs=200` throughput около 5 starts/sec. Несколько protocol engines, periodic refresh и hot quote requests легко могут enqueue быстрее этого значения.

## 23.2. Требуемый scheduler

Реализовать явную bounded queue, а не implicit Promise chain.

Рекомендуемый интерфейс:

```ts
type RpcPriority = 'interactive' | 'bootstrap' | 'refresh';

type RpcJobOptions = {
  priority: RpcPriority;
  deadlineAtMs?: number;
  coalesceKey?: string;
  description: string;
};

scheduleRpc<T>(options: RpcJobOptions, fn: () => Promise<T>): Promise<T>
```

## 23.3. Priority

Порядок:

1. `interactive` — quote/simulation/request needed by live evaluator.
2. `bootstrap` — initial pool/tick/bin loading required to make pool usable.
3. `refresh` — periodic maintenance.

Low-priority refresh не должен задерживать interactive quote на десятки секунд из-за старого maintenance backlog.

## 23.4. Boundedness

Добавить hard maximum pending jobs, configurable, например default `256`.

Но нельзя просто drop arbitrary jobs:

- refresh jobs должны иметь `coalesceKey` и заменяться latest-equivalent maintenance job;
- expired job с `deadlineAtMs` не запускается; caller получает typed deadline/queue-expired error;
- bootstrap jobs либо bounded/deduplicated по resource key, либо при overflow приводят к явной engine error;
- interactive jobs при переполнении не должны silently disappear: вернуть structured overload error.

## 23.5. Coalescing keys

Примеры:

```text
refresh:meteora:<poolId>
refresh:orca:<poolId>
refresh:raydium-clmm:<poolId>
load-ticks:<poolId>:<direction-or-range>
```

Одинаковые replaceable maintenance jobs не должны занимать N queue slots.

## 23.6. Rate semantics

Scheduler гарантирует minimum interval между **фактическими starts RPC calls**. Отсчёт должен использовать monotonic source (`performance.now()` или эквивалент), а не wall clock.

## 23.7. Metrics

- queue length total/by priority;
- high watermark;
- enqueued/completed/failed;
- coalesced refresh jobs;
- expired jobs;
- rejected-overload jobs;
- average/max queue wait;
- last request start timestamp.

## 23.8. Tests

- 10 000 одинаковых refresh jobs одного pool → queue остаётся O(1).
- 100 refresh + 1 interactive → interactive выполняется первым после текущего active slot.
- deadlines expire до execution и fn не вызывается.
- hard cap соблюдается.
- spacing request starts не меньше configured minimum interval с разумным test tolerance.
- scheduler продолжает работу после rejected RPC call.

---

# 24. BUG-019 — убрать full-state emission storm из Meteora/Orca dependency updates

## 24.1. Проблема

Meteora bin-array и Orca tick-array updates меняют local dependency cache, после чего немедленно формируют и публикуют полный `pool_state`. Это создаёт:

- повторный decode/allocate;
- создание больших JS objects;
- `JSON.stringify`;
- pipe traffic;
- Python JSON parse/allocation;
- лишние MarketEvents;
- потенциальные evaluator wakeups.

Raydium CLMM уже демонстрирует более здоровую модель: dependency/tick update может обновлять локальный cache без обязательной публикации полного pool event на каждый tick.

## 24.2. Разделить local cache update и external notification

Dependency update обязан:

1. обновить локальный bin/tick cache;
2. обновить dependency-specific freshness/slot metadata;
3. пометить pool `dirty`;
4. **не делать немедленный full `emitPoolState()` для каждого dependency account update.**

## 24.3. Coalesced notification

Ввести per-pool debounced/coalesced notification, например configurable `POOL_STATE_EMIT_MIN_INTERVAL_MS` default `100` ms.

Semantics:

- первое meaningful изменение может запланировать emit;
- сколько угодно последующих dependency updates внутри окна заменяются одним pending emit;
- trailing emit содержит **последнее** состояние pool metadata;
- на pool максимум один scheduled timer/pending notification;
- core account update при необходимости может попросить immediate/high-priority emit, но тоже должен проходить общий stdout coalescer.

Если Python на самом деле не использует full dependency details, предпочтительнее уменьшить payload до compact pool-change state, оставив тяжёлые tick/bin structures только в Node.

## 24.4. Periodic refresh не должен emit unchanged state

После refresh вычислять semantic fingerprint/versions relevant fields. Если ни core state, ни dependency generation/slot summary не изменились, не отправлять новый внешний `pool_state` только потому, что RPC request завершился.

## 24.5. CPU rule

Не делать глубокое JSON-представление всех tick/bin arrays для внешнего state event, если Python не использует их напрямую. Внешний message должен содержать только необходимые для routing/freshness/provenance поля.

## 24.6. Tests

- 1 000 dependency updates одного pool за 100 ms → не 1 000 external emits; число должно быть bounded debounce semantics (обычно 1–2).
- final emitted dependency slot/generation соответствует latest update.
- core state update не теряется.
- unchanged periodic refresh даёт 0 external state messages.
- timer очищается на unsubscribe/shutdown и не удерживает pool object после удаления.

---

# 25. BUG-020 — разделить core pool slot и dependency slots

## 25.1. Проблема

Нельзя использовать одно поле `pool.slot = max(all component slots)` как slot самого AMM state.

Пример:

```text
core pool account slot = 100
bin/tick dependency slot = 110
```

Если `pool.slot` становится 110, код начинает утверждать, что core pool account тоже относится к slot 110, что неверно. Более того, core refresh slot 105 может быть ошибочно отброшен как supposedly stale относительно 110.

## 25.2. Новая модель provenance

Минимум хранить отдельно:

```ts
coreStateSlot: number | null
coreReceivedAtMs: number | null

dependencySlotMin: number | null
dependencySlotMax: number | null
dependencyGeneration: number

dependencyReceivedAtMs: number | null
observedTipSlot: number | null   // optional max observed, НЕ core state slot
```

Если нужны per-account slots для evidence/simulation, хранить map/resource vector отдельно.

## 25.3. Правила

- core account update изменяет только `coreStateSlot` и core fields;
- tick/bin update **не изменяет `coreStateSlot`**;
- periodic core snapshot сравнивается с `coreStateSlot`, а не `dependencySlotMax`;
- dependency stale check сравнивается с corresponding dependency slots/generation;
- legacy `state_slot`, если нужен backward compatibility, должен иметь чётко задокументированную semantics. Предпочтительно сделать его alias `core_state_slot`, а не max всех компонентов.

## 25.4. Quote result provenance

Local quote/simulation result должен содержать как минимум:

```text
core_state_slot
dependency_slot_min
dependency_slot_max
dependency_generation
```

Если quote реально использовал конкретный dependency vector, сохранить этот vector/evidence hash там, где это уже поддерживается архитектурой.

## 25.5. Tests

Сценарий:

1. core slot 100;
2. dependency slot 110;
3. core refresh slot 105.

Ожидание:

- refresh 105 принимается;
- `core_state_slot == 105`;
- dependency max остаётся 110;
- quote provenance не сообщает core slot 110.

Дополнительно dependency update slot 120 не должен блокировать следующий core update 106.

---

# 26. BUG-021 — исправить regression `CexTopOfBookEvent` ↔ `SolanaRouteEvaluator`

## 26.1. Проблема

Memory optimization правильно перестал класть full-depth `BookSnapshot` в общий rolling store и публикует compact `CexTopOfBookEvent`.

Но `SolanaRouteEvaluator` продолжает ожидать `BookSnapshot` из store. В production integration он может получать `None`/`waiting_for_required_state`, тогда как unit test, вручную положивший старый тип `BookSnapshot`, маскирует регрессию.

Нельзя «исправлять» это возвращением тяжёлого full-depth book в общий generic history: это откат memory optimization.

## 26.2. Ввести explicit full-depth provider/resolver

Создать интерфейс уровня Python, например:

```python
@dataclass(frozen=True)
class CexDepthState:
    book: BookSnapshot
    source: str
    source_epoch: int
    event_id: int
    received_realtime_ns: int
    received_monotonic_ns: int

class CexDepthProvider(Protocol):
    def latest_depth(self, venue: str, symbol: str) -> CexDepthState | None: ...
```

Точное API можно адаптировать к существующим классам, но должны сохраняться depth **и provenance**.

`CexBookStateSource` должен быть владельцем bounded latest full-depth state и предоставлять resolver без копирования full book в generic `RollingStateStore`.

## 26.3. Route evaluator semantics

`SolanaRouteEvaluator`:

- compact BBO event может использовать только как trigger;
- непосредственно перед расчётом получает latest full-depth book через injected resolver;
- проверяет freshness по monotonic timestamp;
- проверяет current source epoch;
- не использует depth previous epoch;
- если full depth отсутствует/stale — выдаёт понятный `waiting_for_required_state`, а не пытается построить расчёт по BBO как будто это depth.

## 26.4. Epoch transition

При CEX epoch advance resolver обязан удалить/инвалидировать full-depth book предыдущей epoch до публикации/использования нового состояния.

## 26.5. Tests

Обязателен **integration test production wiring**, а не только вручную созданный store:

1. `CexBookStateSource` получает full `BookSnapshot`.
2. В generic store появляется `CexTopOfBookEvent`.
3. `SolanaRouteEvaluator` получает full depth через resolver.
4. Route calculation выполняется.
5. После epoch advance старый depth больше недоступен.
6. Новый BBO без нового full depth не должен случайно использовать старый book.

Добавить test, который упадёт, если evaluator снова начнёт делать `isinstance(store.value, BookSnapshot)`.

---

# 27. BUG-022 — привести `scanner.py` manifest/config и фактическое wiring local evaluator к одному состоянию

## 27.1. Проблема

Top-level scanner включает/показывает local route evaluator и `hot_local_routes`, но raw Solana builder документирован как создающий sources **без evaluator**. Это делает status/manifest вводящим в заблуждение и может тратить ресурсы на hot pool universe без фактического consumer.

## 27.2. Целевая semantics

Выбрать и реализовать один путь. Для текущей архитектуры и существующей конфигурации требование этого ТЗ: **если `local_route_evaluator.enabled == true` и есть hot local routes, evaluator должен быть реально подключён ровно один раз.**

Если проект сознательно больше не хочет использовать evaluator в unified scanner, тогда необходимо удалить автоматическое включение и все claims из manifest. Но агент не должен оставлять промежуточное состояние «enabled=true, но объекта нет».

## 27.3. Рекомендуемая реализация

Сделать build result явным, например:

```python
@dataclass
class SolanaRuntimeComponents:
    sources: list[MarketSource]
    route_evaluator: SolanaRouteEvaluator | None
    depth_provider: CexDepthProvider | None
    worker: ...
```

или эквивалентный dependency injection без hidden globals.

`build_unified_market_data_scanner()` обязан:

- получить components;
- добавить evaluator в analyzer/consumer lifecycle, если enabled;
- передать CEX full-depth resolver;
- гарантировать start/stop exactly once;
- не создавать второй evaluator в legacy path.

## 27.4. Fail-fast invariant

На startup:

```text
local_route_evaluator.enabled == true
AND hot_local_routes > 0
AND route_evaluator is None
```

должно приводить к явной configuration/runtime error до начала дорогих subscriptions.

## 27.5. Manifest/status

Добавить фактические поля:

```text
local_route_evaluator_requested
local_route_evaluator_attached
local_route_count
local_quote_worker_attached
```

Не утверждать «hot routes active», основываясь только на config.

## 27.6. Tests

- enabled + routes → evaluator created and registered exactly once;
- enabled + routes + cannot build evaluator → startup fails;
- disabled → evaluator absent и expensive evaluator-only subscriptions не создаются;
- manifest отражает фактическое состояние;
- shutdown не оставляет evaluator tasks.

---

# 28. BUG-023 — убрать bursty `refreshAllPoolStates()` и не обновлять свежие pools без необходимости

## 28.1. Проблема

Периодический refresh проходит по нескольким engines примерно раз в 15 секунд. Даже последовательный вызов способен создать CPU/RPC burst, а при unchanged state дополнительно генерировать лишние external events.

WebSocket уже поддерживает многие pools свежими, поэтому полный refresh всех pools каждые 15 секунд часто избыточен.

## 28.2. Сделать refresh stale-driven

Каждый pool/engine должен знать:

- last successful core WS update monotonic time;
- last dependency update time;
- last successful RPC refresh time;
- refresh in-flight flag/generation.

Periodic maintenance loop с небольшим tick (например 1 sec) только **выбирает stale pools**, а реальный RPC идёт через low-priority bounded scheduler из `BUG-018`.

Пример config:

```text
core_refresh_after_ms = 15_000
dependency_refresh_after_ms = protocol-specific
maintenance_scan_interval_ms = 1_000
```

Если core account получил свежий WS update 2 секунды назад, не нужно RPC refresh только потому, что прошёл глобальный wall-clock boundary.

## 28.3. Staggering

Чтобы после reconnect/bootstrap все pools не refresh'ились в одну миллисекунду, использовать deterministic staggering, например hash(pool_id) внутри небольшого window. Не использовать random behavior, который делает tests nondeterministic.

## 28.4. No duplicate in-flight refresh

Для одного `(protocol,pool,resource-kind)` не должно быть двух refresh jobs одновременно/pending. Использовать scheduler `coalesceKey`.

## 28.5. Emit only on semantic change

RPC refresh, который подтвердил тот же state/version/slots, обновляет health/freshness internally, но не обязан публиковать полный внешний `pool_state`.

## 28.6. Tests

- свежий WS pool не RPC-refresh'ится на каждом maintenance cycle;
- stale pool refresh'ится;
- 100 stale pools распределяются bounded scheduler/stagger, а не запускаются burst'ом;
- duplicate refresh одного pool coalesce'ится;
- unchanged refresh не создаёт external state event.

---

# 29. BUG-024 — добавить Node worker observability для доказательства bounded memory/CPU

## 29.1. Зачем

Без queue/memory metrics невозможно отличить:

- нормальный большой SDK baseline RSS;
- V8 heap leak;
- external Buffer growth;
- stdout backlog;
- RPC backlog;
- per-pool update backlog;
- periodic refresh burst.

## 29.2. Worker stats

Добавить compact periodic `worker_stats` не чаще чем раз в 5–10 секунд и/или status request. Он должен проходить backpressure-aware emitter и иметь стабильный coalescing key.

Поля минимум:

```text
rss_bytes
heap_total_bytes
heap_used_bytes
external_bytes
array_buffers_bytes
uptime_seconds

stdout_blocked
stdout_lossless_queue_size
stdout_state_pending_keys
stdout_state_coalesced_total

rpc_queue_total
rpc_queue_interactive
rpc_queue_bootstrap
rpc_queue_refresh
rpc_active
rpc_queue_high_watermark

pool_counts_by_protocol
refresh_inflight_by_protocol
coalesced_core_updates_by_protocol
external_pool_state_emits_total
```

Использовать `process.memoryUsage()`; не запускать forced GC в production.

## 29.3. Python status integration

Python worker wrapper должен хранить только latest stats под стабильным key. Не писать каждый stats sample в бесконечную историю.

Status должен явно показывать queue high-water marks и current sizes.

## 29.4. Alert-like diagnostics

Не требуется полноценный monitoring system, но если:

- lossless stdout queue > 75% hard cap;
- RPC queue > 75% hard cap;
- writer blocked непрерывно дольше configurable threshold;

записать rate-limited warning в stderr/status.

## 29.5. Tests

- stats message содержит обязательные поля;
- repeated stats заменяют previous latest state, не растят cardinality;
- metrics counters корректно меняются в synthetic backpressure/RPC queue tests.

---



# 30. Дополнительные сквозные требования после всех исправлений

## 30.1. Никаких hidden unbounded containers

Провести audit всех долгоживущих структур Python и Node:

```bash
rg 'dict\[|set\[|defaultdict|Map<|Set<|\[\]|Promise\.resolve\(\).*tail|\.then\(' src workers/solana-quote-worker/src
```

Это не означает, что каждый `dict`/`Map` обязан иметь limit, но для каждого контейнера, чей key space зависит от market data/request parameters, агент должен ответить:

- кто удаляет entry;
- какой максимум cardinality;
- что происходит после reconnect;
- что происходит после pool/route removal;
- есть ли stale/TTL sweep.

В итоговом PR отчёте перечислить audit results.

## 30.2. Timer/task lifecycle

Любой новый debounce/maintenance timer:

- отменяется на shutdown;
- не удерживает удалённый pool;
- не запускает callback после engine dispose;
- не создаёт второй concurrent flush/refresh loop.

Python asyncio tasks также должны быть awaited/cancelled deterministically.

## 30.3. Не возвращать full-depth/full-tick data в generic rolling history

Memory bug нельзя исправлять откатом оптимизаций:

- full CEX depth хранить только в bounded dedicated latest-depth provider;
- tick/bin arrays оставлять внутри Node engines;
- наружу отдавать compact provenance/state, если Python не требует массивы целиком.

## 30.4. Error visibility

Любой deliberate drop/coalesce/overload должен быть виден счётчиком. Silent drop запрещён.

---

# 31. Обязательная программа тестирования

## 31.1. Python unit tests

Покрыть все `BUG-001..015`, включая ранее описанные в соответствующих разделах tests.

Минимально:

- 100 000 dynamic notionals → bounded keys/cache/indexes;
- hot-key event storm → cold unique key не теряется;
- epoch advance → previous-epoch analyzer cache немедленно invalid;
- hidden reconnect → epoch/provenance boundary корректный;
- stable period reset backoff;
- rejected event не очищает health error;
- QuoteBroker key mismatch проходит failure accounting/cooldown;
- per-network-call pacing;
- monotonic candidate lifecycle;
- NaN/inf/non-integral config rejects;
- Decimal key canonicalization;
- Protocol type check;
- 10 remote consumers → 1 inflight backend request.

## 31.2. Python/Node integration tests

Обязательно создать тест реального composition path:

```text
CEX stream -> CexBookStateSource
           -> compact BBO generic event/store
           -> dedicated full-depth provider
           -> SolanaRouteEvaluator
           -> local Node quote worker
```

Проверить:

- evaluator реально зарегистрирован, если enabled;
- full-depth берётся не из generic store;
- epoch invalidation работает;
- route получает результат при наличии state;
- shutdown очищает processes/tasks.

## 31.3. Node synthetic stress tests

### Test N1 — stdout backpressure

- fake slow writable;
- не меньше 100 000 replaceable state updates;
- pending keys bounded;
- final latest values delivered;
- lossless results не потеряны.

### Test N2 — per-pool update storm

- заблокировать обработку первого Raydium CLMM update;
- отправить 100 000 updates;
- pending core update <= 1;
- final slot correct.

### Test N3 — RPC overload

- enqueue 100 000 coalescible refresh jobs;
- scheduler queue остаётся bounded;
- interactive request не starve'ится.

### Test N4 — Meteora/Orca dependency storm

- 100 000 dependency updates фиксированного числа pools;
- external event count ограничен debounce/coalescing policy;
- state latest/provenance correct.

### Test N5 — slot provenance

- core/dependency slots развести искусственно;
- core refresh меньший dependency max, но больший previous core slot принимается.

## 31.4. Soak test — обязательный gate

Запустить scanner/worker на фиксированном synthetic или controlled live universe минимум 30–60 минут; предпочтительно иметь отдельный synthetic soak, чтобы CI/локально было воспроизводимо.

Снимать каждые 5–10 секунд:

- Node RSS;
- heap used;
- external/arrayBuffers;
- Python RSS;
- stdout pending/queue sizes;
- RPC queue sizes;
- active pool count;
- event bus pending key count;
- analyzer cache cardinality;
- CPU %.

### Pass criteria

После warm-up (например первые 10 минут исключить из оценки):

1. При фиксированном universe cardinality bounded containers не имеет положительного тренда, связанного с числом market updates.
2. `stdout_state_pending_keys <= active state keys` всегда.
3. RPC queue не показывает монотонный рост; после burst возвращается к baseline.
4. Per-pool pending update максимум 1.
5. Node `heap_used` должен колебаться вслед за GC, а не демонстрировать устойчивый линейный рост. Допустимый noise задавать относительно baseline, а не требовать идеально плоскую линию.
6. RSS может вырасти на warm-up из-за V8/SDK caches, но после warm-up не должен продолжать расти пропорционально времени/числу updates.
7. Нет OOM, event-loop stall или watchdog restart.

В PR приложить CSV/JSON summary или текстовую таблицу минимум начала/warm-up-end/конца soak test.

---

# 32. Performance acceptance для текущего симптома «Node ест много RAM/CPU»

Нельзя считать задачу закрытой только потому, что unit tests прошли.

## 32.1. Memory

На fixed universe выполнить controlled load с одинаковым числом pools/routes.

Требование:

- рост total processed updates в 10x/100x не должен приводить к 10x/100x росту retained queue/cache cardinality;
- после прекращения artificial burst очереди возвращаются к baseline;
- forced slow stdout consumer не создаёт пропорционального росту числа state updates heap backlog.

## 32.2. CPU

Измерить CPU до/после для dependency storm.

Ожидаемый результат:

- JSON stringify/write count существенно меньше raw dependency update count;
- unchanged periodic refresh не создаёт массовых emissions;
- debounce/coalescing не ухудшает final state correctness.

Не задаётся искусственный абсолютный процент CPU, потому что он зависит от CPU/RPC/universe. Критерий — устранение avoidable per-update downstream churn и bounded work queues.

## 32.3. 15-second spikes

Если после изменения остаются periodic CPU spikes, профилировать их отдельно. Maintenance refresh должен быть stale-driven и staggered, поэтому синхронный «каждые 15 секунд обновить всё» паттерн должен исчезнуть.

---

# 33. Observability/status schema после изменений

Status должен различать как минимум:

### Python event plane

- received events;
- accepted state events;
- rejected stale/out-of-order events;
- coalesced pending replacements;
- pending unique keys;
- high watermark;
- retired state keys;
- current state key cardinality.

### Analyzer caches

- cycle quote cache entries;
- perp DEX quote cache entries;
- cache entries pruned/retired;
- stale epoch rejections.

### CEX depth provider

- number of latest depth books;
- books invalidated on epoch;
- current epochs.

### Node

Поля из `BUG-024`.

Status strings не должны утверждать `receives every update`, если semantics — latest-state coalescing.

---

# 34. Backward compatibility

## Сохранить по возможности

- существующие CLI entrypoints;
- persisted candidate/evidence formats;
- экономические формулы и thresholds;
- public provider names;
- read-only nature проекта.

## Допустимые расширения

- новые optional provenance fields;
- новые worker stats/status fields;
- новый compact pool state protocol version;
- новые config knobs с безопасными defaults;
- новые typed overload/deadline errors.

## Если меняется worker protocol

Добавить protocol version/capability negotiation либо одновременно обновить producer/consumer и fail-fast при несовместимой версии. Нельзя silently парсить старый message shape как новый.

---

# 35. Рекомендуемая последовательность реализации

## Phase 0 — baseline

1. Снять текущие test results.
2. Снять 5–10 минут runtime metrics Node/Python на текущей версии.
3. Зафиксировать active pool/route counts.
4. Не использовать эти цифры как acceptance сами по себе; это baseline для сравнения.

## Phase 1 — bounded identity/state в Python

- BUG-001
- BUG-012
- analyzer index lifecycle
- BUG-005

После этого dynamic market prices не должны менять state key cardinality.

## Phase 2 — event plane / health

- BUG-002
- BUG-007
- observability Python bus/state.

## Phase 3 — epoch/reconnect/full-depth integration

- BUG-003
- BUG-004
- BUG-021
- BUG-022

Эта фаза должна завершиться реальным integration test composition root.

## Phase 4 — Node bounded queues

В таком порядке:

1. BUG-016 stdout writer;
2. BUG-018 bounded RPC scheduler;
3. BUG-017 CLMM mailbox;
4. BUG-019 dependency emission coalescing;
5. BUG-020 slot provenance;
6. BUG-023 stale-driven refresh;
7. BUG-024 stats.

Не подключать/нагружать новый evaluator до завершения bounded queue fixes.

## Phase 5 — remaining Python correctness

- BUG-006
- BUG-008
- BUG-009
- BUG-010
- BUG-011
- BUG-013
- BUG-015

## Phase 6 — runtime reproducibility

- BUG-014;
- fresh install Node dependencies;
- exact Node version documented/tested.

## Phase 7 — stress/soak

Полный раздел 31–32.

---

# 36. Разделение между coding agents

Если работа делается параллельно, использовать следующее разделение, чтобы уменьшить merge conflicts.

## Agent A — Stable identity / analyzer caches

Ответственность:

- BUG-001;
- BUG-012;
- analyzer cache/index pruning;
- часть BUG-005, относящаяся к dynamic keys.

Не трогать Node.

## Agent B — Python event bus/state/health/backoff

- BUG-002;
- BUG-005 global retention;
- BUG-006;
- BUG-007;
- Python status metrics.

## Agent C — Epoch/CEX/evaluator integration

- BUG-003;
- BUG-004;
- BUG-021;
- BUG-022.

Обязан добавить production-wiring integration test.

## Agent D — QuoteBroker/provider/config

- BUG-008;
- BUG-009;
- BUG-010;
- BUG-011;
- BUG-013;
- BUG-015.

## Agent E — Node output/scheduler

- BUG-016;
- BUG-018;
- worker output/RPC metrics foundation.

## Agent F — Solana engine processing/provenance

- BUG-017;
- BUG-019;
- BUG-020;
- BUG-023.

Должен использовать emitter/scheduler Agent E, а не создавать отдельные очереди.

## Agent G — Reproducibility + soak gate

- BUG-014;
- BUG-024 final integration;
- test commands;
- soak harness/report.

## Merge order

Рекомендуется:

```text
A -> B -> E -> F -> C -> D -> G
```

Причина: evaluator integration должно опираться уже на bounded worker path; engine changes должны опираться на новый emitter/RPC scheduler.

---

# 37. Запрещённые «исправления»

Нельзя:

1. Просто увеличить queue size / V8 heap (`--max-old-space-size`) и назвать memory bug исправленным.
2. Убрать Node stats/логирование, не устранив backlog.
3. Возвращать full `BookSnapshot` во все generic MarketEvents ради evaluator.
4. Drop'ать arbitrary lossless quote results при stdout pressure.
5. Drop'ать arbitrary unique event key в Python bus из-за hot key.
6. Оставлять unlimited Promise tail, добавив только warning.
7. Снижать число pools вручную как единственное решение performance bug.
8. Отключать periodic refresh полностью без freshness/recovery replacement.
9. Подменять core slot максимальным dependency slot.
10. Сбрасывать все analyzer caches на каждый обычный event вместо корректной epoch/key lifecycle.
11. Исправлять test так, чтобы он соответствовал багу, вместо исправления production path.
12. Удалять failing assertions ради green suite.
13. Добавлять sleeps как substitute для proper rate scheduler/backpressure.

---

# 38. Code quality requirements

- Новые shared concepts (`coalesce key`, `depth state`, `RPC job`, provenance) должны иметь typed/dataclass/interface representation, а не набор ad-hoc dict fields.
- В Node включить/сохранить строгую TypeScript проверку.
- В Python новые structures типизировать.
- Комментарии должны объяснять **инвариант**, а не переписывать строку кода.
- Не делать broad unrelated refactor в одном commit с bugfix.
- Каждый bugfix commit должен содержать regression test, который падает на старой реализации.

---

# 39. Definition of Done по каждому BUG

BUG считается закрытым только если одновременно:

1. Есть reproduction/regression test.
2. Production code исправлен.
3. Test проверяет именно реальный production type/path, а не искусственный старый shape.
4. Добавлены/обновлены metrics там, где bug связан с drop/coalesce/queue.
5. Нет нового unbounded container.
6. Shutdown/reconnect path протестирован, если изменение создаёт task/timer/queue.
7. Документация/config/status больше не противоречат behavior.

---

# 40. Полный Definition of Done проекта

Работа по этому ТЗ считается завершённой, когда:

- [ ] BUG-001 закрыт тестом 100k dynamic notionals и bounded cardinality.
- [ ] BUG-002 реализует keyed latest-state delivery без arbitrary FIFO loss.
- [ ] BUG-003 старый epoch не используется analyzer'ами.
- [ ] BUG-004 transport reconnect соответствует epoch semantics.
- [ ] BUG-005 state/history реально bounded и retirement работает.
- [ ] BUG-006 backoff reset после стабильной работы.
- [ ] BUG-007 health различает received/accepted/rejected.
- [ ] BUG-008 QuoteBroker mismatch проходит failure accounting.
- [ ] BUG-009 pacing работает на каждом фактическом network request.
- [ ] BUG-010 durations monotonic.
- [ ] BUG-011 invalid numeric configs reject.
- [ ] BUG-012 Decimal identity canonical.
- [ ] BUG-013 provider Protocol исправлен и проверяется type checker'ом.
- [ ] BUG-014 Node runtime/dependencies exact/reproducible.
- [ ] BUG-015 remote inflight dedup test реально запускает 10 consumers.
- [ ] BUG-016 stdout respects backpressure; state output coalesced/bounded.
- [ ] BUG-017 CLMM core update pending <= 1 per pool.
- [ ] BUG-018 RPC scheduler bounded, prioritized, deadline-aware.
- [ ] BUG-019 Meteora/Orca dependency storm не превращается в full-state emission storm.
- [ ] BUG-020 core/dependency slot provenance разделена.
- [ ] BUG-021 evaluator работает с production compact-BBO + dedicated-depth architecture.
- [ ] BUG-022 scanner manifest соответствует фактически подключённому evaluator.
- [ ] BUG-023 refresh stale-driven/staggered и не emit unchanged state.
- [ ] BUG-024 worker memory/queue metrics доступны в status.
- [ ] Python test suite green.
- [ ] Node build/typecheck/tests green.
- [ ] integration tests green.
- [ ] synthetic stress tests green.
- [ ] 30–60 minute soak passes boundedness criteria.
- [ ] В PR есть before/after runtime summary.

---

# 41. Что должно получиться архитектурно

После выполнения:

```text
                    ┌──────────────────────┐
CEX WS ────────────►│ CexBookStateSource   │
                    └───────┬──────────────┘
                            │
              compact BBO   │        bounded latest full depth
                            │               │
                            ▼               ▼
                   keyed event bus    CexDepthProvider
                            │               │
                            ▼               │
                 analyzers/evaluator ◄──────┘
                            │
                            │ local quote request
                            ▼
                 ┌─────────────────────┐
                 │ Node Solana worker  │
                 │                     │
Solana WS ──────►│ per-pool latest     │
                 │ mailboxes           │
                 │        │            │
                 │ bounded RPC queue   │
                 │        │            │
                 │ local SDK state     │
                 │        │            │
                 │ coalesced state     │
                 │ output              │
                 │        │            │
                 │ backpressure writer │
                 └────────┬────────────┘
                          │ JSONL
                          ▼
                       Python
```

Ключевое свойство: **каждая стрелка, способная принимать данные быстрее, чем следующий компонент их обрабатывает, имеет явно описанную bounded/backpressure/coalescing semantics.** Никаких скрытых бесконечных очередей.

---

# 42. Итоговый отчёт coding agents

В итоговом PR/отчёте обязательно указать:

1. Какие BUG IDs исправлены.
2. Какие файлы изменены по каждому BUG.
3. Кратко — root cause.
4. Какая новая invariant защищает от повторения.
5. Названия regression tests.
6. Результат Python tests.
7. Результат Node build/typecheck/tests.
8. Результат integration tests.
9. Результат synthetic stress tests.
10. Soak test duration и before/after memory/CPU/queue summary.
11. Максимальные observed queue sizes/high-water marks.
12. Были ли найдены дополнительные баги; если да — отдельными BUG IDs, а не скрытыми unrelated changes.

---

# 43. Примечание агентам

Не считать это ТЗ списком локальных текстовых замен. Основная проблема проекта сейчас находится на границах между lifecycle/state/event/worker components. Исправление считается корректным только если сохраняет read-only исследовательскую архитектуру и одновременно делает поток данных bounded, provenance-строгим и наблюдаемым.

Если фактический текущий `main` отличается от названий в этом документе, адаптировать имя символа можно, но **нельзя ослаблять описанный инвариант или acceptance criterion без отдельного обоснования в PR**.
