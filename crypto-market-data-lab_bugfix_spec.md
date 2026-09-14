# Техническое задание: исправление ошибок и укрепление runtime-инвариантов `crypto-market-data-lab`

**Репозиторий:** `https://github.com/xX-mabster-Xx/crypto-market-data-lab`  
**Целевая ветка для анализа:** `main`  
**Тип работ:** bugfix / reliability hardening / regression testing  
**Основной принцип:** сохранить read-only исследовательскую архитектуру проекта и существующую экономическую модель, исправив ошибки жизненного цикла состояния, event delivery, reconnect/epoch semantics, boundedness, pacing, validation и тестового покрытия.

---

## 1. Цель работы

Необходимо устранить обнаруженные ошибки, из-за которых долгоживущий live scanner может:

- неограниченно накапливать state/cache из-за динамически меняющихся quote notionals;
- терять значимые DEX/perp market updates при перегрузке event bus;
- использовать данные предыдущей `source_epoch` после reconnect;
- не повышать `source_epoch` при внутренних CEX WebSocket reconnect;
- постепенно доходить до максимального reconnect backoff даже при длительных периодах нормальной работы;
- показывать успешный health по событию, которое state layer фактически отверг;
- обходить failure accounting/cooldown в `QuoteBroker` при backend key mismatch;
- нарушать фактический API request rate limit, если один `quote_round()` выполняет несколько запросов;
- хранить inactive state keys дольше заявленного retention;
- некорректно измерять TTL/duration через wall clock;
- принимать некорректные config values (`NaN`, `inf`, дробные значения для integer-полей);
- иметь рассинхрон между Node engine и реальными требованиями зависимостей;
- пропускать часть интерфейсных ошибок из-за неверного `Protocol`;
- считать важный сценарий покрытым тестом, хотя тест фактически пустой.

Результатом должен быть scanner, для которого выполняются следующие системные инварианты:

1. **Кардинальность состояния ограничена.** Изменение рыночной цены не порождает бесконечное число identity keys.
2. **Latest-state delivery корректен.** Частые обновления одного и того же ключа можно coalesce, но обновление другого ключа нельзя молча потерять из-за активности hot key.
3. **Epoch isolation строгая.** После перехода источника в новую epoch данные предыдущей epoch ни при каких условиях не участвуют в новых расчётах.
4. **Reconnect имеет одного владельца либо явную epoch propagation.** Скрытый транспортный reconnect не может происходить без инвалидации старого состояния.
5. **Длительности измеряются monotonic clock.** Wall clock используется только для человекочитаемых/персистентных timestamps.
6. **Health отражает принятое состояние, а не просто пришедший пакет.**
7. **Все outbound requests ограничиваются pacing на уровне реального запроса, а не логического round.**
8. **Любой failure path проходит единый accounting/gating/cooldown pipeline.**
9. **Retention и capacity действительно ограничивают память.**
10. **Исправления защищены детерминированными regression tests.**

---

## 2. Границы задачи

### 2.1. Что входит в задачу

Исправить код и тесты как минимум в следующих областях:

- `src/market_data_lab/realtime_scanner.py`
- `src/market_data_lab/versioned_market_state.py`
- `src/market_data_lab/polling_quote_sources.py`
- `src/market_data_lab/unified_market_data.py`
- `src/market_data_lab/unified_cycle_analyzer.py`
- `src/market_data_lab/unified_perp_analyzer.py`
- `src/market_data_lab/cex_book_streams.py`
- `src/market_data_lab/solana_realtime_scanner.py`
- `src/market_data_lab/quote_broker.py`
- `src/market_data_lab/dex_quotes.py`
- `workers/solana-quote-worker/package.json`
- `workers/solana-quote-worker/package-lock.json`
- соответствующие unit/integration tests в `tests/`
- документацию/status schema, если меняется наблюдаемое поведение.

Имена и расположение символов на момент реализации необходимо проверить по текущему `main`: номера строк не являются частью контракта.

### 2.2. Что не входит в задачу без отдельного доказанного дефекта

Не следует одновременно с этим ТЗ:

- добавлять реальное исполнение сделок;
- подключать wallet signing / transaction submission;
- менять экономические формулы PnL, fee model или admission thresholds только ради рефакторинга;
- превращать read-only candidate scanner в execution engine;
- делать крупный unrelated redesign UI/CLI;
- менять публичные форматы persisted candidate/evidence без необходимости;
- оптимизировать код «на глаз», если это не связано с указанными инвариантами.

Если в процессе тестирования обнаружится отдельная математическая ошибка, её следует оформить отдельным bugfix commit с тестом, который сначала воспроизводит дефект.

---

# 3. Приоритеты

## P0 — обязательно до следующего длительного live-run

1. `BUG-001` — dynamic-notional создаёт неограниченное количество state/analyzer keys.
2. `BUG-002` — event bus не выполняет настоящий keyed coalescing и может потерять уникальный update.
3. `BUG-003` — analyzer cache переживает `source_epoch` и может использовать старый quote.
4. `BUG-004` — внутренний CEX reconnect не соответствует epoch semantics.
5. `BUG-005` — state retention/capacity не гарантируют bounded memory.

## P1 — исправить в той же серии изменений

6. `BUG-006` — retry backoff не сбрасывается после стабильной работы.
7. `BUG-007` — health обновляется до решения state layer принять событие.
8. `BUG-008` — `QuoteBroker` key mismatch обходит `observe_result`/failure accounting.
9. `BUG-009` — request pacer применяется к `quote_round`, а не каждому outbound request.
10. `BUG-010` — lifecycle TTL/persistence измеряются wall clock.

## P2 — correctness / reproducibility / hygiene

11. `BUG-011` — config validation принимает `NaN`/`inf` и дробные int.
12. `BUG-012` — decimal canonicalization несовместима между producer/mapping lookup.
13. `BUG-013` — `DexQuoteProvider.config()` ошибочно расположен после `return`.
14. `BUG-014` — Node engine/dependency versions рассинхронизированы.
15. `BUG-015` — пустой remote inflight-dedup test.

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

# 21. Source health и status schema

После изменений status должен позволять понять, теряет ли pipeline данные и почему.

Рекомендуемые поля:

```text
received_events
accepted_events
rejected_state_events
coalesced_updates
pending_keys
queue_capacity
queue_high_watermark
publisher_backpressure_waits
state_key_count
state_key_retirements
state_key_capacity_evictions
source_epoch
source_restarts
last_accepted_event_age_ms
last_error
```

Не обязательно использовать именно эти названия, но observability должна покрывать эти факты.

Если меняется JSON schema:

- увеличить соответствующий `schema_version`;
- обновить tests;
- обновить docs;
- не оставлять consumer с молча изменившимся meaning старого поля.

---

# 22. Backward compatibility и persisted data

## 22.1. Что желательно сохранить

- существующие candidate/evidence fields;
- realtime timestamps;
- actual requested notionals;
- current PnL result fields;
- read-only execution safety fields (`execution_ready=False` там, где это было намеренно).

## 22.2. Что допустимо добавить

- `quote_slot_id`;
- source/epoch provenance для internal/persisted diagnostics;
- close reason `source_epoch_advanced`;
- новые bus/store metrics;
- status schema version.

## 22.3. Что нельзя делать

Нельзя ради совместимости продолжать принимать старую ошибочную identity model внутри live process. Если есть replay старых event JSON, сделать явный compatibility adapter на ingestion boundary, а не размножать legacy semantics по новому коду.

---

# 23. Требования к тестированию

## 23.1. Unit tests

Обязательно покрыть каждый `BUG-xxx` отдельным regression test, который до исправления падает либо демонстрирует неверную cardinality/metric.

## 23.2. Deterministic stress test

Добавить offline stress test без сети:

- один dynamic triangle slot;
- 100 000 synthetic amount changes;
- mixed CEX hot updates;
- периодические epoch changes;
- медленный event consumer;
- периодические state sweeps.

После run проверить:

- state key count ограничен ожидаемым числом logical slots + diagnostics;
- analyzer cache cardinality ограничена;
- secondary indexes не растут;
- нет старой epoch в active cache;
- cold-key event не потерян;
- bus coalescing counter > 0;
- arbitrary drop counter = 0;
- no task leaks after shutdown.

Не утверждать strict byte-level RAM limit в unit test: это нестабильно. Проверять cardinality и при желании `tracemalloc` как дополнительный non-fragile smoke metric.

## 23.3. Shutdown tests

Поскольку появятся backpressure/Condition waits:

- scanner stop должен разбудить blocked publishers/consumers;
- `await scanner.stop()` не зависает;
- все source/receiver tasks завершаются;
- никаких `Task was destroyed but it is pending!`.

## 23.4. Suggested Python commands

Сначала определить фактический test runner проекта. Для текущего unittest-style suite ожидается как минимум:

```bash
python -m compileall src tests
python -m unittest discover -s tests -p 'test_*.py'
```

Если проект официально использует `pytest`, запустить также canonical project command.

Запустить любые существующие lint/type-check commands, указанные в `pyproject.toml`/README/CI.

## 23.5. Node worker

На Node 24:

```bash
cd workers/solana-quote-worker
npm ci
npm run check
npm test
```

Если `npm test` отсутствует, не придумывать fake success: добавить/использовать существующий worker validation command и документировать фактически выполненные команды.

---

# 24. Рекомендуемая последовательность работ

Чтобы агенты не создавали конфликтующие частичные решения, выполнять в таком порядке.

## Phase 0 — baseline

1. Checkout current `main`.
2. Зафиксировать current test results.
3. Не обновлять dependencies до functional changes.
4. Создать regression tests для известных bugs по возможности **до** implementation.

## Phase 1 — stable quote identity

Включает:

- canonical Decimal helper;
- `quote_slot_id`;
- stable event keys;
- replacement semantics analyzer caches;
- cache/index pruning helpers;
- cardinality tests.

После Phase 1 dynamic notional больше не должен создавать новые logical state keys.

## Phase 2 — bounded state + real keyed bus + health

Включает:

- true keyed coalescing;
- backpressure;
- `PublishResult`;
- health-after-admission;
- global state sweep;
- hard `max_state_keys`;
- `VersionedMarketState.retire()`;
- shutdown tests.

## Phase 3 — epoch propagation + CEX reconnect ownership

Включает:

- `SourceEpochChange` control path;
- analyzer purge;
- provenance metadata в perp legs;
- single reconnect owner;
- sharded failure semantics;
- old-epoch regression tests.

Не делать Phase 3 до того, как определён окончательный bus/source control API Phase 2.

## Phase 4 — timers/backoff

- stable-run backoff reset;
- monotonic lifecycle timers;
- fake-clock tests.

## Phase 5 — QuoteBroker + API pacing

- mismatched key failure accounting;
- полноценный T25 inflight test;
- per-network-request pacing;
- shared quota tests.

## Phase 6 — config/type/dependency cleanup

- strict numeric validation;
- `DexQuoteProvider` Protocol;
- Node 24;
- exact dependency pins;
- docs/CI consistency.

## Phase 7 — full regression / stress

- весь Python suite;
- Node suite;
- deterministic stress;
- clean shutdown;
- final status schema check.

---

# 25. Разделение работы между несколькими агентами

Если работу выполняют параллельно несколько coding agents, использовать следующий dependency-aware split.

## Agent A — Quote identity / analyzer cache lifecycle

Ответственность:

- `numeric_text.py`;
- `polling_quote_sources.py`;
- dynamic suppliers в `unified_market_data.py`;
- stable keys в `UnifiedCycleAnalyzer`;
- stable keys в `UnifiedPerpAnalyzer`;
- cardinality/pruning tests.

Не реализовывать самостоятельно epoch control API — оставить небольшой integration seam для Agent C.

## Agent B — Realtime bus / state / health / backoff

Ответственность:

- `realtime_scanner.py`;
- `versioned_market_state.py`;
- keyed coalescing;
- `PublishResult`;
- bounded state/sweep;
- health admission semantics;
- backoff reset;
- shutdown/backpressure tests.

Должен первым стабилизировать public/internal control API, который понадобится Agent C.

## Agent C — Epoch / CEX reconnect / provenance

Зависит от Agent B.

Ответственность:

- `SourceEpochChange`;
- `cex_book_streams.py`;
- `CexBookStateSource`;
- sharded reconnect semantics;
- analyzer epoch callbacks/invalidation integration;
- provenance fields в cached perp/spot legs;
- epoch/reconnect tests.

Agent C должен согласовать изменения analyzer files с Agent A, чтобы не перетереть stable-key refactor.

## Agent D — QuoteBroker / pacing / provider Protocol

Ответственность:

- `quote_broker.py`;
- `dex_quotes.py`;
- provider construction paths;
- remote inflight test;
- per-request pacing tests;
- Protocol fix.

## Agent E — Config / Node / dependency reproducibility / final validation

Ответственность:

- strict numeric config parsers;
- worker `package.json`/lockfile;
- Node references в CI/docs;
- full suite orchestration;
- documentation/status schema review.

## Merge order

Рекомендуемый merge order:

```text
A -> B -> C -> D -> E
```

A и B могут разрабатываться параллельно, если не меняют одни и те же constructor signatures без coordination. C должен ребейзиться на A+B. D в основном независим. E выполняется последним.

---

# 26. Детальные acceptance criteria всего проекта

Работа считается завершённой только если одновременно выполняется всё ниже.

## Memory/cardinality

- Dynamic reference quote не создаёт новый state key при каждом изменении CEX price.
- Analyzer caches не растут с числом rounds при фиксированном наборе logical slots.
- State store имеет TTL retirement и hard key capacity.
- Secondary indexes физически очищаются.

## Event delivery

- Hot key updates coalesce.
- Unique cold key не удаляется произвольным FIFO drop.
- При saturation применяется bounded backpressure.
- Shutdown при blocked publisher корректен.

## Epoch correctness

- Новый source epoch становится известен store и analyzers до первого event новой epoch.
- Старый cached quote/leg не участвует после transition.
- Delayed old-epoch event игнорируется.
- Любой CEX transport reconnect приводит к новой logical source epoch.
- Orderbook delta state не переносится через connection boundary без fresh snapshot semantics.

## Timing

- TTL, idle duration, minimum persistence, throttling и backoff duration основаны на monotonic clock.
- Persisted timestamps остаются realtime.
- Stable source run сбрасывает exponential failure streak.

## Health

- Rejected event не может сделать source healthy.
- Status показывает accepted/rejected/coalesced/backpressure metrics.

## QuoteBroker

- Backend key mismatch проходит standard failure accounting.
- 10 identical concurrent remote requests дают один backend execution.
- Cache/inflight semantics остаются корректны.

## Pacing

- Каждый фактический outbound request проходит pacer.
- Нет double pacing.
- Shared quota domain действительно shared.

## Validation/reproducibility

- `NaN`, infinities и fractional ints отвергаются там, где они недопустимы.
- Protocol соответствует реальному provider interface.
- Worker официально использует Node 24.
- `latest` отсутствует у top-level worker dependencies, нужных для детерминированного runtime.
- `npm ci` воспроизводим и не меняет lockfile.

## Regression safety

- Все существовавшие tests проходят либо изменение ожидания обосновано в commit/message.
- Все новые regression tests проходят.
- Deterministic stress test проходит.
- Нет live network dependency в обычных unit tests.
- Нет pending asyncio tasks после shutdown tests.

---

# 27. Что агент обязан написать в итоговом отчёте/PR

Для каждого `BUG-xxx`:

1. Какие файлы изменены.
2. Как воспроизводился баг до исправления.
3. Какой invariant теперь гарантируется.
4. Название regression test.
5. Какие команды проверки запускались.
6. Есть ли schema/backward-compatibility изменения.
7. Есть ли оставшиеся ограничения.

Дополнительно привести before/after metrics для synthetic stress test:

```text
state_key_count
cycle_quote_cache_count
perp_quote_cache_count
coalesced_updates
rejected_state_events
capacity_evictions
source_epoch_transitions
```

Не писать «fixed» без теста или конкретного доказательства.

---

# 28. Запрещённые упрощения

Следующие варианты **не считаются исправлением**:

### Для unbounded keys

- просто увеличить RAM/queue/cache limit;
- округлять dynamic notional до нескольких знаков и всё равно использовать его как identity;
- периодически `dict.clear()` без stable slot model.

### Для event bus

- увеличить `asyncio.Queue(maxsize=...)`;
- продолжить FIFO-drop, назвав его coalescing;
- сохранять latest только в store, если analyzer всё ещё зависит от потерянного event и не reread'ит store.

### Для epoch

- надеяться только на freshness timeout;
- оставить internal reconnect и не сообщать о нём state/analyzer layers;
- сравнивать только timestamps вместо epoch.

### Для memory

- очищать только primary dict, оставляя secondary indexes;
- считать `deque(maxlen=N)` достаточной защитой при неограниченном числе keys.

### Для pacing

- делать один wait на `quote_round`, если внутри несколько network requests.

### Для time

- использовать `time.time_ns()` для duration потому что «обычно NTP почти не прыгает».

### Для dependencies

- выполнить `npm update` и принять случайно новые SDK версии;
- оставить `latest`, рассчитывая только на старый lockfile.

---

# 29. Риски при реализации

## 29.1. Deadlock в новом event bus

Наиболее опасный implementation risk — ждать queue capacity, удерживая lock, который нужен consumer. Использовать `asyncio.Condition` корректно и добавить timeout/shutdown tests.

## 29.2. Starvation

Hot key не должен постоянно сдвигать себя в конец так, чтобы cold keys никогда не обрабатывались. Сохранять queue position при replacement pending payload — хороший default.

## 29.3. Epoch callback deadlock

Epoch handler не должен ожидать market event, который не может быть опубликован до завершения handler. Control-plane callback должен быть коротким: purge/invalidate/schedule recomputation, без cyclic waits.

## 29.4. Cache/index races

Analyzer callbacks и event handling могут выполняться конкурентно. Если analyzer state сейчас защищён одним task/event loop и serial execution, сохранить эту модель. Если epoch callback вводит параллельный writer, сериализовать через тот же lock/queue.

## 29.5. Sharded CEX restart storm

Whole-source restart при одном shard failure проще и корректнее, но может увеличить reconnect frequency. Backoff + stable-run reset должны применяться на уровне logical source; добавить metrics по failing shard.

## 29.6. Over-aggressive TTL sweep

Не удалять активно используемый state из-за сравнения realtime и monotonic часов. Все retention age comparisons должны использовать timestamps одной clock domain.

---

# 30. Definition of Done

Изменения считаются готовыми к merge, когда:

- [ ] `BUG-001` stable quote slot identity реализована.
- [ ] `BUG-002` event bus делает keyed coalescing без arbitrary distinct-key drop.
- [ ] `BUG-003` analyzers инвалидируют данные на source epoch change.
- [ ] `BUG-004` CEX reconnect всегда соответствует новой source epoch.
- [ ] `BUG-005` state key memory bounded TTL + hard capacity.
- [ ] `BUG-006` backoff reset после stable run.
- [ ] `BUG-007` health считает accepted state корректно.
- [ ] `BUG-008` QuoteBroker mismatch проходит failure accounting.
- [ ] `BUG-009` pacing выполняется на каждом outbound request.
- [ ] `BUG-010` lifecycle durations используют monotonic clock.
- [ ] `BUG-011` strict numeric config validation реализована.
- [ ] `BUG-012` Decimal canonicalization едина.
- [ ] `BUG-013` Provider Protocol исправлен.
- [ ] `BUG-014` Node/runtime dependency contract согласован и pinned.
- [ ] `BUG-015` remote inflight dedup реально тестируется.
- [ ] Secondary cache indexes очищаются без dangling entries.
- [ ] Status/docs соответствуют новой semantics.
- [ ] Python full test suite зелёный.
- [ ] Node 24 `npm ci` + check/test зелёные.
- [ ] Deterministic stress test зелёный.
- [ ] Clean shutdown test зелёный.
- [ ] Ни одно исправление не добавило order execution/signing capability.

---

# 31. Краткий целевой результат после выполнения ТЗ

После исправлений pipeline должен выглядеть концептуально так:

```text
transport session
      │
      │ failure => outer supervisor restart
      ▼
source_epoch N
      │
      ├──► Versioned/Rolling state invalidation
      │
      ├──► SourceEpochChange ──► analyzer cache purge
      │
      ▼
accepted MarketEvent
      │
      ├── stable logical state key
      │
      ▼
keyed latest-state coalescing bus
      │
      │ same key => replace pending latest
      │ different key + full => bounded backpressure
      ▼
analyzers
      │
      ├── bounded stable-slot caches
      ├── explicit source + epoch provenance
      ├── monotonic TTL/persistence
      └── no old-epoch evidence
```

Для dynamic DEX quote:

```text
logical slot: triangle-reference-usdt:100

round #1 actual input = 0.00152 BTC
round #2 actual input = 0.00149 BTC
round #3 actual input = 0.00157 BTC

state identity остается одним и тем же;
payload заменяется последним фактическим quote.
```

Именно это является конечной correctness-моделью данного ТЗ.

---

# 32. Примечание для coding agents

Перед внесением патча обязательно ещё раз открыть актуальный `main`, потому что отдельные символы могут сдвинуться после параллельных изменений. Опираться следует на **семантические инварианты этого ТЗ**, а не на номера строк.

Если обнаруженная текущая реализация уже частично исправляет какой-либо пункт, не вносить дублирующий механизм. Сначала добавить regression test на требуемый invariant; если test уже проходит по корректной причине, документировать это и переходить к следующему пункту.

Любое отклонение от рекомендуемой реализации допустимо только если оно обеспечивает те же или более строгие acceptance criteria и сохраняет boundedness, epoch isolation, deterministic behavior и read-only nature системы.
