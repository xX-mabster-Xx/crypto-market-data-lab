# Crypto market-data lab

Первый технический этап: воспроизводимый путь от публичных биржевых данных до
нормализованного каталога и replay. На этом этапе код не использует API-ключи и
не умеет отправлять ордера.

## Выбранный стек

- NautilusTrader — нормализованная модель данных, Parquet-каталог и replay.
- Bybit public order-book archives — бесплатная история L2 для первого PoC.
- Public WebSocket/REST Bybit и OKX уже подключены. CCXT останется
  вспомогательным адаптером для MEXC и широких сканеров, но не основным
  HFT-рекордером.

Версия `nautilus_trader==2.0.0rc3` закреплена намеренно. Это release candidate:
он подходит для data-only исследования, но до отдельного решения не должен
управлять реальным капиталом.

## Post-trade AMM simulation

Read-only shadow simulation of Solana AMM paths lives in
`README_AMM_SIMULATION.md`.  It remains disabled by default; the current supported
mathematical adapter is synthetic CPMM v1, while real protocol-specific adapters
are explicit and do not masquerade pre-trade SDK quotes as post-trade state.

## Первый PoC

Пример использует публичный архив Bybit:

```text
https://quote-saver.bycsi.com/orderbook/linear/SOLUSDT/2026-08-01_SOLUSDT_ob200.data.zip
```

После установки окружения:

```bash
import-bybit-archive \
  --archive data/raw/bybit/2026-08-01_SOLUSDT_ob200.data.zip \
  --catalog data/catalog/bybit-solusdt-2026-08-01-my-run \
  --limit 250000 \
  --source-url https://quote-saver.bycsi.com/orderbook/linear/SOLUSDT/2026-08-01_SOLUSDT_ob200.data.zip
```

Команда:

1. читает только целые события, не разрывая snapshot/delta batch;
2. выводит исторические precision и минимальные шаги из самого архива;
3. конвертирует события в `OrderBookDelta` NautilusTrader;
4. записывает instrument definition и deltas в Parquet-каталог;
5. читает каталог обратно, собирает L2-стакан и запускает integrity check;
6. сохраняет `manifest.json` с SHA-256 исходного ZIP и итоговой статистикой.

Каталог должен быть новым и пустым: утилита намеренно не перезаписывает
существующие данные.

## Что проверяем перед live-сбором

- число исходных и прочитанных из Parquet событий совпадает;
- timestamps и sequence монотонны там, где это гарантирует источник;
- после replay стакан не пересечён и проходит `OrderBook.check_integrity()`;
- в исходном потоке нет регрессий exchange timestamp и sequence;
- manifest позволяет воспроизвести импорт из того же исходного файла.

Учебный сигнал imbalance из upstream-примера здесь намеренно не запускается:
сам NautilusTrader прямо отмечает, что у него нет торгового преимущества.

## Live recorder Bybit

Recorder подключает только публичный market-data client и не регистрирует
execution client. API-ключи не нужны.

```bash
.venv/bin/record-bybit-live --duration-seconds 60
```

По умолчанию собираются `BTCUSDT`, `ETHUSDT` и `SOLUSDT`:

- L2 order-book deltas глубины 50;
- BBO quotes и public trades;
- mark price, index price и funding rate;
- open interest через публичный REST ticker каждые 5 секунд;
- оценка смещения часов по публичному `GET /v5/market/time`;
- socket/reconnect events, exchange sequence и update-id gaps;
- распределение `ts_init - ts_event` и максимальные интервалы без сообщений.

Поток сначала записывается штатным `StreamingFeatherWriter`, после корректной
остановки Feather-фрагменты объединяются по типу и инструменту, конвертируются
в Parquet и читаются обратно. Для небольших запусков проверяется точное
совпадение количества записей и повторно собирается L2-стакан. Служебный
snapshot `CLEAR` нормализуется к precision инструмента до записи — это обходит
ошибку metadata в NautilusTrader 2.0.0rc3. Каждый запуск получает отдельный
каталог под `data/live/bybit/<run-id>/`, поэтому существующие данные не
перезаписываются.

`ts_init - ts_event` включает рассинхронизацию часов, внутреннюю задержку биржи,
сетевой маршрут и обработку адаптером; даже после настройки NTP это не чистая
сетевая задержка.

Bybit recorder явно использует WebSocket backend `tungstenite`. В
текущей сборке NautilusTrader default backend `sockudo` может
длительно ждать Bybit WebSocket handshake, хотя REST endpoints
отвечают. Выбранный backend сохраняется в manifest; для
диагностики можно вернуть `--transport-backend sockudo`.

## Live recorder OKX

Второй независимый источник использует ту же нормализованную схему и тот же
Parquet/replay-контроль:

```bash
.venv/bin/python -m market_data_lab.live_okx --duration-seconds 60
```

По умолчанию собираются `BTC-USDT-SWAP`, `ETH-USDT-SWAP` и
`SOL-USDT-SWAP`:

- L2 deltas, BBO quotes и public trades;
- mark price, index price и funding rate;
- open interest через публичный REST endpoint;
- `seqId`, resnapshot, socket events и целостность восстановленного стакана;
- отдельные распределения сырого `ts_init - ts_event`, HTTP RTT и смещения
  часов OKX относительно локальной машины.

API-ключи не нужны, execution client отсутствует. В режиме `VIP0` адаптер
использует стандартный public-канал `books`. Каналы
`books50-l2-tbt`/`books-l2-tbt` с более высокой частотой требуют
соответствующего VIP-уровня и не включаются автоматически. Стандартный канал
в проверенном VIP0-запуске отдал 400 уровней на каждую сторону стакана, хотя
локальный subscription depth был задан как 50.

Оценка смещения часов для каждого REST-запроса использует midpoint:
`offset = t_exchange - (t_send + RTT_monotonic / 2)`. В manifest основной
оценкой служит медиана offset среди 20% запросов с минимальным RTT (минимум три
запроса), а не среднее всех запросов. Это уменьшает влияние очередей в сети, но
не превращает HTTP endpoint в NTP: асимметрию прямого и обратного маршрута он
определить не может.

Сырые измерения сохраняются в `clock_offsets.jsonl`, OI — в
`open_interest.jsonl`, а агрегаты входят в `manifest.json`. RTT измеряется через
`CLOCK_MONOTONIC`. Параллельно recorder сравнивает прошедшее время
`CLOCK_REALTIME` и `CLOCK_MONOTONIC` и пишет `local_clock_continuity`: коррекция
часов на 5 мс и более будет отмечена как подозрительный discontinuity.

Каждый запуск создаёт новый каталог `data/live/okx/<run-id>/`; существующий
каталог никогда не перезаписывается. Для HTTP/HTTPS-прокси можно передать
`--proxy-url`. SOCKS поддерживается самим OKX WebSocket/HTTP-клиентом, но
stdlib-сэмплер OI через SOCKS не работает — в таком режиме используйте
`--disable-open-interest`.

## Временная модель для сравнения бирж

Не следует сводить все timestamps к одному полю:

- `ts_init` — локальное время получения/инициализации сообщения адаптером. При
  одновременном сборе Bybit и OKX на одной машине эти значения лежат на одной
  шкале и подходят для arrival-time lead/lag;
- `ts_event` — timestamp самой биржи. Он нужен для event-time анализа, но его
  семантика зависит от venue и канала;
- `CLOCK_MONOTONIC` — только измерение локально прошедшего времени и контроль
  скачков `CLOCK_REALTIME`; это не UTC timestamp.

Для стандартного OKX `books` поле `ts_event` — время генерации стакана. Bybit
передаёт и системное `ts`, и matching-engine `cts`, однако NautilusTrader
2.0.0rc3 записывает в order-book `ts_event` именно `ts` и не сохраняет `cts`.
Поэтому текущий основной межбиржевой тест должен сравнивать `ts_init`; сравнение
matching-engine timestamps потребует отдельного raw Bybit sidecar или изменения
адаптера.

Публичные time endpoints не исправляют и не переписывают market-data
timestamps. Они лишь дают диагностическую оценку `exchange - local` с явно
сохранённой неопределённостью. Системный NTP/chrony остаётся отдельным слоем.

## Одновременная запись Bybit + OKX

Для межбиржевого теста обе биржи нужно писать одновременно на одной машине.
Оркестратор запускает два независимых recorder-процесса почти одновременно,
проверяет фактическое пересечение их L2-потоков и создаёт общий manifest:

```bash
.venv/bin/python -m market_data_lab.dual_recorder \
  --bases SOL \
  --duration-seconds 600 \
  --run-group-id sol-10m-01
```

Результат находится в `data/live/dual/sol-10m-01/`: подкаталоги `bybit/` и
`okx/`, отдельные логи и общий `manifest.json`. По умолчанию REST polling
open interest и clock probe выключен, чтобы измерение L2 не засорялось
дополнительными запросами. API-ключи по-прежнему не нужны.

Каждый recorder также пишет `arrivals.jsonl`: один ряд на нормализованный
callback с `ts_init`, `CLOCK_REALTIME` и `CLOCK_MONOTONIC`. Для сигналов основной
шкалой остаётся `ts_init`; monotonic sidecar нужен как независимая проверка
порядка прихода между двумя процессами на одном Linux-хосте.

## Первый анализ исполнимого межбиржевого спреда

После успешной dual-записи:

```bash
.venv/bin/python -m market_data_lab.cross_venue \
  --run-group data/live/dual/sol-10m-01 \
  --notionals 100,1000,10000 \
  --latencies-ms 0,20,50,100,150,250 \
  --max-book-age-ms 100
```

Анализатор восстанавливает L2-книги и на каждом локальном `ts_init` использует
только последнее уже пришедшее состояние второй биржи. Для обоих направлений
он проходит по доступной глубине, покупает заданный USDT-notional, продаёт тот
же base-объём и считает gross/net edge. По умолчанию заложены консервативные
taker-комиссии 10 bps на каждой бирже; их следует заменить своими реальными
fee tier через `--bybit-taker-fee-bps` и `--okx-taker-fee-bps`.
`--max-book-age-ms` отбрасывает сравнение, если последняя книга хотя бы
одной биржи старше порога; это защита от stale-book псевдоспредов.

Отчёт `analysis/cross_venue_report.json` содержит распределения edge,
положительные эпизоды и repricing начала каждого эпизода после заданной
задержки. Это пока диагностическая верхняя граница, а не backtest PnL: модель
ещё не учитывает order acknowledgement, частичное исполнение между снимками,
инвентарный hedge/rebalance, funding и лимиты риска.

## Read-only DEX quote recorder

Следующий разведочный слой одновременно запрашивает исполнимые котировки для
нескольких размеров, не подключая кошелёк и не отправляя транзакции:

```bash
.venv/bin/python -m market_data_lab.dex_quotes \
  --duration-seconds 60 \
  --interval-seconds 5 \
  --notionals 100,1000,5000 \
  --run-id dex-1m-01
```

По умолчанию включены:

- Uniswap v3 WETH/USDC на Base через прямой `eth_call` к QuoterV2 с block tag
  `pending`;
- Uniswap v3 WETH/USDC и WETH/USDC.e на Polygon через прямой `eth_call` с
  fee tiers 1, 5 и 30 bps;
- Raydium SOL/USDC через публичный Route API v2;
- STON.fi TON/USDT через публичную DEX simulation API v2.

Для каждого notional сначала моделируется покупка base за quote, затем продажа
полученного количества base обратно. В `quotes.jsonl` сохраняются обе стороны,
средняя исполнимая цена, raw amounts, price impact/route metadata, локальные
временные метки запроса и ответа и HTTP RTT. `manifest.json` фиксирует источники,
параметры и качество сбора. Новый запуск всегда требует новый каталог.

Это два разных класса данных. Uniswap здесь опрашивается непосредственно через
RPC и состояние контракта; Raydium и STON.fi пока являются API-разведкой и могут
агрегировать или кэшировать состояние. Их arrival timestamp нельзя трактовать
как время прихода состояния валидатора. Для настоящего latency/MEV-теста на
Solana следующим слоем нужен собственный RPC WebSocket `accountSubscribe` по
выбранным пулам; на TON — подписка на состояния пулов и разбор асинхронных
transaction traces. Jupiter Swap API v2 не является обязательной зависимостью:
он допускает keyless research-режим с жёстким лимитом, а API key нужен для
более высокой и наблюдаемой квоты; Raydium и STON.fi endpoints публичны, но
тоже имеют service-level rate limits.

Можно ограничить источники, например:

```bash
.venv/bin/python -m market_data_lab.dex_quotes \
  --providers RAYDIUM,STONFI \
  --duration-seconds 300 \
  --notionals 100,1000
```

Публичные endpoints годятся для поиска кандидатов и проверки размера edge, но
не доказывают реализуемый PnL. Перед исполнением ещё нужно вычесть gas/priority
fee, tip, CEX fees, adverse selection и стоимость hedge/rebalance, а также
проверить свежесть состояния и вероятность включения транзакции.

## CEX ↔ DEX inventory-cycle scanner

`scan-cex-dex-cycles` сопоставляет точные DEX-котировки с пройденной глубиной
spot-книги CEX. Это только read-only экран: он не принимает ключи, кошелёк или
private key и не строит/не отправляет транзакции.

```bash
.venv/bin/scan-cex-dex-cycles \
  --cex-venue MEXC \
  --mexc-book-source ws \
  --cex-depth 20 \
  --cex-stream-max-age-ms 500 \
  --markets SOL_SOLANA_RAYDIUM_USDT,PUMP_SOLANA_RAYDIUM_USDT \
  --notionals 50,100,200 \
  --duration-seconds 300 \
  --interval-seconds 5 \
  --max-response-skew-ms 300 \
  --sequential-providers \
  --run-id mexc-solana-screen-01
```

Поддерживаются public books Bybit, MEXC, Binance и OKX; для MEXC режим `ws`
держит официальную partial-depth книгу 5/10/20 уровней и отбрасывает событие,
если оно старше `--cex-stream-max-age-ms`. В JSONL всегда сохраняются источник
книги, время получения, CEX event time, exact-input DEX quote, пройденная
глубина, DEX impact/fees, CEX taker fee и нижняя оценка network cost.

Solana USDT-маршруты доступны, например, как
`SOL_SOLANA_RAYDIUM_USDT`, `JUP_SOLANA_RAYDIUM_USDT`,
`PUMP_SOLANA_RAYDIUM_USDT` и их `JUPITER_USDT` варианты. Solana USD₮ mint
зафиксирован по официальному адресу Tether. `cbBTC` помечается отдельно: это
не нативный BTC, поэтому redemption/wrapper basis исключён из PnL-модели.

Raydium public quote API ограничен 120 запросами/минуту на IP. Все Raydium
providers в одном запуске используют общий pacer `0.6` секунды по умолчанию;
его можно изменить через `--raydium-min-request-interval-seconds`. Jupiter
без ключа используется только в режиме исследования (0.5 request/s); ключ
бесплатного плана повышает лимит до 1 request/s и нужен до любого
долгоживущего мониторинга.

Положительная строка означает лишь кандидата на **pre-funded inventory cycle**:
base и quote уже должны лежать на обеих площадках. Она не подтверждает
atomic-arbitrage или withdraw-and-transfer PnL, поскольку transfer/rebalance,
priority fee/tip сверх floor, вероятность fill, изменения пула до inclusion и
капитальные издержки остаются вне модели.

### Долгий монитор с окном сырых данных

`monitor-rolling-cycles` — вариант для долгого наблюдения без трёхчасового
дампа каждой книги и котировки. По умолчанию он обходит MEXC, Bybit, OKX и
Binance против Raydium, Jupiter, Omniston, STON.fi и Uniswap на заранее
ограниченном наборе SOL/PUMP/cbBTC, GRAM/NOT и WETH/WBTC маршрутов.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.rolling_cycle_monitor \
  --duration-seconds 10800 \
  --run-id rolling-cex-dex-3h-01
```

В каталоге запуска есть только три полезных постоянных артефакта:

- `recent.jsonl` — **атомарно перезаписываемое** окно последних 60 секунд
  диагностических наблюдений;
- `candidate_events.jsonl` — только старт, улучшение и закрытие
  timing-valid положительного inventory-cycle, с длительностью и максимумом;
- `stats.json` — счётчики, ошибки источников и лучший edge по каждому кругу.

Поэтому `recent.jsonl` можно безопасно читать прямо во время процесса, а
рост постоянных данных зависит от числа реальных кандидатов, а не от частоты
книг. Значения CEX fees в manifest — исследовательские допущения: по умолчанию
MEXC `5 bps` (документированная стандартная ставка, не персональная акция), остальные `10 bps`; перед реальным исполнением их надо заменить
на фактические условия аккаунта.

Для максимального покрытия уже исследованных маршрутов не запускай все пары
в одном медленном цикле. Есть пять непересекающихся профилей: `core` (10
ликвидных Solana/TON маршрутов на всех четырёх CEX, cadence 20 s),
`solana-mexc-longtail` (10), `solana-bybit-usdc` (14), `ton-longtail` (6) и
`evm-reference` (4). Вместе это 44 ранее наблюдавшихся CEX↔DEX маршрута.
Long-tail обновляется раз в 2–3 минуты; это сохраняет лимит Raydium/Jupiter и
не задерживает liquid core.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.rolling_cycle_monitor \
  --profile core \
  --duration-seconds 10800 \
  --stdout-candidates \
  --run-id coverage-core-3h-01
```

### Непрерывный event-driven CEX ↔ DEX monitor

`monitor-continuous-cycles` предназначен для другого режима: CEX не
опрашиваются по таймеру. Для MEXC, Bybit, OKX и Binance открываются публичные
WebSocket-книги и каждая свежая книга немедленно сверяется с ещё свежей точной
DEX-котировкой, которая хранится только в памяти. DEX-источники вызываются
непрерывно, но каждый в пределах отдельного публичного rate budget: Raydium и
Jupiter используют общий внутренний pacer, а STON.fi, Omniston и Uniswap —
отдельные общие ограничения запуска quote round.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.continuous_cycle_monitor \
  --markets HNT_SOLANA_RAYDIUM_USDT,NOT_TON_OMNISTON,DOGS_TON_STONFI \
  --cex-venues MEXC,BYBIT,OKX,BINANCE \
  --duration-seconds 600 \
  --max-response-skew-ms 300 \
  --max-dex-cache-age-ms 300 \
  --stdout-candidates \
  --run-id hot-routes-10m-01
```

У него **нет общего `--interval-seconds`**. Частота CEX равна частоте
публичных push-сообщений; точный DEX quote повторяется сразу после завершения
предыдущего, когда это разрешает общий pacer источника. Для Jupiter без
отдельной quota это всё равно не означает sub-second quote для каждой пары из
широкого списка: все пары делят публичный лимит. Поэтому широкий universe
годится для постоянного обнаружения, а отдельный hot-list из 1–3 маршрутов —
для максимальной частоты exact quotes.

На диск не пишутся сырые книги и котировки. В новом каталоге остаются только:

- `candidate_events.jsonl` — lifecycle timing-valid положительных кругов;
- `stats.json` — частоты CEX updates/DEX quotes, счётчики и последние 100
  диагностических ошибок;
- `manifest.json` — конфигурация, источники и границы модели.

Именно этот режим следует использовать для проверки кратких HNT/NOT/DOGS
всплесков. Он остаётся read-only и не создаёт ордера или транзакции.

`--stdout-candidates` печатает одну компактную JSON-строку только для
подтверждённого positive candidate (`candidate_started`, улучшение или
закрытие): после exact DEX quote, CEX depth-walk, fee/network floor и проверки
response skew. Это удобно оставить видимым в `tmux` без потока отрицательных
наблюдений.

### Точные комиссии аккаунта для CEX spot-ноги

Публичная таблица fee — только безопасный fallback: VIP-уровень, регион,
symbol promotion и fee-token могут отличаться у конкретного аккаунта. Поэтому
для `monitor-continuous-cycles`, `monitor-rolling-cycles` и
`monitor-triangle-cycles` добавлен отдельный одноразовый read-only аудит.
Он вызывает только документированные signed `GET` fee endpoints Bybit, OKX,
Binance и MEXC. Для OKX он также читает `account/instruments`, чтобы
сопоставить symbol с актуальным `feeGroup`; не читает balances/positions, не
содержит order endpoint и не сохраняет ключ, secret, passphrase, signature или
raw private response.

Создай на нужной бирже **отдельный** ключ с минимальным read-only разрешением
на fee/account-trade information, без trade, withdraw, transfer и wallet
permissions. Сохрани его лишь в локальных environment variables с именами,
которые печатает `audit-cex-fees --help`, затем запусти аудит нужного
universe. Например, `all-current` автоматически берёт все CEX-symbol из
нынешних maximum single-asset маршрутов и 95 triangle-маршрутов, включая
`USDT`/`USDC` и формат `BTC-USDT` у OKX:

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.account_fee_audit \
  --venues BYBIT,OKX,BINANCE,MEXC \
  --market-universe all-current \
  --run-id my-spot-fees-01
```

Есть более узкие режимы `continuous-maximum` / `rolling-maximum` (текущие 44
single-asset маршрута) и `triangle-default` (обе CEX-ноги 95 cross-пар).
Аудит делает отдельный read-only `GET` для каждого `venue:symbol` с паузой
0.55 s по умолчанию. Если широкая выборка включает несуществующую пару или
временный API отказ, это попадает в `symbol_errors`, а остальные подтверждённые
ставки сохраняются. Такой symbol **не** получает выдуманную ставку: монитор
оставит для него public fallback и не запишет candidate. Для малого ручного
набора по-прежнему можно передать `--symbols-by-venue`. Такая пауза держит OKX
`trade-fee` ниже документированного лимита 5 запросов за 2 секунды.

Отчёт появится как `data/fee-audits/my-spot-fees-01.json`. Он хранит maker и
taker ставки отдельно для BUY/SELL; это нужно для Binance, где fee components
могут зависеть от стороны. Подай его в непрерывный монитор:

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.continuous_cycle_monitor \
  --markets ETH_BASE_UNISWAP,BTC_BASE_UNISWAP \
  --cex-venues BYBIT,OKX,BINANCE,MEXC \
  --cex-fee-audit-file data/fee-audits/my-spot-fees-01.json \
  --duration-seconds 600 \
  --run-id account-fee-screen-10m-01
```

Только совпавшие `venue:symbol` с `account_verified=true` могут попасть в
`candidate_events.jsonl`. Для triangle нужны подтверждённые ставки **обеих**
CEX-ног. Если для пары нет audit rate, монитор всё ещё считает её по
консервативному public fallback и показывает в `stats.json` как diagnostic
positive, но не называет candidate. Это защищает от ложной точности.
Даже audited rate не делает PnL точным: BNB/MX fee-token availability в момент
fill, network gas, funding, transfer/rebalance и fill probability остаются
отдельными расходами.

### DEX spot ↔ Bybit perpetual basis

`monitor-perp-dex-basis` рассматривает не мгновенный перевод между площадками,
а дельта-хеджированную позицию: DEX `USDC → WBTC` и одновременный short
`BTCUSDT` perpetual на Bybit (или обратное направление). Он получает:

- Bybit linear L50, mark/index, текущий funding и время следующего funding;
- публичные параметры контракта: шаг, minimum order/notional и лимит market
  order;
- DEX exact quote. Для Uniswap v3 на Base/Polygon DEX-buy выполняется как
  exact-output quote на объём, заранее кратный шагу Bybit perpetual, поэтому
  не возникает скрытый незащищённый остаток;
- две CEX taker-комиссии (открытие и текущий reserve на закрытие) и две
  консервативные DEX network-cost floors — на вход и будущий выход.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.perp_dex_monitor \
  --markets BTC_BASE_UNISWAP,BTC_POLYGON_UNISWAP \
  --notionals 250,500 \
  --duration-seconds 600 \
  --stdout-candidates \
  --run-id btc-perp-basis-10m-01
```

Без ключа Bybit используется только явно помеченная публичная ставка VIP0
`5.5 bps` для linear taker. Это **не** персональная комиссия и положительный
ряд с ней не сохраняется как candidate. Для точной ставки аккаунта создай
отдельный API key с единственным read-only permission, без trade/withdrawal,
передай его только через локальные переменные окружения и добавь
`--use-account-fee-rate`. Монитор вызывает исключительно
`GET /v5/account/fee-rate`, не сохраняет ключ/secret и не имеет execution
client.

Постоянные файлы — только `manifest.json`, агрегированный `stats.json` и
ограниченный `candidate_events.jsonl`; сырые DEX/CEX updates не записываются.
Даже после точной account fee это модель entry basis, а не обещание PnL: будущий
funding, gas/priority fee, state DEX при включении, wrapper/redeem и inventory
rebalance остаются отдельными рисками.

### On-chain CLMM prefilter для PUMP

Когда публичный Raydium/Jupiter quote API слишком медленный для непрерывного
поиска, `record-raydium-clmm-prefilter` читает непосредственно два Raydium CLMM
pool-state аккаунта через Solana `accountSubscribe`: `PUMP/SOL` и `SOL/USDT`.
Он соединяет их mid-price с live best bid/ask MEXC и записывает все срезы,
помечая кандидаты выше `--trigger-bps`, но **не** называет их прибылью: здесь
ещё нет fees, tick traversal, impact, пройденной CEX-глубины или network cost.
Триггер разрешает лишь немедленно запросить точную котировку.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.raydium_clmm_prefilter \
  --duration-seconds 300 \
  --interval-seconds 0.25 \
  --trigger-bps 30 \
  --run-id raydium-pump-prefilter-01
```

В manifest фиксируются два pool ID, Solana HTTP/WS endpoint, максимальный
наблюдённый mid-edge и отсутствие ключей, кошелька и отправленных транзакций.

Чтобы автоматически проверить редкий raw-trigger exact-input котировкой,
добавь `--exact-quote-notional`. Проверки ограничены
`--exact-min-interval-seconds`; результат пишется в `exact_checks.jsonl` вместе
с nearest MEXC book, depth-walk, настроенной CEX fee и network-cost floor.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.raydium_clmm_prefilter \
  --duration-seconds 300 \
  --trigger-bps 30 \
  --exact-quote-notional 100 \
  --exact-min-interval-seconds 5 \
  --run-id raydium-pump-exact-screen-01
```

### Unified realtime scanner

`scanner.py` — единый supervisor, а не набор ручных `tmux`-команд. Он запускает
public CEX WebSocket, прямые Solana account subscriptions, ограниченные TON/EVM
quote loops, Bybit-perpetual basis monitor и отдельный нейтральный слой
perp-котировок. Последний собирает одинаковые нормализованные L2/context
события от Hyperliquid, Aevo, Bulk, dYdX и Drift; он **не** выбирает стратегию
и не создаёт кандидатов сам. Для Drift локальный TypeScript worker строит
публичный on-chain DLOB вместе с vAMM/oracle через настроенный Solana RPC,
поскольку старый hosted DLOB endpoint больше не существует.

Каждый источник публикует нормализованное состояние в RAM; на диск идут только
manifest/status/stats и ограниченные lifecycle-события уже существующих
экранов. По умолчанию в памяти остаётся не более трёх минут истории на ключ,
сырые DEX/CEX updates не сохраняются. Для очень частых L2 лент ретроспективное
top-of-book окно coalesce-ится до 20 мс, но live bus получает каждый update.

Обычный непрерывный запуск:

```bash
.venv/bin/python scanner.py
```

Для короткой проверки добавляется только `--duration-seconds 60`. Последний
каталог и состояние всех компонентов указаны в `data/live/scanner/latest.json`.

Низкоуровневый бесплатный пример намеренно мал: два известных PUMP/SOL и
SOL/USDT CLMM-пула плюс MEXC. Основной `scanner.py` поверх него автоматически
добавляет проверенные пулы из локального registry/discovery; пример остаётся
быстрой транспортной проверкой, а не торговой рекомендацией.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.solana_realtime_scanner \
  --config config/solana-pump-free.toml \
  --duration-seconds 300 \
  --run-id solana-state-smoke-01
```

Во время работы `data/live/unified-scanner/<run-id>/status.json` показывает
возраст последнего update, slot, reconnect/errors, ограничение очереди и число
в RAM-состояний. Источники, worker и CEX streams живут под одним supervisor;
не нужно вручную держать согласованный набор `tmux`-команд.

### Solana pool registry и local exact route evaluator

`refresh-solana-pool-registry` — редкая discovery-задача, а не price feed. Она
ограниченными запросами к Raydium, Meteora и Orca получает каталог, отбирает
только известные ликвидные mint-пары и сохраняет компактные статические
метаданные пула. Недоступность одного каталога не стирает полезный cache. Сырые
catalog rows и цены не записываются.

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.solana_pool_registry \
  --output data/registry/solana-pools.json
```

Solana hot-path использует один управляемый TypeScript child-worker. Endpoint
передаётся ему только через stdin. Сейчас он локально считает exact-input для:

- Raydium CLMM — pool state и ограниченный cache tick arrays;
- Raydium CPMM и legacy AMM v4 — согласованные pool+vault snapshots и точные
  on-chain fee-параметры;
- Meteora DLMM — pair state и соседние bin arrays;
- Orca Whirlpool — pool state и соседние tick arrays.

Worker обрабатывает quote requests конкурентно, но ограничивает число
одновременных задач; одинаковые RPC refresh дедуплицируются. Он не создаёт
wallet, instruction, transaction или order. Jupiter key, если он указан в
локальном TOML, **не** участвует в hot-path: Jupiter вызывается только после
положительного локального prefilter как независимая quote-only проверка.
Горячие изменения идут через WebSocket; каждые 15 секунд worker делает
низкочастотный batch snapshot всех pool accounts. Это подтверждает состояние
тихих пулов и исправляет возможное пропущенное WS-событие. Timing skew
сравнивает только независимые CEX-стаканы: отсутствие DEX update означает,
что account state не изменился, а не то, что цена неизвестна.
HTTP RPC для этих страховочных refresh имеет общий pacer: по умолчанию не более
пяти стартов запроса в секунду суммарно для Raydium, Meteora и Orca. Это
не ограничивает частоту WS-котировок и уменьшает риск HTTP 429 от refresh-всплесков;
лимиты провайдера и расход других приложений всё равно нужно контролировать.
Pool/tick/bin accounts, на которые есть subscription, продолжают обновляться
без polling; полная HTTP-перезагрузка обходных tick/bin arrays срабатывает не чаще
раза в пять минут или сразу при переходе цены в новый array.

В шаблоне и локальном профиле есть выключенный пример `pump-sol-mexc-usdt`.
Это не пара «PUMP к стейблу на DEX», а явный трёхногий inventory screen:

```text
USDT -> SOL на MEXC -> PUMP в Raydium PUMP/SOL -> USDT на MEXC
```

и обратное направление. Поэтому он проходит глубину **двух** CEX-стаканов и
закладывает обе taker-комиссии; SOL/USDT не считается автоматически равным
одному доллару. Одноразовый read-only smoke-test:

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.solana_realtime_scanner \
  --config config/solana-rpc.local.toml \
  --enable-local-quote-worker \
  --enable-local-route-evaluator \
  --duration-seconds 60 \
  --run-id solana-local-route-smoke-01
```

`status.json.extensions.local_route_evaluator` содержит количество exact
checks, stale/skewed состояний, unavailable quotes, распределение local quote
RTT, лучший положительный edge и lifecycle кандидатов. В
`candidate_events.jsonl` попадают только start /
улучшение / close положительного экрана, максимум `candidate_event_limit`
строк; без positive screen этот файл вообще не создаётся. Все книги, pool
updates и промежуточные quote results остаются только в RAM.

Комиссия из примера MEXC — лишь публичный baseline. Чтобы пометить маршрут
account-verified, отдельно создай read-only audit без торговых прав и укажи
созданный компактный файл как `local_route_evaluator.fee_audit_file`; scanner
сам никогда не читает API secret. Даже тогда positive screen не является
сделкой или реализованным PnL: не включены CEX lot/minimums, actual fills,
priority auction/inclusion, wrapper/redeem basis, наличие инвентаря и
rebalance/transfer.

Быстрый независимый probe, не запускающий тяжёлые market-data clients:

```bash
.venv/bin/python -m market_data_lab.clock_probe \
  --venues BYBIT,OKX \
  --samples 10 \
  --interval-seconds 1 \
  --output data/clock-probes/my-probe.json
```

После обновления editable install доступна эквивалентная команда
`probe-exchange-clocks`. В direct-режиме recorder и probe намеренно игнорируют
ambient `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` только внутри своего процесса,
чтобы случайный глобальный proxy не менял маршрут измерений. Для явно
выбранного HTTP/HTTPS proxy используется `--proxy-url`; значение proxy в
manifest не сохраняется.
