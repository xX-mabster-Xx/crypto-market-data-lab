# Агент A — Python event plane и state admission

**Модель: OpenAI GPT-5.6 Sol. Reasoning: high, если настройка доступна.**
Выбор: конкурентность asyncio, порядок версий и индексы связаны; цена неверного локального упрощения выше экономии на модели.

**Исправить:** R-01, R-02, O-04 из аудита. Связанные требования: BUG-002, BUG-005, BUG-020; сохранить BUG-001/007/012.
**Старт:** волна 1; можно параллельно с B и C. В конце согласовать provenance-контракт с D. Не ждать E/F.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

- `src/market_data_lab/realtime_scanner.py`: CoalescingEventBus, RollingStateStore.
- `src/market_data_lab/versioned_market_state.py` и непосредственно используемый envelope/order contract, если требуется.
- `src/market_data_lab/solana_realtime_scanner.py`: только разбор/версионирование pool_state; metrics-часть позже принадлежит F.
- Свои новые `tests/test_remaining_a_*.py`; относящиеся существующие tests/test_realtime_scanner.py, tests/test_agent_f_worker_contract.py.
- Не редактировать analyzers, Node engines, snapshot codec или worker stats.

## A1. R-01: очередь с конкурентными publishers

Исходное воспроизведение: capacity=2, pending A/C; две задачи publish(B v3)/publish(B v4) засыпают на capacity; consumer забирает A/C без yield; после пробуждения получается order=[B,B], latest={B:v4}; consume B, publish D, следующий consume падает KeyError(B).

Исправь повторную проверку key/capacity под Condition после каждого пробуждения. Поддерживай инварианты: каждая pending key ровно один раз в порядке; order и payload index согласованы; distinct keys <= capacity; ожидающий устаревший publisher не перетирает более новую принятую версию. Продумай store.add, уже выполненный до await, отмену ожидающего publisher и shutdown. Не держи lock во время пользовательского обработчика и не заменяй это неограниченной очередью задач. Не лечи KeyError простым игнорированием повреждённого индекса.

Тесты: описанное расписание; обратный порядок пробуждения старой/новой версии; capacity=1 и 2; несколько hot keys + cold unique key; rejected store event не меняет очередь; cancel/shutdown будят/завершают waiters без hang. Управляй barrier/events, а не случайными sleep. Проверяй latest-value и структуру после каждого этапа.

## A2. R-02: версии core и dependencies на границе Python

Сейчас chain_position=core slot превращается в source_sequence; равный core со свежими dependencies отклоняется как duplicate_source_sequence.

Пропиши явный контракт порядка состояния. Core chain slot должен остаться реальным core slot, а не max dependency slot или искусственно упакованным числом. Версия состояния должна различать изменения dependencies, сохранять epoch/boot boundaries и не ослаблять stale/duplicate protection остальных источников. Допустим отдельный явно типизированный state revision/составная версия; если выбираешь source-local revision, сначала отфильтруй устаревшее содержимое и докажи его связь с engine generation. Не просто убирай source_sequence и не считай любой поздно пришедший event свежим.

Регрессия обязана пройти через `RaydiumLocalQuoteStateSource.run → RollingStateStore → CoalescingEventBus → callback`, а не только collector:
- core100/dep110/gen1 → core100/dep120/gen2: оба приняты, второй доставлен, health отражает acceptance;
- точный повтор gen2 отвергнут/идемпотентен; gen1 после gen2 не откатывает state;
- core105 при dependency max120 принимается: dependency slot не блокирует более новый core;
- старый core не принимается под видом новой generation;
- новая source epoch может сбросить generation; старая epoch отклоняется;
- отсутствие обязательного provenance/malformed fields обрабатываются явно по совместимому контракту.

Передай D точное описание полей и сравнения: где generation увеличивается, где сбрасывается, как принимается core update без dependency change. Не изменяй консистентность данных engines на стороне Python.

## A3. O-04: устранить full-store scan на каждом tick

Сейчас add() всегда вызывает _retire_state_key, который копирует/обходит два полных словаря; epoch invalidation повторяет это для каждого ключа.

Сделай key-local cleanup через согласованный reverse index state_key→event keys (или эквивалент) и по необходимости source→state keys. Обычный update неизменного key должен быть amortized O(1) по числу чужих live keys; retirement — по числу реально затронутых entries. Sweeps могут быть O(K), но не каждый tick; epoch purge не должен быть O(K²). Сохрани TTL, capacity, deterministic eviction, history/latest/versioned-store consistency, смену state_key у существующего event_key и отсутствие dangling reverse entries.

Тесты: hot update при K=10/1,000/10,000; operation-count instrumentation вместо хрупкого wall-clock assert; физическое retirement/epoch/capacity cleanup во всех индексах; переиспользование event key/state key; 100,000 dynamic notionals с фиксированными logical slots. Добавь компактный benchmark со временем/allocations как наблюдение, не аппаратно-зависимый жёсткий gate.

## Приёмка A

Новые регрессии зелёные, связанные существующие Python tests не сломаны; ни один тест не требует сети. Нет синтетического chain slot, новых unbounded индексов и потери accepted-state health semantics. Handoff содержит стабильный контракт для D/E/G. В result используй ID A.
