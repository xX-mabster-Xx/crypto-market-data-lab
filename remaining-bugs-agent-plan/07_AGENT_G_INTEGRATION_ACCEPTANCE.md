# Агент G — интеграция, независимая проверка и финальный gate

**Модель: OpenAI GPT-5.6 Sol. Reasoning: high для review; обычные тесты выполняй инструментами, не трать reasoning на ожидание.**
Выбор: независимая проверка нескольких контрактов и доказательство отсутствия регрессий. Это не повторное написание исправлений с нуля.

**Закрыть:** доказательства исправления ВСЕХ R-01..R-06/O-01..O-06 и полный статус BUG-001..024; незавершённые acceptance gates исходной спецификации.
**Старт:** после сданных A–F. Не принимать отдельный зелёный unit suite за готовность.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

Финальные integration/stress/soak tests и harness, сбор артефактов, итоговый статус. Читать весь diff. Production изменения после handoff допустимы только для выявленного integration defect с собственной регрессией и указанной причиной; крупную переработку вернуть профильному агенту. На этом этапе параллельного владения production файлами уже нет.

## G1. Проверка сдач и red→green доказательств

Прочитай все results/AGENT_*_RESULT.md, сравни actual diff и тесты. При отсутствующем доказательстве запусти воспроизведение самостоятельно. Не делай git checkout/reset рабочей копии для before-case: используй безопасный отдельный checkout/worktree/temp copy с соблюдением approval; исходный commit и пользовательские удаления сохранить.

Матрица12 findings:
- R-01: concurrent blocked same-key publishers, unique order, final latest, cancellation/shutdown.
- R-02: dependency-only Node state принимается в Python store/bus и доходит до реального consumer; equal/stale/epoch cases.
- R-03: missed WS восстановлен RPC; delayed old response не откатывает dependency; stale cache не получает ложную freshness.
- R-04: actual fee data и recorded config version не расходятся при core100/config120/RPC105.
- R-05: provenance и consistency survives snapshot→decode→simulation/evidence; capture не омолаживает stale cache; cross-process monotonic не сравнивается напрямую.
- R-06: affected candidate закрывается, unrelated сохраняет identity/duration/dirty work.
- O-01: physical fetch starts paced при sequential/parallel nested SDK jobs/retries, bounded queues, concurrency1 no deadlock.
- O-02: все100 pools реально обслуживаются при600 maintenance ticks; failing first pool не starve'ит остальных.
- O-03: unchanged refresh не увеличивает semantic generation/emission; final dependency change доставлен.
- O-04: normal update не обходит все unrelated keys; epoch retirement не квадратичный; indexes physically bounded.
- O-05: clean Node24 npm ci/check/test, exact metadata/pins, реальные SDK versions.
- O-06: required stats end-to-end, one stable latest, warnings/rate limit, reset/shutdown.

## G2. Совместимость и реальные production boundaries

Добавь offline integration harness реального пути:
CEX stream→CexBookStateSource→compact BBO store + separate full-depth resolver→SolanaRouteEvaluator→реальный локальный Node subprocess с deterministic fixture transport.
Подмена внешнего RPC/CEX IO допустима; подмена самого quote engine/evaluator/pacer/store заглушкой не подтверждает их integration. Никаких реальных orders. Проверить enabled/disabled/fail-fast composition, dependency-only wakeup, CEX reconnect epoch, правильный route result при доступном state, полный subprocess/task shutdown.

Особо проверить shared file handoffs: A/F Python source parsing; C/D scheduling API; D/E data+provenance+age; E/F worker protocol/SDK metadata. Не допускается тихое игнорирование нового field или metrics с другим смыслом после интеграции.

## G3. Быстрые gates и stress

На зафиксированном итоговом дереве:
- .venv/bin/pytest -q -p no:cacheprovider;
- Node24: npm run check и npm test в workers/solana-quote-worker;
- targeted Protocol/type smoke BUG-013; не утверждать полный Python typecheck без настроенного и реально выполненного checker;
- 100,000 dynamic notionals, bounded keys/indexes;
- N1 slow stdout100,000 updates, lossless results и final latest;
- N2 blocked first CLMM processing +100,000 updates, pending<=1;
- N3 100,000 coalescible refresh jobs, bounded queues и interactive service;
- N4 100,000 Meteora/Orca dependency updates, bounded output, правильные final data/provenance;
- N5 искусственно разнесённые core/dependency versions, newer core не блокируется dependency max;
- elapsed lifecycle/reconnect/config/broker regressions исходных16 Supported items не сломаны.

Production engines нужны там, где проверяется engine semantics; helper-only tests недостаточны. Новые short smoke тесты допускаются как отладка, но не подменяют long gate.

## G4. Настоящий timed synthetic soak

Создай/используй воспроизводимый offline fixed-universe harness через реальные scanner/worker components. Минимум30 минут wall time, предпочтительно60; первые10 минут warm-up. Fake-clock ускоренный тест не считается этим gate. Отдельные short CI smoke и long manual mode. Не запускать live universe без отдельного разрешения на внешние запросы.

Каждые5–10s записывать bounded-on-disk CSV/JSONL: elapsed, processed updates, Python/Node RSS, Node heap/external/arrayBuffers, CPU/event-loop delay, stdout keys/lossless size, logical/physical RPC queues/inflight, per-pool pending, active pool count, bus keys, analyzer/index cardinality. Не логировать каждый market event. Зафиксировать hardware/runtime/universe/rates/seed/нагрузочные фазы.

Сценарий: warm-up→steady→dependency burst→slow stdout→recovery→shutdown; 10x/100x increase updates не должен давать соответствующий рост retained cardinality. После recovery очереди возвращаются к baseline. Процессы нельзя оставлять висящими по окончании/ошибке.

Pass criteria по spec§31.4/32:
- bounded containers не имеют update-count-dependent positive trend;
- stdout state keys<=active state keys;
- RPC queues bounded и восстанавливаются;
- core pending<=1 на pool;
- heap отражает GC, не sustained linear retention; RSS warm-up отдельно от post-warm-up;
- нет OOM, crash, watchdog restart, неограниченных loop stalls;
- JSON/stringify/emission count существенно ниже raw dependency updates; unchanged refresh не создаёт бурю.

Предварительно задай и запиши разумный baseline-relative noise/tolerance и как считаешь trend, не подгоняй после результата; исходная спецификация не задаёт магический абсолютный RSS/CPU limit. Forced production GC запрещён. Сравнение before/after CPU выполнять на одинаковом fixture workload в отдельном окружении, без изменения текущей рабочей копии.

Не опрашивай тест каждую секунду дорогой моделью: log/progress checkpoints и доступный follow-up механизм. Сохраняй понятные пользователю обновления, не блокируй интерфейс долгими wait.

## G5. Итоговые артефакты

Создай:
- results/AGENT_G_RESULT.md;
- results/FINAL_ACCEPTANCE.md: таблицы всех12 findings и всех24 original bugs;
- results/soak_summary.json + компактный CSV/JSONL metrics artifact и log команд;
- таблицу start / warm-up-end / end и причины каждого FAIL/BLOCKED/NOT RUN.

Статусы FIXED должны иметь reference теста и результат, integration gate — реальные команды/runtime/commit. «Все оставшиеся баги исправлены и проверены» допустимо только при полном закрытии12 findings и обязательных gates. Если фиксы готовы, но Node24/soak недоступен, так и написать; не маскировать это общим зелёным suite. В результате указать сохранность пользовательских изменений, отсутствие live trades и отсутствие незапрошенных commits/push.
