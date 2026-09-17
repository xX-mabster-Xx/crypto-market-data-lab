# Агент B — точная инвалидация candidate lifecycle

**Модель: OpenAI GPT-5.6 Luna. Reasoning: high, если доступно.**
Выбор: локальная задача в двух analyzer-классах с точно заданной матрицей тестов. Если восстановить реальные зависимости candidate не удаётся, передай конкретный блокер G/Sol, а не угадывай принадлежность по строке ключа.

**Исправить:** R-06. Связанные требования: BUG-003; не нарушить BUG-010.
**Старт:** волна 1, параллельно A/C.

## Обязательные правила выполнения

Это задание на исправление кода, но выполнять его следует только после назначения этого задания агенту. Само наличие файла не запускает работу. Работай в репозитории crypto-market-data-lab; найди его корень, не полагайся на текущий каталог. Базовый аудит: commit `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`, файл `BUGFIX_V2_AUDIT_2026-09-16.md`. Оригинальные требования: `crypto-market-data-lab_bugfix_spec_v2.md`. Прочитай относящиеся к тебе разделы и локальные AGENTS.md. Аудит — проверяемая исходная информация, не замена чтению текущего кода: номера строк могли измениться.

Начни с `git status --short` и проверки текущей реализации. Не восстанавливай и не включай в свою работу пользовательские удаления `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, `bug-002-keyed-coalescing-event-bus.patch`. Сохраняй все чужие изменения. Не делай reset/clean, массовое форматирование, upgrade SDK, commit/push или live trading. Не изменяй исторический аудит задним числом. Не создавай субагентов автоматически.

Сначала воспроизведи дефект тестом на production-коде; затем минимально исправь и покажи, что тот же тест стал зелёным. Моки допустимы на IO/SDK/clock-границе, но не подменяй исправляемый алгоритм его тестовой копией. Проверки текста исходников не заменяют runtime-тесты. Не ослабляй ожидания и не удаляй регрессии ради зелёного результата. Используй apply_patch для ручных изменений. Генераторы lockfile и форматтеры допустимы в своём scope; соблюдай действующие approval/network правила.

Читай чужие файлы по необходимости, но редактируй только свою зону. Если нужен чужой интерфейс, зафиксируй точный контракт в handoff и согласуй передачу владения; не вноси скрытых параллельных правок. Отсутствие доступа/зависимости/Node 24 отмечай как BLOCKED, не как PASS. По умолчанию тесты offline: без реальных RPC, секретов, глобального прокси, root и изменения системного runtime.

Для экономии сначала запускай новые узкие тесты, затем относящийся к задаче пакет. Полный совместный gate принадлежит агенту G. Не считай прежние 486 Python tests / 12 Node test files гарантией исправления: они были зелёными и при найденных дефектах.

## Формат сдачи

Создай `remaining-bugs-agent-plan/results/AGENT_<ID>_RESULT.md`, подставив свою букву. Укажи: commit/dirty baseline; закрываемые audit IDs; изменённые файлы; причина дефекта; что изменено; команды и фактические результаты тестов; доказательство red→green; изменения интерфейсов/схем; совместимость; риски и BLOCKED/NOT RUN проверки. Для каждого ID — FIXED / PARTIAL / NOT FIXED с доказательством. Приложи компактные артефакты, но не бесконечные raw-event логи и не секреты. Нельзя писать «всё исправлено», если хотя бы один обязательный критерий не проверен.

## Зона владения

- `src/market_data_lab/unified_cycle_analyzer.py`.
- `src/market_data_lab/unified_perp_analyzer.py`.
- Локальные структуры provenance active candidates и их индексы.
- Свои `tests/test_remaining_b_*.py`; существующие analyzer/monotonic-lifecycle tests при необходимости.
- `unified_market_data.py` прочитать для broadcast; не отключать broadcast как обход ошибки. Event store и Node код не менять.

## Дефект и требуемая семантика

Методы source-epoch purge корректно удаляют source-owned quotes/legs, но затем безусловно очищают все dirty groups и закрывают все active candidates. Даже reconnect источника B, от которого candidate A не зависит, ломает длительность и историю A.

Сначала добавь production-тест с реально открытым candidate, зависящим от источника A; transition B обязан оставить тот же active candidate, original open monotonic timestamp, persisted flag, counters и dirty work. Не ограничивайся пустым analyzer или вставкой фиктивного объекта, которая обходит построение candidate.

Затем:
1. Явно установи зависимости active candidate от source/epoch/legs, включая direct, triangle, spot-perp и DEX-perp варианты. Используй существующий provenance, дополни candidate state только если он действительно недостаточен.
2. Закрывай только candidates, чьи используемые данные больше не принадлежат актуальной epoch. Удаляй/пересчитывай только затронутые dirty groups; независимая работа должна сохраниться.
3. Для shared base/provider, где разные candidates имеют разные источники, принадлежность должна определяться actual legs, не только base/provider string.
4. Сохрани строгий отказ старым events, idempotence повторного transition, порядок control-plane-before-new-data и возможность переоткрытия affected candidate на свежих legs.
5. Candidate lifetime считать monotonic; wall clock оставить для человеческих timestamps. Не создавать лишние close/open записи для unaffected candidate.
6. Любые новые indexes удалять при close, eviction, shutdown; память ограничена живыми candidates/legs.

## Обязательные тесты

Для ОБОИХ analyzers:
- candidate A активен; первая регистрация и reconnect unrelated B не закрывают A;
- transition A закрывает affected candidate ровно один раз с правильной причиной;
- два candidates одной base: один affected, другой нет;
- candidate с несколькими sources: transition любой реально использованной stale leg инвалидирует только нужные candidates;
- persisted и ещё не persisted состояния; счетчик shorter_than_minimum_persistence не растёт из-за unrelated transition;
- независимые dirty groups не исчезают; affected groups не используют старые legs;
- повтор той же epoch, old-epoch event и fresh replacement;
- jump wall clock не меняет duration, shutdown очищает новые индексы.

## Приёмка B

Тесты подтверждают фактическую историю/counters и содержимое active/dirty sets, не только факт вызова purge. Запусти свои тесты, tests/test_agent_c_bugfixes.py, tests/test_bug_010_monotonic_candidate_lifecycle.py и оба analyzer test-файла. В result используй ID B.
