# План исправления оставшихся багов: 7 агентов

Дата: 2026-09-16. Основа: `BUGFIX_V2_AUDIT_2026-09-16.md` на `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`.
Этот пакет — **инструкции для будущего запуска**, а не выполненные исправления. Агенты не запущены; production-код на этапе подготовки пакета не менялся.

## Назначение моделей и файлов

| Агент | Модель | Работа | Задание |
| --- | --- | --- | --- |
| A | OpenAI GPT-5.6 Sol, high | Python bus, версии состояния, индексы | [01_AGENT_A_PYTHON_STATE.md](01_AGENT_A_PYTHON_STATE.md) |
| B | OpenAI GPT-5.6 Luna, high | Точная инвалидация candidate lifecycle | [02_AGENT_B_CANDIDATE_EPOCHS.md](02_AGENT_B_CANDIDATE_EPOCHS.md) |
| C | OpenAI GPT-5.6 Sol, high | Pacing физических RPC requests | [03_AGENT_C_RPC_PACING.md](03_AGENT_C_RPC_PACING.md) |
| D | OpenAI GPT-5.6 Sol, high | Solana caches, provenance, refresh fairness | [04_AGENT_D_SOLANA_ENGINES.md](04_AGENT_D_SOLANA_ENGINES.md) |
| E | OpenAI GPT-5.6 Sol, high | Snapshot evidence и freshness | [05_AGENT_E_SNAPSHOT_EVIDENCE.md](05_AGENT_E_SNAPSHOT_EVIDENCE.md) |
| F | OpenAI GPT-5.6 Luna, medium | Worker stats, lockfile и Node24 | [06_AGENT_F_OBSERVABILITY_RUNTIME.md](06_AGENT_F_OBSERVABILITY_RUNTIME.md) |
| G | OpenAI GPT-5.6 Sol, high | Независимая интеграционная приёмка и soak | [07_AGENT_G_INTEGRATION_ACCEPTANCE.md](07_AGENT_G_INTEGRATION_ACCEPTANCE.md) |

Reasoning — рекомендация, только если runner её поддерживает. Выбирать указанные модели явно; не подменять Sol неопределённым alias. A–F —6 исполнителей, G —финальный интегратор. **Не запускать всех7 одновременно.**

## Почему такая стоимость/качество

Это оценка риска задач, а не benchmark этих моделей на данном репозитории. По официальному описанию Sol предназначен для сложной профессиональной работы, Luna —для cost-sensitive workloads. Поэтому Luna отданы локальное targeted invalidation с подробными тестами и schema/config/observability; Sol —связанные concurrency/provenance изменения и финальная проверка.

Официальные standard API ориентиры на дату подготовки: Sol 4 USD input /20 USD output за1млн tokens; Luna 0.20/1.20 USD. Источники: [Sol](https://developers.openai.com/api/docs/models/gpt-5.6-sol), [Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna). Это не обещание стоимости в вашей платформе: тариф посредника, cached tokens, reasoning, context tier и число итераций могут отличаться. Из меньшей цены токена не следует автоматически меньшая цена успешно исправленного бага.

Другие разрешённые пользователем модели (DeepSeek V4.1 Flash, DeepSeek V4 Flash 0731, Laguna S2.1, GLM5.3 Flash) не обязательны для использования. Здесь им не приписываются непроверенные относительные качество/тарифы; это не вывод, что они хуже. Для данного плана достаточно двух подтверждённых профилей. Если хотите заменить Luna другой разрешённой моделью, оставьте тот же scope и обязательный G-review; цену/качество сначала измерить на B, не менять владельцев сложных протокольных задач вслепую.

Как экономить: один bounded task на сессию; передавать только его файл, аудит и относящиеся разделы спецификации; не форкать весь многотысячный диалог; targeted red→green tests перед full suite; не включать max reasoning везде; не тратить генерацию текста на ожидание soak. После двух попыток с одной и той же непонятой причиной — короткий reproduction/handoff к Sol вместо бесконечных дешёвых повторов. Число2 — бюджетная эвристика, не критерий технической невозможности.

## Полное распределение 12 оставшихся findings

| Finding | Владелец | Суть | Независимая приёмка |
| --- | --- | --- | --- |
| R-01 | A | Конкурентные same-key publishers повреждают очередь | G |
| R-02 | A, контракт D | Python отвергает dependency-only обновление | G |
| R-03 | D | RPC refresh не восстанавливает stale WS dependency | G |
| R-04 | D | Older RPC откатывает CPMM fee config | G |
| R-05 | E, данные D | Snapshot теряет provenance/омолаживает stale state | G |
| R-06 | B | Unrelated epoch закрывает чужие candidates | G |
| O-01 | C | Nested HTTP bypasses pacing | G |
| O-02 | D | Starvation late pools при maintenance | G |
| O-03 | D | Unchanged refresh увеличивает generation/emits | G |
| O-04 | A | Full-store scans на нормальном tick | G |
| O-05 | F; SDK metadata E | Node24/lock/version reproducibility | G |
| O-06 | F | Missing worker_stats/status/warnings | G |

Покрыты все частично исправленные BUG-002/003/014/018/019/020/023 и отсутствующая реализация BUG-024; также дополнительные дефекты аудита. Остальные16 original items не переписывать без нового доказанного дефекта, но G проверяет отсутствие регрессий и неисполненные общие gates.

## Порядок запуска: один shared workspace

1. **Волна1:** A + B + C параллельно; по3 разным наборам файлов. При ограничении ресурсов можно последовательно.
2. **Волна2:** D после A и C. B может продолжать свою независимую работу.
3. **Волна3:** E после D. Он временно владеет worker.ts/snapshot protocol.
4. **Волна4:** F после A/C/D/E. Он получает worker.ts и Python source для stats и проверяет Node24.
5. **Волна5:** G после всех A–F, включая B. Все остальные прекращают редактирование.

Это намеренно ограниченная параллельность: D/E/F разделяют worker.ts, A/F —Python source, C/D —scheduling contract. Одновременное редактирование одного файла запрещено даже для разных строк. На отдельных worktrees можно готовить независимые тесты раньше, но перенос/merge и semantic conflicts должен проверить G; не считать автоматический merge доказательством совместимости.

## Контракты на границах

- A→D: ordering state vs core slot, dependency generation и source epoch.
- C→D/F: logical job admission vs physical fetch gate, deadlines/shutdown, честные метрики.
- D→E: immutable data+provenance, реальные age/validation facts, atomic capture.
- E→F: snapshot schema/consistency/freshness и источник exact SDK versions.
- A–F→G: result file, изменения, регрессии, команды, оставшиеся ограничения.

Если интерфейс upstream изменился, downstream сначала читает новый handoff и запускает boundary-test. Не исправлять несовместимость обходом validation.

## Как передавать задание агенту

Выбрать модель из таблицы и передать **один полный соответствующий .md**. Дать доступ к репозиторию, исходному аудиту и спецификации; это обычные входные файлы, не нужно копировать весь репозиторий в prompt. Файлы задач самодостаточны по scope, алгоритмическим ограничениям, тестам и сдаче; README нужен координатору.

Короткий launch prompt:
> Выполни приложенное задание агента <буква> в репозитории crypto-market-data-lab. Сначала проверь условия старта и handoff зависимостей. Исправь только свой scope, добавь production-регрессии и сохрани results/AGENT_<буква>_RESULT.md. Сохраняй чужие изменения. Не объявляй непроверенные критерии выполненными.

Файл с заданием не предоставляет root/network/install approvals автоматически. Существующие access policy и пользовательские ограничения действуют. На подготовительном этапе ничего из этих инструкций ещё не выполнено.

## Когда работа действительно завершена

Все12 findings имеют подтверждённый FIXED, все24 BUG IDs —обновлённый статус; выполнены Node24 clean install, Python/Node suites, production integration, stress N1–N5, минимум30min timed synthetic soak с10min warm-up и метриками. Непройденный/недоступный gate явно FAIL/BLOCKED/NOT RUN. Прежние зелёные486 tests и12files не заменяют эти доказательства.

Архив пакета также содержит исходный аудит и спецификацию. Исторический аудит не менять; окончательный результат —в results/FINAL_ACCEPTANCE.md.
