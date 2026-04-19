GROUPS_PAGE_SCRIPT_RENDER = """    function setDistributionStatus(message, tone) {
      distributionStatus.hidden = false;
      distributionStatus.textContent = message;
      distributionStatus.classList.remove("is-error", "is-success");
      if (tone) {
        distributionStatus.classList.add(tone);
      }
    }

    function clearDistributionStatus() {
      distributionStatus.hidden = true;
      distributionStatus.textContent = "";
      distributionStatus.classList.remove("is-error", "is-success");
    }

    function showDistributionLanding() {
      distributionGroupsPanel.hidden = false;
      clearDistributionStatus();
      distributionForm.hidden = true;
    }

    function showDistributionForm() {
      distributionGroupsPanel.hidden = true;
      distributionStatus.hidden = false;
    }

    function getDistributionTypeLabel(distributionType) {
      if (distributionType === "round_robin_load_time") {
        return "По очереди с лимитами по нагрузке и времени";
      }
      return distributionType || "Тип распределения не указан";
    }

    function lookupDistributionUserName(userId) {
      const users = (distributionState.referenceData && distributionState.referenceData.users) || [];
      const match = users.find((user) => user.id === userId);
      return match ? (match.name || userId) : userId;
    }

    function lookupDistributionStageName(stageId) {
      const stages = (distributionState.referenceData && distributionState.referenceData.stages) || [];
      const match = stages.find((stage) => stage.id === stageId);
      return match ? (match.name || stageId) : stageId;
    }

    function renderDistributionGroupsPanel() {
      distributionGroupsList.innerHTML = "";
      const config = distributionState.config;
      if (!config) {
        return;
      }

      const card = document.createElement("article");
      card.className = `distribution-group-card${config.is_active ? "" : " is-inactive"}`;

      const header = document.createElement("div");
      header.className = "distribution-group-card-head";

      const checkbox = document.createElement("button");
      checkbox.className = "distribution-group-checkbox";
      checkbox.type = "button";
      checkbox.setAttribute("aria-label", config.is_active ? "Выключить группу" : "Включить группу");
      checkbox.setAttribute("aria-pressed", config.is_active ? "true" : "false");
      checkbox.disabled = Boolean(distributionState.isCardActionLoading);
      checkbox.textContent = config.is_active ? "✓" : "";
      checkbox.addEventListener("click", handleToggleDistributionGroupClick);

      const title = document.createElement("h3");
      title.className = "distribution-group-title";
      title.textContent = config.name || "Группа распределения";

      const titleWrap = document.createElement("div");
      titleWrap.className = "distribution-group-title-wrap";

      const statusBadge = document.createElement("span");
      statusBadge.className = `distribution-group-status${config.is_active ? " is-active" : " is-inactive"}`;
      statusBadge.textContent = config.is_active ? "Активна" : "Неактивна";

      titleWrap.append(title, statusBadge);
      header.append(checkbox, titleWrap);

      const subtitle = document.createElement("p");
      subtitle.className = "distribution-group-subtitle";
      subtitle.textContent = getDistributionTypeLabel(config.distribution_type);

      const stageName = lookupDistributionStageName(config.distribution_stage_id || "");
      const memberNames = (Array.isArray(config.members) ? config.members : [])
        .map((member) => lookupDistributionUserName(member.user_id))
        .filter(Boolean);
      const description = document.createElement("p");
      description.className = "distribution-group-description";
      description.append("Распределяет сделки в статусе ");
      const stageStrong = document.createElement("strong");
      stageStrong.textContent = `«${stageName || "не выбран"}»`;
      description.append(stageStrong, " между следующими менеджерами: ");
      const membersStrong = document.createElement("strong");
      membersStrong.textContent = memberNames.join(", ") || "список пуст";
      description.append(membersStrong, ".");

      const actions = document.createElement("div");
      actions.className = "distribution-group-actions";

      const editButton = document.createElement("button");
      editButton.className = "distribution-group-edit";
      editButton.type = "button";
      editButton.textContent = "Изменить";
      editButton.disabled = Boolean(distributionState.isCardActionLoading);
      editButton.addEventListener("click", handleEditDistributionGroupClick);

      const deleteButton = document.createElement("button");
      deleteButton.className = "distribution-group-delete";
      deleteButton.type = "button";
      deleteButton.textContent = "Удалить";
      deleteButton.disabled = Boolean(distributionState.isCardActionLoading);
      deleteButton.addEventListener("click", handleDeleteDistributionGroupClick);

      actions.append(editButton, deleteButton);
      card.append(header, subtitle, description, actions);
      distributionGroupsList.appendChild(card);
    }

    function setStatsStatus(message, tone) {
      statsStatus.hidden = false;
      statsStatus.textContent = message;
      statsStatus.classList.remove("is-error", "is-success");
      if (tone) {
        statsStatus.classList.add(tone);
      }
    }

    function createStatsItem(title, metaLines, pills) {
      const item = document.createElement("div");
      item.className = "reference-item";

      const itemTitle = document.createElement("div");
      itemTitle.className = "reference-item-title";
      itemTitle.textContent = title;
      item.appendChild(itemTitle);

      (metaLines || []).filter(Boolean).forEach((line) => {
        const meta = document.createElement("div");
        meta.className = "reference-item-meta";
        meta.textContent = line;
        item.appendChild(meta);
      });

      if (Array.isArray(pills) && pills.length) {
        const pillRow = document.createElement("div");
        pillRow.className = "reference-pill-row";
        pills.forEach((pill) => {
          const badge = document.createElement("span");
          badge.className = `reference-pill${pill.muted ? " is-muted" : ""}${pill.tone ? ` is-${pill.tone}` : ""}`;
          badge.textContent = pill.label;
          pillRow.appendChild(badge);
        });
        item.appendChild(pillRow);
      }

      return item;
    }

    function createStatsMetric(title, value, note) {
      const item = document.createElement("div");
      item.className = "stats-summary-item";

      const metricValue = document.createElement("div");
      metricValue.className = "stats-summary-value";
      metricValue.textContent = String(value);

      const metricTitle = document.createElement("div");
      metricTitle.className = "stats-summary-title";
      metricTitle.textContent = title;

      item.append(metricValue, metricTitle);
      if (note) {
        const metricNote = document.createElement("div");
        metricNote.className = "stats-summary-note";
        metricNote.textContent = note;
        item.appendChild(metricNote);
      }
      return item;
    }

    function renderStatsEmpty(container, message) {
      container.innerHTML = "";
      const empty = document.createElement("div");
      empty.className = "stats-empty";
      empty.textContent = message;
      container.appendChild(empty);
    }

    function formatStatsDate(value) {
      if (!value) {
        return "нет данных";
      }
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) {
        return value;
      }
      return parsed.toLocaleString("ru-RU");
    }

    function formatStatsDetailValue(value) {
      if (value === null || value === undefined || value === "") {
        return "null";
      }
      if (typeof value === "object") {
        return JSON.stringify(value);
      }
      return String(value);
    }

    function formatStatsDetails(details) {
      if (!details || typeof details !== "object") {
        return [];
      }
      return Object.entries(details).map(([key, value]) => `${key}: ${formatStatsDetailValue(value)}`);
    }

    function getStatsToneByStatus(status) {
      if (status === "assigned") {
        return "success";
      }
      if (status === "waiting") {
        return "warning";
      }
      if (status === "error") {
        return "danger";
      }
      return "muted";
    }

    function buildStatsDistributionModel(payload, summary, journal, members) {
      if (payload && payload.distribution && Array.isArray(payload.distribution.items)) {
        return payload.distribution;
      }

      const assignedCounter = new Map();
      journal.forEach((item) => {
        if (item.status !== "assigned" || !item.assigned_user_id) {
          return;
        }
        assignedCounter.set(item.assigned_user_id, (assignedCounter.get(item.assigned_user_id) || 0) + 1);
      });

      const memberMap = new Map((members || []).map((member) => [member.user_id, member]));
      const userIds = Array.from(new Set([
        ...assignedCounter.keys(),
        ...memberMap.keys(),
      ]));
      return {
        group_name: distributionState.config && distributionState.config.name ? distributionState.config.name : "",
        assigned_total: summary.assigned_count || 0,
        items: userIds.map((userId) => {
          const member = memberMap.get(userId) || {};
          return {
            user_id: userId,
            group_name: distributionState.config && distributionState.config.name ? distributionState.config.name : "",
            assigned_count: assignedCounter.get(userId) || 0,
            last_assigned_deal_id: member.last_assigned_deal_id || null,
            last_assigned_at: member.last_assigned_at || null,
          };
        }),
      };
    }

    function renderStatsDistributionTable(distribution, summary) {
      statsDistributionTableBody.innerHTML = "";
      statsDistributionTableFoot.innerHTML = "";
      const items = Array.isArray(distribution && distribution.items) ? distribution.items : [];

      if (!items.length) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 5;
        cell.className = "stats-table-empty";
        cell.textContent = "По текущим CRM-данным нагрузка по менеджерам пока не сформирована.";
        row.appendChild(cell);
        statsDistributionTableBody.appendChild(row);
        return;
      }

      items.forEach((item) => {
        const row = document.createElement("tr");

        const managerCell = document.createElement("td");
        const managerPrimary = document.createElement("div");
        managerPrimary.className = "stats-table-primary";
        managerPrimary.textContent = lookupDistributionUserName(item.user_id) || item.user_name || `Пользователь ${item.user_id}`;
        const managerSecondary = document.createElement("div");
        managerSecondary.className = "stats-table-secondary";
        managerSecondary.textContent = `ID: ${item.user_id}`;
        managerCell.append(managerPrimary, managerSecondary);

        const groupCell = document.createElement("td");
        groupCell.textContent = item.group_name || distribution.group_name || "Группа не настроена";

        const dealsCell = document.createElement("td");
        const dealsPrimary = document.createElement("div");
        dealsPrimary.className = "stats-table-primary";
        dealsPrimary.textContent = String(item.current_load === 0 || item.current_load ? item.current_load : (item.assigned_count || 0));
        const dealsSecondary = document.createElement("div");
        dealsSecondary.className = "stats-table-secondary";
        dealsSecondary.textContent = `Лимит: ${item.limit === 0 || item.limit ? item.limit : "—"}`;
        dealsCell.append(dealsPrimary, dealsSecondary);

        const dealCell = document.createElement("td");
        dealCell.textContent = item.last_assigned_deal_id ? `#${item.last_assigned_deal_id}` : "—";

        const assignedAtCell = document.createElement("td");
        assignedAtCell.textContent = formatStatsDate(item.last_assigned_at);

        row.append(managerCell, groupCell, dealsCell, dealCell, assignedAtCell);
        statsDistributionTableBody.appendChild(row);
      });

      const footRow = document.createElement("tr");
      const totalLabelCell = document.createElement("td");
      totalLabelCell.colSpan = 2;
      totalLabelCell.textContent = "Итого";

      const totalValueCell = document.createElement("td");
      totalValueCell.textContent = String(
        distribution.current_load_total === 0 || distribution.current_load_total
          ? distribution.current_load_total
          : (distribution.assigned_total || 0)
      );

      const totalNoteCell = document.createElement("td");
      totalNoteCell.colSpan = 2;
      totalNoteCell.textContent = `Записей в журнале: ${summary.journal_count || 0}`;

      footRow.append(totalLabelCell, totalValueCell, totalNoteCell);
      statsDistributionTableFoot.appendChild(footRow);
    }

    function renderStatsData(payload) {
      const summary = payload && payload.summary ? payload.summary : {};
      const diagnostics = Array.isArray(payload && payload.diagnostics) ? payload.diagnostics : [];
      const journal = Array.isArray(payload && payload.journal) ? payload.journal : [];
      const members = Array.isArray(payload && payload.members) ? payload.members : [];
      const distribution = buildStatsDistributionModel(payload, summary, journal, members);

      statsSummaryList.innerHTML = "";
      statsSummaryList.appendChild(createStatsMetric("Записей в журнале", summary.journal_count || 0, `показаны последние ${summary.journal_limit || journal.length || 0}`));
      statsSummaryList.appendChild(createStatsMetric("Назначено", summary.assigned_count || 0));
      statsSummaryList.appendChild(createStatsMetric("Ожидание", summary.waiting_count || 0));
      statsSummaryList.appendChild(createStatsMetric("Игнорировано", summary.ignored_count || 0));
      statsSummaryList.appendChild(createStatsMetric("Диагностика", summary.diagnostic_count || 0, `показаны последние ${summary.diagnostic_limit || diagnostics.length || 0}`));

      renderStatsDistributionTable(distribution, summary);
      statsMembersCount.textContent = String(members.length);
      statsDiagnosticsCount.textContent = String(diagnostics.length);

      if (!members.length) {
        renderStatsEmpty(statsMembersList, "Пока нет runtime-записей по участникам.");
      } else {
        statsMembersList.innerHTML = "";
        members.forEach((member) => {
          statsMembersList.appendChild(createStatsItem(
            member.user_name || `Пользователь ${member.user_id}`,
            [
              `Последняя сделка: ${member.last_assigned_deal_id || "не было"}`,
              `Последнее назначение: ${formatStatsDate(member.last_assigned_at)}`,
              `Обновлено: ${formatStatsDate(member.updated_at)}`,
            ],
            [],
          ));
        });
      }

      if (!diagnostics.length) {
        renderStatsEmpty(statsDiagnosticsList, "Диагностических записей пока нет.");
      } else {
        statsDiagnosticsList.innerHTML = "";
        diagnostics.forEach((item) => {
          const detailLines = [];
          detailLines.push(`Источник: ${item.source}`);
          if (item.deal_id) {
            detailLines.push(`Сделка: #${item.deal_id}`);
          }
          detailLines.push(`Время: ${formatStatsDate(item.created_at)}`);
          detailLines.push(...formatStatsDetails(item.details));
          statsDiagnosticsList.appendChild(createStatsItem(
            item.message || "Диагностическое событие",
            detailLines,
            [
              { label: item.level || "info", tone: item.level === "error" ? "danger" : (item.level === "warning" ? "warning" : "muted"), muted: item.level === "info" },
            ],
          ));
        });
      }

      if (!journal.length) {
        renderStatsEmpty(statsJournalList, "Журнал пуст. Как только backend запишет обработку сделок, записи появятся здесь.");
      } else {
        statsJournalList.innerHTML = "";
        journal.forEach((item) => {
          const assignedTo = item.assigned_user_id
            ? `${lookupDistributionUserName(item.assigned_user_id) || item.assigned_user_name || item.assigned_user_id} (${item.assigned_user_id})`
            : "не назначено";
          statsJournalList.appendChild(createStatsItem(
            `Сделка #${item.deal_id}`,
            [
              `Назначено: ${assignedTo}`,
              `Поле: ${item.assigned_field_id || "не заполнялось"}`,
              `Комментарий: ${item.note || "нет"}`,
              `Создано: ${formatStatsDate(item.created_at)}`,
              `Обновлено: ${formatStatsDate(item.updated_at)}`,
            ],
            [
              { label: item.status || "unknown", tone: getStatsToneByStatus(item.status), muted: item.status === "ignored" },
              { label: item.event_type || "event", muted: true },
            ],
          ));
        });
      }
    }

    function createDefaultDistributionConfig(referenceData) {
      const defaultResponsibleField = (referenceData.responsible_fields || []).find((field) => field.is_default)
        || (referenceData.responsible_fields || [])[0]
        || null;
      return {
        name: "",
        distribution_type: "round_robin_load_time",
        event_type: "deal_created",
        distribution_stage_id: "",
        responsible_field_id: defaultResponsibleField ? defaultResponsibleField.id : "",
        wait_seconds: "",
        retry_interval_seconds: "",
        is_active: true,
        members: [],
        load_stage_ids: [],
      };
    }

    function normalizeLoadedDistributionConfig(config, referenceData) {
      const defaults = createDefaultDistributionConfig(referenceData);
      if (!config) {
        return defaults;
      }

      const members = Array.isArray(config.members) ? config.members : [];
      const loadStageIds = Array.isArray(config.load_stage_ids) ? config.load_stage_ids : [];

      return {
        ...defaults,
        ...config,
        members,
        load_stage_ids: loadStageIds,
      };
    }

    function renderSelectOptions(selectElement, items, selectedValue, placeholder) {
      selectElement.innerHTML = "";

      if (placeholder) {
        const placeholderOption = document.createElement("option");
        placeholderOption.value = "";
        placeholderOption.textContent = placeholder;
        selectElement.appendChild(placeholderOption);
      }

      (items || []).forEach((item) => {
        const option = document.createElement("option");
        option.value = item.id;
        option.textContent = item.name || item.id;
        option.selected = item.id === selectedValue;
        selectElement.appendChild(option);
      });
    }

    function renderParticipants(config, users) {
      participantsList.innerHTML = "";
      const selectedMembers = new Map((config.members || []).map((member) => [member.user_id, member]));

      (users || []).forEach((user) => {
        const selectedMember = selectedMembers.get(user.id);
        const row = document.createElement("div");
        row.className = "participant-row";

        const label = document.createElement("label");
        label.className = "participant-main";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = Boolean(selectedMember);
        checkbox.dataset.userId = user.id;

        const info = document.createElement("span");
        info.className = "participant-info";

        const name = document.createElement("span");
        name.className = "participant-name";
        name.textContent = user.name || `Пользователь ${user.id}`;

        const meta = document.createElement("span");
        meta.className = "participant-meta";
        meta.textContent = `ID: ${user.id}${user.is_active ? " · активен" : " · неактивен"}`;

        info.append(name, meta);
        label.append(checkbox, info);

        const limitWrapper = document.createElement("label");
        limitWrapper.className = "participant-limit";

        const limitLabel = document.createElement("span");
        limitLabel.className = "participant-limit-label";
        limitLabel.textContent = "Лимит";

        const limitInput = document.createElement("input");
        limitInput.className = "distribution-input";
        limitInput.type = "number";
        limitInput.min = "0";
        limitInput.step = "1";
        limitInput.placeholder = "0";
        limitInput.value = selectedMember ? String(selectedMember.limit) : "";
        limitInput.dataset.limitFor = user.id;
        limitInput.disabled = !checkbox.checked;

        checkbox.addEventListener("change", () => {
          limitInput.disabled = !checkbox.checked;
          if (!checkbox.checked) {
            limitInput.value = "";
          }
        });

        limitWrapper.append(limitLabel, limitInput);
        row.append(label, limitWrapper);
        participantsList.appendChild(row);
      });
    }

    function renderLoadStages(config, stages) {
      loadStagesList.innerHTML = "";
      const selectedStageIds = new Set(config.load_stage_ids || []);

      (stages || []).forEach((stage) => {
        const row = document.createElement("label");
        row.className = "checkbox-row";

        const main = document.createElement("span");
        main.className = "checkbox-main";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = selectedStageIds.has(stage.id);
        checkbox.dataset.stageId = stage.id;

        const info = document.createElement("span");
        info.className = "checkbox-info";

        const name = document.createElement("span");
        name.className = "checkbox-name";
        name.textContent = stage.name || stage.id;

        const meta = document.createElement("span");
        meta.className = "checkbox-meta";
        meta.textContent = `ID: ${stage.id}${stage.sort !== undefined ? ` · sort: ${stage.sort}` : ""}`;

        info.append(name, meta);
        main.append(checkbox, info);
        row.appendChild(main);
        loadStagesList.appendChild(row);
      });
    }

    function renderDistributionConfigForm() {
      const referenceData = distributionState.referenceData || { users: [], stages: [], responsible_fields: [] };
      const config = distributionState.formMode === "create"
        ? createDefaultDistributionConfig(referenceData)
        : (distributionState.config || createDefaultDistributionConfig(referenceData));

      groupNameInput.value = config.name || "";
      distributionTypeSelect.value = config.distribution_type || "round_robin_load_time";
      eventTypeSelect.value = config.event_type || "deal_created";
      waitSecondsInput.value = config.wait_seconds === "" ? "" : String(config.wait_seconds ?? "");
      retryIntervalInput.value = config.retry_interval_seconds === "" ? "" : String(config.retry_interval_seconds ?? "");
      groupActiveInput.checked = Boolean(config.is_active);

      renderSelectOptions(distributionStageSelect, referenceData.stages, config.distribution_stage_id, "Выберите статус");
      renderSelectOptions(
        responsibleFieldSelect,
        referenceData.responsible_fields,
        config.responsible_field_id,
        "Выберите поле",
      );
      renderParticipants(config, referenceData.users);
      renderLoadStages(config, referenceData.stages);
      showDistributionForm();
      distributionForm.hidden = false;
    }

    function collectDistributionConfigFromForm() {
      const members = [];
      participantsList.querySelectorAll("[data-user-id]").forEach((checkbox) => {
        if (!checkbox.checked) {
          return;
        }

        const limitInput = participantsList.querySelector(`[data-limit-for="${checkbox.dataset.userId}"]`);
        members.push({
          user_id: checkbox.dataset.userId,
          limit: limitInput ? Number(limitInput.value || 0) : 0,
        });
      });

      const loadStageIds = [];
      loadStagesList.querySelectorAll("[data-stage-id]").forEach((checkbox) => {
        if (checkbox.checked) {
          loadStageIds.push(checkbox.dataset.stageId);
        }
      });

      return {
        name: groupNameInput.value.trim(),
        distribution_type: distributionTypeSelect.value,
        event_type: eventTypeSelect.value,
        distribution_stage_id: distributionStageSelect.value,
        responsible_field_id: responsibleFieldSelect.value,
        wait_seconds: Number(waitSecondsInput.value || 0),
        retry_interval_seconds: Number(retryIntervalInput.value || 0),
        is_active: groupActiveInput.checked,
        members,
        load_stage_ids: loadStageIds,
      };
    }

    function applyBulkLimitFromForm() {
      const bulkValue = bulkLimitValueInput.value.trim();
      if (!bulkValue) {
        setDistributionStatus("Укажите значение лимита для массового заполнения.", "is-error");
        return;
      }

      const shouldApplyToAll = bulkLimitScopeSelect.value === "all";
      participantsList.querySelectorAll("[data-user-id]").forEach((checkbox) => {
        if (!shouldApplyToAll && !checkbox.checked) {
          return;
        }

        const limitInput = participantsList.querySelector(`[data-limit-for="${checkbox.dataset.userId}"]`);
        if (!limitInput) {
          return;
        }
        if (!checkbox.checked && shouldApplyToAll) {
          checkbox.checked = true;
          limitInput.disabled = false;
        }
        limitInput.value = bulkValue;
      });

      clearDistributionStatus();
    }

    function setActiveView(view) {
      const content = sectionContent[view] || sectionContent.overview;
      const isDistribution = view === "distribution";
      const isStats = view === "stats";

      defaultPanel.hidden = isDistribution || isStats;
      statsPanel.hidden = !isStats;
      distributionPanel.hidden = !isDistribution;
      mainCard.classList.remove("is-centered");

      if (!isDistribution && !isStats) {
        distributionState.openFormRequested = false;
        distributionState.formMode = "edit";
        sectionBadge.textContent = content.badge;
        sectionTitle.textContent = content.title;
        sectionDescription.textContent = content.description;
      } else if (isDistribution) {
        distributionState.openFormRequested = false;
        distributionState.formMode = "edit";
        showDistributionLanding();
        loadDistributionConfigData(false);
      } else {
        loadStatsData(false);
      }

      menuButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.view === view);
      });
    }
"""
