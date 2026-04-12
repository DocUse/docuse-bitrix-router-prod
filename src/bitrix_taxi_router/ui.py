from __future__ import annotations

import json


def render_settings_page(*, initial_member_id: str | None = None, portal_message: str | None = None) -> str:
    initial_member_id_json = json.dumps(initial_member_id or "", ensure_ascii=False)
    portal_message_json = json.dumps(portal_message or "", ensure_ascii=False)
    template = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bitrix Taxi Router</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      background: #f5f7fb;
      color: #1f2937;
    }
    .page {
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }
    .card {
      background: #fff;
      border-radius: 14px;
      padding: 20px;
      box-shadow: 0 8px 28px rgba(15, 23, 42, 0.08);
      margin-bottom: 20px;
    }
    h1, h2, h3 { margin-top: 0; }
    .row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }
    label {
      display: block;
      font-size: 14px;
      font-weight: 600;
      margin-bottom: 6px;
    }
    input, button {
      font: inherit;
    }
    input[type="text"], input[type="number"] {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #d1d5db;
      border-radius: 10px;
      padding: 10px 12px;
      background: #fff;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 16px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      cursor: pointer;
      background: #2563eb;
      color: #fff;
      font-weight: 600;
    }
    button.secondary {
      background: #e5e7eb;
      color: #111827;
    }
    button.danger {
      background: #dc2626;
    }
    .muted {
      color: #6b7280;
      font-size: 14px;
    }
    .employee-list {
      display: grid;
      gap: 10px;
      max-height: 420px;
      overflow: auto;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 12px;
      background: #fafafa;
    }
    .employee-row {
      display: grid;
      grid-template-columns: 28px minmax(160px, 1fr) 110px;
      gap: 12px;
      align-items: center;
      background: #fff;
      border-radius: 10px;
      padding: 10px 12px;
      border: 1px solid #e5e7eb;
    }
    .group-item {
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 14px;
      margin-bottom: 12px;
    }
    .group-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      margin-bottom: 8px;
    }
    .group-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 10px;
    }
    .pill {
      border-radius: 999px;
      padding: 5px 10px;
      background: #eef2ff;
      color: #3730a3;
      font-size: 12px;
      font-weight: 700;
    }
    .status {
      margin-top: 10px;
      font-size: 14px;
      min-height: 20px;
    }
    .status.error {
      color: #b91c1c;
    }
    .status.success {
      color: #047857;
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <h1>Bitrix Taxi Router</h1>
      <p class="muted">Настройка групп распределения лидов с автоматической передачей следующему рекрутеру по таймеру.</p>
      <p class="muted">Экран можно открывать напрямую или внутри Bitrix24. Если портал уже передал контекст, он подхватится автоматически.</p>
      <div class="row">
        <div>
          <label for="memberIdInput">Member ID портала</label>
          <input id="memberIdInput" type="text" placeholder="Например, a223c6b3710f85df22e9377d6c4f7553">
        </div>
      </div>
      <div class="actions">
        <button id="loadPortalButton" type="button">Загрузить портал</button>
        <button id="runWorkerButton" class="secondary" type="button">Запустить перераспределение сейчас</button>
      </div>
      <div id="portalStatus" class="status"></div>
    </div>

    <div class="card">
      <h2>Группа распределения</h2>
      <div class="row">
        <div>
          <label for="groupName">Наименование группы</label>
          <input id="groupName" type="text" placeholder="Распределение 1">
        </div>
        <div>
          <label for="initialStageId">Первоначальный этап</label>
          <input id="initialStageId" type="text" placeholder="Новый кандидат / NEW">
        </div>
        <div>
          <label for="timeoutSeconds">Время ожидания, сек</label>
          <input id="timeoutSeconds" type="number" min="10" step="10" value="60">
        </div>
        <div>
          <label for="priority">Приоритет группы</label>
          <input id="priority" type="number" min="1" step="1" value="1">
        </div>
      </div>
      <div class="row">
        <div>
          <label><input id="eventOnAdd" type="checkbox" checked> Событие создания лида</label>
        </div>
        <div>
          <label><input id="eventOnUpdate" type="checkbox" checked> Событие изменения лида</label>
        </div>
        <div>
          <label><input id="isActive" type="checkbox" checked> Группа активна</label>
        </div>
      </div>
      <h3>Сотрудники</h3>
      <p class="muted">Отметьте сотрудников, которые участвуют в распределении, и задайте порядок очереди.</p>
      <div id="employees" class="employee-list"></div>
      <div class="actions">
        <button id="saveGroupButton" type="button">Сохранить группу</button>
        <button id="resetFormButton" class="secondary" type="button">Очистить форму</button>
      </div>
      <div id="groupFormStatus" class="status"></div>
    </div>

    <div class="card">
      <h2>Сохраненные группы</h2>
      <div id="groupsList"></div>
    </div>
  </div>

  <script>
    const state = {
      memberId: "",
      employees: [],
      groups: [],
      editingGroupId: null,
    };

    const refs = {
      memberIdInput: document.getElementById("memberIdInput"),
      portalStatus: document.getElementById("portalStatus"),
      employees: document.getElementById("employees"),
      groupsList: document.getElementById("groupsList"),
      groupFormStatus: document.getElementById("groupFormStatus"),
      groupName: document.getElementById("groupName"),
      initialStageId: document.getElementById("initialStageId"),
      timeoutSeconds: document.getElementById("timeoutSeconds"),
      priority: document.getElementById("priority"),
      eventOnAdd: document.getElementById("eventOnAdd"),
      eventOnUpdate: document.getElementById("eventOnUpdate"),
      isActive: document.getElementById("isActive"),
    };

    function setStatus(target, message, type = "") {
      target.textContent = message || "";
      target.className = "status" + (type ? " " + type : "");
    }

    function getMemberIdFromQuery() {
      const params = new URLSearchParams(window.location.search);
      return params.get("member_id") || "";
    }

    async function apiJson(url, options) {
      const response = await fetch(url, options);
      let payload = null;
      try {
        payload = await response.json();
      } catch (error) {
        payload = null;
      }
      if (!response.ok) {
        const detail = payload && payload.detail ? payload.detail : "Ошибка запроса";
        throw new Error(detail);
      }
      return payload;
    }

    function renderEmployees() {
      if (!state.employees.length) {
        refs.employees.innerHTML = '<div class="muted">Сначала загрузите сотрудников портала.</div>';
        return;
      }
      refs.employees.innerHTML = state.employees.map((employee, index) => {
        const checked = employee.selected ? "checked" : "";
        const order = employee.sort_order ?? index + 1;
        const title = employee.work_position ? `${employee.name} (${employee.work_position})` : employee.name;
        return `
          <div class="employee-row">
            <input data-user-id="${employee.id}" class="employee-toggle" type="checkbox" ${checked}>
            <div>${title}</div>
            <input data-order-user-id="${employee.id}" class="employee-order" type="number" min="1" step="1" value="${order}">
          </div>
        `;
      }).join("");

      document.querySelectorAll(".employee-toggle").forEach((checkbox) => {
        checkbox.addEventListener("change", (event) => {
          const userId = Number(event.target.dataset.userId);
          const employee = state.employees.find((item) => item.id === userId);
          if (employee) {
            employee.selected = event.target.checked;
          }
        });
      });

      document.querySelectorAll(".employee-order").forEach((input) => {
        input.addEventListener("input", (event) => {
          const userId = Number(event.target.dataset.orderUserId);
          const employee = state.employees.find((item) => item.id === userId);
          if (employee) {
            employee.sort_order = Number(event.target.value || 0) || 1;
          }
        });
      });
    }

    function renderGroups() {
      if (!state.groups.length) {
        refs.groupsList.innerHTML = '<div class="muted">Группы еще не созданы.</div>';
        return;
      }
      refs.groupsList.innerHTML = state.groups.map((group) => `
        <div class="group-item">
          <div class="group-head">
            <div>
              <strong>${group.name}</strong>
              <div class="muted">ID группы: ${group.id}</div>
            </div>
            <div class="actions">
              <button class="secondary" data-edit-group-id="${group.id}" type="button">Редактировать</button>
              <button class="danger" data-delete-group-id="${group.id}" type="button">Удалить</button>
            </div>
          </div>
          <div class="group-meta">
            <span class="pill">Этап: ${group.initial_stage_id}</span>
            <span class="pill">Таймер: ${group.timeout_seconds} сек</span>
            <span class="pill">Приоритет: ${group.priority}</span>
            <span class="pill">${group.is_active ? "Активна" : "Выключена"}</span>
          </div>
          <div class="muted">Сотрудники: ${group.members.map((member) => member.bitrix_user_id).join(", ") || "нет"}</div>
        </div>
      `).join("");

      document.querySelectorAll("[data-edit-group-id]").forEach((button) => {
        button.addEventListener("click", () => editGroup(Number(button.dataset.editGroupId)));
      });

      document.querySelectorAll("[data-delete-group-id]").forEach((button) => {
        button.addEventListener("click", () => deleteGroup(Number(button.dataset.deleteGroupId)));
      });
    }

    function resetForm() {
      state.editingGroupId = null;
      refs.groupName.value = "";
      refs.initialStageId.value = "";
      refs.timeoutSeconds.value = "60";
      refs.priority.value = "1";
      refs.eventOnAdd.checked = true;
      refs.eventOnUpdate.checked = true;
      refs.isActive.checked = true;
      state.employees = state.employees.map((employee, index) => ({
        ...employee,
        selected: false,
        sort_order: index + 1,
      }));
      renderEmployees();
      setStatus(refs.groupFormStatus, "");
    }

    function editGroup(groupId) {
      const group = state.groups.find((item) => item.id === groupId);
      if (!group) {
        return;
      }
      state.editingGroupId = group.id;
      refs.groupName.value = group.name || "";
      refs.initialStageId.value = group.initial_stage_id || "";
      refs.timeoutSeconds.value = String(group.timeout_seconds || 60);
      refs.priority.value = String(group.priority || 1);
      refs.eventOnAdd.checked = !!group.event_on_add;
      refs.eventOnUpdate.checked = !!group.event_on_update;
      refs.isActive.checked = !!group.is_active;
      const memberMap = new Map(group.members.map((member) => [Number(member.bitrix_user_id), Number(member.sort_order)]));
      state.employees = state.employees.map((employee, index) => ({
        ...employee,
        selected: memberMap.has(employee.id),
        sort_order: memberMap.get(employee.id) || index + 1,
      }));
      renderEmployees();
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    async function deleteGroup(groupId) {
      if (!state.memberId) {
        return;
      }
      if (!window.confirm("Удалить группу?")) {
        return;
      }
      try {
        await apiJson(`/api/groups/${groupId}?member_id=${encodeURIComponent(state.memberId)}`, {
          method: "DELETE",
        });
        setStatus(refs.groupFormStatus, "Группа удалена.", "success");
        await loadGroups();
      } catch (error) {
        setStatus(refs.groupFormStatus, error.message, "error");
      }
    }

    async function loadGroups() {
      state.groups = await apiJson(`/api/groups?member_id=${encodeURIComponent(state.memberId)}`);
      renderGroups();
    }

    async function loadEmployees() {
      const employees = await apiJson(`/api/employees?member_id=${encodeURIComponent(state.memberId)}`);
      state.employees = employees.map((employee, index) => ({
        ...employee,
        selected: false,
        sort_order: index + 1,
      }));
      renderEmployees();
    }

    async function loadPortal() {
      state.memberId = refs.memberIdInput.value.trim();
      if (!state.memberId) {
        setStatus(refs.portalStatus, "Укажите member_id портала.", "error");
        return;
      }
      try {
        setStatus(refs.portalStatus, "Загружаю сотрудников и группы...");
        await Promise.all([loadEmployees(), loadGroups()]);
        setStatus(refs.portalStatus, "Портал загружен.", "success");
      } catch (error) {
        setStatus(refs.portalStatus, error.message, "error");
      }
    }

    function buildPayload() {
      const members = state.employees
        .filter((employee) => employee.selected)
        .sort((left, right) => (left.sort_order || 0) - (right.sort_order || 0))
        .map((employee, index) => ({
          user_id: employee.id,
          sort_order: employee.sort_order || index + 1,
          is_active: true,
        }));

      return {
        group_id: state.editingGroupId,
        portal_member_id: state.memberId,
        name: refs.groupName.value.trim(),
        initial_stage_id: refs.initialStageId.value.trim(),
        timeout_seconds: Number(refs.timeoutSeconds.value || 0),
        priority: Number(refs.priority.value || 1),
        event_on_add: refs.eventOnAdd.checked,
        event_on_update: refs.eventOnUpdate.checked,
        is_active: refs.isActive.checked,
        members,
      };
    }

    async function saveGroup() {
      if (!state.memberId) {
        setStatus(refs.groupFormStatus, "Сначала загрузите портал.", "error");
        return;
      }
      const payload = buildPayload();
      if (!payload.name || !payload.initial_stage_id) {
        setStatus(refs.groupFormStatus, "Заполните название группы и первоначальный этап.", "error");
        return;
      }
      if (!payload.timeout_seconds || payload.timeout_seconds < 10) {
        setStatus(refs.groupFormStatus, "Таймер должен быть не меньше 10 секунд.", "error");
        return;
      }
      if (!payload.members.length) {
        setStatus(refs.groupFormStatus, "Выберите хотя бы одного сотрудника.", "error");
        return;
      }

      try {
        const result = await apiJson("/api/groups", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        setStatus(refs.groupFormStatus, `Группа сохранена. ID: ${result.group_id}`, "success");
        await loadGroups();
        resetForm();
      } catch (error) {
        setStatus(refs.groupFormStatus, error.message, "error");
      }
    }

    async function runWorker() {
      try {
        const result = await apiJson("/api/jobs/reassign", { method: "POST" });
        setStatus(
          refs.portalStatus,
          `Воркер выполнен: обработано ${result.processed}, передано ${result.reassigned}, завершено ${result.completed}.`,
          "success"
        );
        if (state.memberId) {
          await loadGroups();
        }
      } catch (error) {
        setStatus(refs.portalStatus, error.message, "error");
      }
    }

    document.getElementById("loadPortalButton").addEventListener("click", loadPortal);
    document.getElementById("saveGroupButton").addEventListener("click", saveGroup);
    document.getElementById("resetFormButton").addEventListener("click", resetForm);
    document.getElementById("runWorkerButton").addEventListener("click", runWorker);

    const initialMemberId = __INITIAL_MEMBER_ID__;
    const initialPortalMessage = __PORTAL_MESSAGE__;

    refs.memberIdInput.value = initialMemberId || getMemberIdFromQuery();
    if (initialPortalMessage) {
      setStatus(refs.portalStatus, initialPortalMessage, "success");
    }
    if (refs.memberIdInput.value) {
      loadPortal();
    } else {
      renderEmployees();
      renderGroups();
    }
  </script>
</body>
</html>
"""
    return (
        template
        .replace("__INITIAL_MEMBER_ID__", initial_member_id_json)
        .replace("__PORTAL_MESSAGE__", portal_message_json)
    )
