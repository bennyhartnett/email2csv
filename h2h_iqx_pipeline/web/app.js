const elements = {
  configPath: document.getElementById("configPath"),
  inputRoot: document.getElementById("inputRoot"),
  outputRoot: document.getElementById("outputRoot"),
  month: document.getElementById("month"),
  runButton: document.getElementById("runButton"),
  openOutputButton: document.getElementById("openOutputButton"),
  openLogButton: document.getElementById("openLogButton"),
  configBrowse: document.getElementById("configBrowse"),
  inputBrowse: document.getElementById("inputBrowse"),
  outputBrowse: document.getElementById("outputBrowse"),
  statusPill: document.getElementById("statusPill"),
  logOutput: document.getElementById("logOutput"),
  outputPathLabel: document.getElementById("outputPathLabel"),
  logPathLabel: document.getElementById("logPathLabel"),
};

let api = null;
let polling = false;
let lastOutputRoot = "";
let lastLogFile = "";
const maxLogLines = 500;

function setRunning(running) {
  document.body.dataset.running = running ? "true" : "false";
  elements.runButton.disabled = running;
  elements.openOutputButton.disabled = running || !lastOutputRoot;
  elements.openLogButton.disabled = running || !lastLogFile;
  updateStage(running);
  updatePathLabels();
}

function statusClass(text) {
  const lower = text.toLowerCase();
  if (lower.includes("error")) return "error";
  if (lower.includes("running")) return "running";
  if (lower.includes("complete") || lower.includes("done")) return "done";
  return "idle";
}

function setStatus(text) {
  elements.statusPill.textContent = text;
  elements.statusPill.className = `pill ${statusClass(text)}`;
}

function updateStage(running) {
  const stage = running ? "running" : lastOutputRoot ? "done" : "idle";
  document.body.dataset.stage = stage;
  document.body.classList.toggle("state-ready", stage === "done");
}

function updatePathLabels() {
  if (elements.outputPathLabel) {
    elements.outputPathLabel.textContent = lastOutputRoot || "Not ready";
  }
  if (elements.logPathLabel) {
    elements.logPathLabel.textContent = lastLogFile || "Not ready";
  }
}

function appendLog(message) {
  const line = document.createElement("div");
  line.textContent = message;
  elements.logOutput.appendChild(line);
  while (elements.logOutput.children.length > maxLogLines) {
    elements.logOutput.removeChild(elements.logOutput.firstChild);
  }
  elements.logOutput.scrollTop = elements.logOutput.scrollHeight;
}

function clearLog() {
  elements.logOutput.textContent = "";
}

function readFormValues() {
  return {
    configPath: elements.configPath.value.trim(),
    inputRoot: elements.inputRoot.value.trim(),
    outputRoot: elements.outputRoot.value.trim(),
    month: elements.month.value.trim(),
  };
}

async function prefillFromConfig() {
  if (!api) return;
  const configPath = elements.configPath.value.trim();
  if (!configPath) return;
  const result = (await api.prefill_from_config(configPath)) || {};
  if (result.input_root && !elements.inputRoot.value.trim()) {
    elements.inputRoot.value = result.input_root;
  }
  if (result.output_root && !elements.outputRoot.value.trim()) {
    elements.outputRoot.value = result.output_root;
  }
  if (result.month && !elements.month.value.trim()) {
    elements.month.value = result.month;
  }
  scheduleSettingsSave();
}

let settingsTimer = null;
function scheduleSettingsSave() {
  if (!api) return;
  if (settingsTimer) {
    window.clearTimeout(settingsTimer);
  }
  settingsTimer = window.setTimeout(async () => {
    const values = readFormValues();
    await api.save_settings(
      values.configPath,
      values.inputRoot,
      values.outputRoot,
      values.month
    );
  }, 400);
}

async function runPipeline() {
  if (!api) return;
  const values = readFormValues();
  const result = await api.run_pipeline(
    values.configPath,
    values.inputRoot,
    values.outputRoot,
    values.month
  );
  if (!result || !result.ok) {
    setStatus("Error");
    appendLog(`ERROR: ${result?.error || "Unable to start pipeline."}`);
  } else {
    setRunning(true);
    setStatus("Running...");
  }
}

async function openOutput() {
  if (!api) return;
  const result = await api.open_output();
  if (!result || !result.ok) {
    appendLog(`ERROR: ${result?.error || "Unable to open output folder."}`);
  }
}

async function openLog() {
  if (!api) return;
  const result = await api.open_log();
  if (!result || !result.ok) {
    appendLog(`ERROR: ${result?.error || "Unable to open log file."}`);
  }
}

async function pollUpdates() {
  if (!api || polling) return;
  polling = true;
  try {
    const updates = await api.get_updates();
    updates.forEach((event) => {
      switch (event.type) {
        case "log_clear":
          clearLog();
          break;
        case "log":
          appendLog(event.message);
          break;
        case "status":
          setStatus(event.message);
          break;
        case "state":
          lastOutputRoot = event.output_root || "";
          lastLogFile = event.log_file || "";
          setRunning(event.running);
          break;
        case "error":
          setStatus("Error");
          appendLog(`ERROR: ${event.message}`);
          break;
        default:
          break;
      }
    });
  } finally {
    polling = false;
  }
}

function attachHandlers() {
  elements.configBrowse.addEventListener("click", async () => {
    if (!api) return;
    const path = await api.choose_config();
    if (path) {
      elements.configPath.value = path;
      await prefillFromConfig();
      scheduleSettingsSave();
    }
  });

  elements.inputBrowse.addEventListener("click", async () => {
    if (!api) return;
    const path = await api.choose_input_root();
    if (path) {
      elements.inputRoot.value = path;
      scheduleSettingsSave();
    }
  });

  elements.outputBrowse.addEventListener("click", async () => {
    if (!api) return;
    const path = await api.choose_output_root();
    if (path) {
      elements.outputRoot.value = path;
      scheduleSettingsSave();
    }
  });

  elements.configPath.addEventListener("change", prefillFromConfig);
  elements.configPath.addEventListener("blur", prefillFromConfig);

  [elements.inputRoot, elements.outputRoot, elements.month].forEach((input) => {
    input.addEventListener("change", scheduleSettingsSave);
    input.addEventListener("blur", scheduleSettingsSave);
  });

  elements.runButton.addEventListener("click", runPipeline);
  elements.openOutputButton.addEventListener("click", openOutput);
  elements.openLogButton.addEventListener("click", openLog);
}

let initAttempts = 0;
const maxInitAttempts = 50;
const initRetryDelayMs = 200;

function scheduleInitRetry() {
  if (api || initAttempts >= maxInitAttempts) return;
  initAttempts += 1;
  window.setTimeout(() => {
    if (api) return;
    if (window.pywebview?.api) {
      init();
    } else {
      scheduleInitRetry();
    }
  }, initRetryDelayMs);
}

async function init() {
  if (api) return;
  api = window.pywebview?.api;
  if (!api) {
    scheduleInitRetry();
    return;
  }
  document.body.classList.remove("no-api");
  const state = await api.get_state();
  elements.configPath.value = state.config_path || "";
  elements.inputRoot.value = state.input_root || "";
  elements.outputRoot.value = state.output_root || "";
  elements.month.value = state.month || "";
  lastOutputRoot = state.last_output_root || "";
  lastLogFile = state.last_log_file || "";
  setStatus(state.status || "Idle");
  setRunning(Boolean(state.running));
  attachHandlers();
  if (elements.configPath.value) {
    await prefillFromConfig();
  }
  setInterval(pollUpdates, 300);
}

function boot() {
  if (window.pywebview?.api) {
    init();
  } else {
    scheduleInitRetry();
  }
}

document.addEventListener("pywebviewready", init);
window.addEventListener("pywebviewready", init);
window.addEventListener("DOMContentLoaded", boot);
